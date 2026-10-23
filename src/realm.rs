//! Managed Realm service transactions. No caller-controlled paths or shell commands.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};
use tokio::{fs, process::Command, sync::Mutex};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub log: Log,
    pub network: Network,
    pub endpoints: Vec<Endpoint>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Log {
    pub level: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Network {
    pub no_tcp: bool,
    pub use_udp: bool,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub listen: String,
    pub remote: String,
}
#[derive(Default, Clone, Deserialize, Serialize)]
struct State {
    revision: u64,
    digest: String,
    run: bool,
    #[serde(default)]
    config: Value,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Init {
    Systemd,
    OpenRc,
    Unsupported,
}

pub struct Manager {
    root: PathBuf,
    init: Init,
    program: PathBuf,
    updater: PathBuf,
    unit: PathBuf,
    lock: Mutex<()>,
}
impl Manager {
    fn new(root: PathBuf) -> Self {
        {
            let init = if Path::new("/run/systemd/system").exists() {
                Init::Systemd
            } else if Path::new("/run/openrc/softlevel").exists() {
                Init::OpenRc
            } else {
                Init::Unsupported
            };
            Self {
                root,
                lock: Mutex::new(()),
                init,
                program: if init == Init::Systemd { "systemctl" } else { "rc-service" }.into(),
                updater: "rc-update".into(),
                unit: if init == Init::Systemd {
                    "/etc/systemd/system/monitor-realm.service"
                } else {
                    "/etc/init.d/monitor-realm"
                }
                .into(),
            }
        }
    }
    fn config(&self) -> PathBuf {
        self.root.join("config/realm.json")
    }
    fn state(&self) -> PathBuf {
        self.root.join("config/realm-state.json")
    }
    async fn owned(&self) -> bool {
        for path in [self.root.join("realm"), self.root.join(".realm-managed"), self.config(), self.state()] {
            if fs::symlink_metadata(&path).await.is_ok_and(|m| m.file_type().is_symlink()) {
                return false;
            }
        }
        if !self.root.join(".realm-managed").is_file() {
            return false;
        }
        !fs::symlink_metadata(&self.unit).await.is_ok_and(|m| m.file_type().is_symlink())
            && fs::read_to_string(&self.unit)
                .await
                .is_ok_and(|s| s.contains("# Managed by monitor installer"))
    }
    async fn state_read(&self) -> State {
        fs::read(self.state()).await.ok().and_then(|s| serde_json::from_slice(&s).ok()).unwrap_or_default()
    }
    async fn service(&self, action: &str) -> Result<bool, String> {
        let (program, args): (&str, Vec<&str>) = if self.init == Init::Systemd {
            (
                self.program.to_str().ok_or("invalid service path")?,
                match action {
                    "running" => vec!["is-active", "--quiet", "monitor-realm.service"],
                    "enabled" => vec!["is-enabled", "--quiet", "monitor-realm.service"],
                    "enable" => vec!["enable", "monitor-realm.service"],
                    "disable" => vec!["disable", "monitor-realm.service"],
                    "start" => vec!["start", "monitor-realm.service"],
                    "stop" => vec!["stop", "monitor-realm.service"],
                    "restart" => vec!["restart", "monitor-realm.service"],
                    _ => return Err("unsupported service operation".into()),
                },
            )
        } else if self.init == Init::OpenRc {
            match action {
                "enabled" => {
                    let out = run(self.updater.to_str().ok_or("invalid service path")?, &["show", "default"])
                        .await?;
                    return Ok(String::from_utf8_lossy(&out.stdout)
                        .lines()
                        .any(|line| line.split_whitespace().next() == Some("monitor-realm")));
                }
                "enable" => (
                    self.updater.to_str().ok_or("invalid service path")?,
                    vec!["add", "monitor-realm", "default"],
                ),
                "disable" => (
                    self.updater.to_str().ok_or("invalid service path")?,
                    vec!["del", "monitor-realm", "default"],
                ),
                "running" => {
                    (self.program.to_str().ok_or("invalid service path")?, vec!["monitor-realm", "status"])
                }
                "start" | "stop" | "restart" => {
                    (self.program.to_str().ok_or("invalid service path")?, vec!["monitor-realm", action])
                }
                _ => return Err("unsupported service operation".into()),
            }
        } else {
            return Err("only systemd and OpenRC are supported".into());
        };
        let out = run(program, &args).await?;
        if matches!(action, "running" | "enabled") {
            return Ok(out.status.success());
        }
        if !out.status.success() {
            return Err(format!(
                "Realm {action} failed: {}",
                String::from_utf8_lossy(&out.stderr[..out.stderr.len().min(2048)])
            ));
        }
        Ok(true)
    }
    async fn status(&self) -> Value {
        let installed = self.root.join("realm").is_file();
        let managed = self.owned().await;
        let version = if installed && managed {
            run(self.root.join("realm").to_str().unwrap_or(""), &["--version"])
                .await
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout[..o.stdout.len().min(256)]).trim().to_owned())
        } else {
            None
        };
        let state = self.state_read().await;
        let matches = fs::read(self.config())
            .await
            .ok()
            .and_then(|v| serde_json::from_slice::<Value>(&v).ok())
            .is_some_and(|v| v == state.config);
        let mut checked = 0;
        let mut reachable = 0;
        let mut total = 0;
        if managed {
            if let Ok(content) = fs::read(self.config()).await {
                if content.len() <= 1024 * 1024 {
                    if let Ok(config) = serde_json::from_slice::<Config>(&content) {
                        total = config.endpoints.len();
                        let mut jobs = tokio::task::JoinSet::new();
                        for endpoint in config.endpoints.into_iter().take(8) {
                            jobs.spawn(async move {
                                tokio::time::timeout(
                                    Duration::from_millis(800),
                                    tokio::net::TcpStream::connect(endpoint.remote),
                                )
                                .await
                                .is_ok_and(|r| r.is_ok())
                            });
                        }
                        while let Some(result) = jobs.join_next().await {
                            checked += 1;
                            if result.unwrap_or(false) {
                                reachable += 1;
                            }
                        }
                    }
                }
            }
        }
        let reachability = if checked == 0 {
            "unknown"
        } else if reachable == 0 {
            "unreachable"
        } else if reachable == checked && checked == total {
            "reachable"
        } else {
            "partial"
        };
        json!({"target_reachability":reachability,"targets_checked":checked,"targets_total":total,"installed":installed,"managed":managed,"version":version,"running": managed && self.service("running").await.unwrap_or(false),"enabled":managed && self.service("enabled").await.unwrap_or(false),"revision":state.revision,"digest":if matches {state.digest}else{String::new()},"desired_running":state.run})
    }
    async fn apply(&self, params: &Value) -> Result<Value, String> {
        if !self.owned().await || !self.root.join("realm").is_file() {
            return Err("Realm is missing or is not installer-managed".into());
        }
        let revision = params.get("revision").and_then(Value::as_u64).ok_or("revision is required")?;
        let digest = params
            .get("digest")
            .and_then(Value::as_str)
            .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or("invalid digest")?;
        let running = params.get("run").and_then(Value::as_bool).ok_or("run is required")?;
        let config = check(params.get("config").ok_or("config is required")?)?;
        if running && config.endpoints.is_empty() {
            return Err("cannot run without endpoints".into());
        }
        let previous = self.state_read().await;
        if revision < previous.revision
            || (revision == previous.revision && !previous.digest.is_empty() && digest != previous.digest)
        {
            return Err("stale Realm revision".into());
        }
        let matches = fs::read(self.config())
            .await
            .ok()
            .and_then(|v| serde_json::from_slice::<Value>(&v).ok())
            .is_some_and(|v| v == previous.config);
        if revision == previous.revision
            && digest == previous.digest
            && matches
            && running == previous.run
            && self.service("running").await? == running
            && self.service("enabled").await? == running
        {
            return Ok(self.status().await);
        }
        let old_config = fs::read(self.config()).await.map_err(|e| format!("read config: {e}"))?;
        let was_running = self.service("running").await?;
        let was_enabled = self.service("enabled").await?;
        let original = self.root.join("config/realm-panel-original.json");
        if !original.exists() {
            atomic_original(&original, &old_config).await?;
        }
        let candidate = serde_json::to_vec_pretty(&config).map_err(|e| e.to_string())?;
        let outcome = async {
            atomic(&self.config(), &candidate).await?;
            if running {
                self.service("enable").await?;
                self.service(if was_running { "restart" } else { "start" }).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                if !self.service("running").await? {
                    return Err("Realm did not remain running".into());
                }
            } else {
                if was_running {
                    self.service("stop").await?;
                }
                if was_enabled {
                    self.service("disable").await?;
                }
            }
            let state = State {
                revision,
                digest: digest.into(),
                run: running,
                config: serde_json::to_value(&config).map_err(|e| e.to_string())?,
            };
            atomic(&self.state(), &serde_json::to_vec(&state).map_err(|e| e.to_string())?).await?;
            Ok::<_, String>(())
        }
        .await;
        if let Err(error) = outcome {
            let rollback = async {
                atomic(&self.config(), &old_config).await?;
                self.service(if was_enabled { "enable" } else { "disable" }).await?;
                self.service(if was_running { "restart" } else { "stop" }).await?;
                Ok::<_, String>(())
            }
            .await;
            return Err(format!(
                "{error}; rollback: {}",
                rollback.err().unwrap_or_else(|| "succeeded".into())
            ));
        }
        Ok(self.status().await)
    }
    pub async fn execute(&self, action: &str, params: &Value) -> Result<Value, String> {
        let _guard = self.lock.lock().await;
        match action {
            "realm.status" => Ok(self.status().await),
            "realm.config.get" => {
                if !self.owned().await {
                    return Err("Realm is not installer-managed".into());
                }
                let content = fs::read_to_string(self.config()).await.map_err(|e| e.to_string())?;
                if content.len() > 1024 * 1024 {
                    return Err("config too large".into());
                }
                Ok(json!({"content":content}))
            }
            "realm.config.check" => {
                check(params.get("config").ok_or("config is required")?)?;
                Ok(json!({"valid":true}))
            }
            "realm.config.apply" => self.apply(params).await,
            "realm.restart" => {
                if !self.owned().await || !self.state_read().await.run {
                    return Err("Realm is unmanaged or is disabled".into());
                }
                self.service("restart").await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                if !self.service("running").await? {
                    return Err("Realm restart failed".into());
                }
                Ok(self.status().await)
            }
            _ => Err("unknown Realm command".into()),
        }
    }
}
pub async fn execute(action: &str, params: &Value) -> Result<Value, String> {
    static MANAGER: OnceLock<Manager> = OnceLock::new();
    MANAGER.get_or_init(|| Manager::new("/opt/monitor".into())).execute(action, params).await
}
fn check(value: &Value) -> Result<Config, String> {
    if value.to_string().len() > 1024 * 1024 {
        return Err("Realm config exceeds 1 MiB".into());
    }
    let config: Config =
        serde_json::from_value(value.clone()).map_err(|e| format!("invalid Realm config: {e}"))?;
    if config.log.level != "warn"
        || config.network.no_tcp
        || config.network.use_udp
        || config.endpoints.len() > 1024
    {
        return Err("unsupported Realm config".into());
    }
    let mut ports = std::collections::HashSet::new();
    for e in &config.endpoints {
        let listen: std::net::SocketAddr = e.listen.parse().map_err(|_| "invalid listen address")?;
        if listen.port() == 0 || !listen.ip().is_unspecified() || !ports.insert(listen.port()) {
            return Err("invalid or duplicate listen port".into());
        }
        let (host, port) = e.remote.rsplit_once(':').ok_or("remote requires port")?;
        if port.parse::<u16>().ok().filter(|p| *p > 0).is_none()
            || host.is_empty()
            || host.len() > 255
            || host.chars().any(char::is_whitespace)
            || host.contains('/')
            || host.contains('@')
        {
            return Err("invalid remote address".into());
        }
        if host.starts_with('[') {
            host.strip_prefix('[')
                .and_then(|h| h.strip_suffix(']'))
                .ok_or("invalid IPv6")?
                .parse::<std::net::Ipv6Addr>()
                .map_err(|_| "invalid IPv6")?;
        } else if host.contains(':')
            || !host.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-'))
        {
            return Err("invalid remote host".into());
        }
    }
    Ok(config)
}
async fn run(program: &str, args: &[&str]) -> Result<std::process::Output, String> {
    let mut command = Command::new(program);
    command.args(args).kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(3), command.output())
        .await
        .map_err(|_| "Realm service command timed out".to_owned())?
        .map_err(|e| e.to_string())
}
async fn atomic_original(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).await.map_err(|e| e.to_string())?;
    file.write_all(bytes).await.map_err(|e| e.to_string())?;
    file.sync_all().await.map_err(|e| e.to_string())
}
async fn atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let tmp = path.with_extension(format!("realm-{}.tmp", rand::random::<u64>()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&tmp).await.map_err(|e| e.to_string())?;
    let result = async {
        file.write_all(bytes).await.map_err(|e| e.to_string())?;
        file.sync_all().await.map_err(|e| e.to_string())?;
        fs::rename(&tmp, path).await.map_err(|e| e.to_string())
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(tmp).await;
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_is_structural_and_rejects_paths_udp_and_conflicts() {
        let mut v = json!({"log":{"level":"warn"},"network":{"no_tcp":false,"use_udp":false},"endpoints":[{"listen":"0.0.0.0:443","remote":"[::1]:444"}]});
        assert!(check(&v).is_ok());
        v["endpoints"][0]["listen"] = json!("127.0.0.1:443");
        assert!(check(&v).is_err());
        v["endpoints"][0]["listen"] = json!("[::]:443");
        let duplicate = v["endpoints"][0].clone();
        v["endpoints"].as_array_mut().unwrap().push(duplicate);
        assert!(check(&v).is_err());
        v["endpoints"] = json!([]);
        v["log"]["output"] = json!("/etc/passwd");
        assert!(check(&v).is_err());
    }
    #[tokio::test]
    async fn unmanaged_machine_is_never_modified() {
        let manager = Manager::new(std::env::temp_dir().join("monitor-realm-unmanaged"));
        assert_eq!(manager.execute("realm.status", &json!({})).await.unwrap()["managed"], false);
        assert!(manager.execute("realm.config.apply", &json!({})).await.is_err());
    }
    #[cfg(unix)]
    async fn fixture(init: Init) -> (Manager, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("monitor-realm-test-{}", rand::random::<u64>()));
        fs::create_dir_all(dir.join("config")).await.unwrap();
        fs::write(dir.join(".realm-managed"), b"").await.unwrap();
        fs::write(
            dir.join("config/realm.json"),
            br#"{"log":{"level":"warn"},"network":{"no_tcp":false,"use_udp":false},"endpoints":[]}"#,
        )
        .await
        .unwrap();
        fs::write(dir.join("unit"), "# Managed by monitor installer\n").await.unwrap();
        fs::write(dir.join("realm"), "#!/bin/sh\necho 'realm 2.9.6'\n").await.unwrap();
        let script = format!(
            r#"#!/bin/sh
cd '{}'
echo "$*" >> operations
case "$1" in
 monitor-realm) shift ;;
esac
case "$1" in
 is-active|status) test -e running ;;
 is-enabled) test -e enabled ;;
 show) if test -e enabled; then echo 'monitor-realm | default'; fi ;;
 enable|add) touch enabled ;;
 disable|del) rm -f enabled ;;
 stop) rm -f running ;;
 start|restart) if test -e fail-once; then rm fail-once; exit 1; fi; touch running ;;
 *) exit 1 ;;
esac
"#,
            dir.display()
        );
        fs::write(dir.join("service"), script).await.unwrap();
        for file in ["service", "realm"] {
            fs::set_permissions(dir.join(file), std::fs::Permissions::from_mode(0o755)).await.unwrap();
        }
        let manager = Manager {
            root: dir.clone(),
            init,
            program: dir.join("service"),
            updater: dir.join("service"),
            unit: dir.join("unit"),
            lock: Mutex::new(()),
        };
        (manager, dir)
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_rollback_stale_revisions_and_drift_for_both_init_systems() {
        for init in [Init::Systemd, Init::OpenRc] {
            let (manager, dir) = fixture(init).await;
            let original = fs::read(manager.config()).await.unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut command = json!({"revision":1,"digest":"a".repeat(64),"run":true,"config":{"log":{"level":"warn"},"network":{"no_tcp":false,"use_udp":false},"endpoints":[{"listen":"0.0.0.0:20443","remote":listener.local_addr().unwrap().to_string()}]}});
            let applied = manager.execute("realm.config.apply", &command).await.unwrap();
            assert_eq!(applied["running"], true);
            assert_eq!(applied["enabled"], true);
            assert_eq!(applied["revision"], 1);
            assert_eq!(applied["target_reachability"], "reachable");
            assert_eq!(fs::read(dir.join("config/realm-panel-original.json")).await.unwrap(), original);
            let good = fs::read(manager.config()).await.unwrap();
            command["revision"] = json!(2);
            command["digest"] = json!("b".repeat(64));
            command["config"]["endpoints"][0]["listen"] = json!("0.0.0.0:20444");
            fs::write(dir.join("fail-once"), "").await.unwrap();
            let error = manager.execute("realm.config.apply", &command).await.unwrap_err();
            assert!(error.contains("rollback: succeeded"), "{error}");
            assert_eq!(fs::read(manager.config()).await.unwrap(), good);
            assert_eq!(manager.state_read().await.revision, 1);
            assert!(dir.join("running").exists());
            assert_eq!(manager.execute("realm.config.apply", &command).await.unwrap()["revision"], 2);
            command["revision"] = json!(1);
            assert!(manager.execute("realm.config.apply", &command).await.unwrap_err().contains("stale"));
            fs::write(manager.config(), &original).await.unwrap();
            assert_eq!(manager.execute("realm.status", &json!({})).await.unwrap()["digest"], "");
            command["revision"] = json!(2);
            assert_eq!(
                manager.execute("realm.config.apply", &command).await.unwrap()["digest"],
                "b".repeat(64)
            );
            command["revision"] = json!(3);
            command["digest"] = json!("c".repeat(64));
            command["run"] = json!(false);
            command["config"]["endpoints"] = json!([]);
            let stopped = manager.execute("realm.config.apply", &command).await.unwrap();
            assert_eq!(stopped["running"], false);
            assert_eq!(stopped["enabled"], false);
            assert!(manager.execute("realm.restart", &json!({})).await.is_err());
            let operations = fs::read_to_string(dir.join("operations")).await.unwrap();
            if init == Init::OpenRc {
                assert!(operations.contains("add monitor-realm default"));
                assert!(operations.contains("monitor-realm restart"));
            } else {
                assert!(operations.contains("enable monitor-realm.service"));
            }
            fs::remove_dir_all(dir).await.unwrap();
        }
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn status_does_not_execute_unmanaged_binary() {
        let (manager, dir) = fixture(Init::Systemd).await;
        fs::remove_file(dir.join(".realm-managed")).await.unwrap();
        fs::write(dir.join("realm"), format!("#!/bin/sh\ntouch '{}'\n", dir.join("executed").display()))
            .await
            .unwrap();
        let status = manager.execute("realm.status", &json!({})).await.unwrap();
        assert_eq!(status["installed"], true);
        assert_eq!(status["managed"], false);
        assert!(status["version"].is_null());
        assert!(!dir.join("executed").exists());
        fs::remove_dir_all(dir).await.unwrap();
    }
}
