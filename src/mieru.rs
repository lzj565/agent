//! Managed mita lifecycle. All file replacement and service operations share one lock.
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};
use tokio::{fs, process::Command, sync::Mutex};
static LOCK: Mutex<()> = Mutex::const_new(());
const ROOT: &str = "/opt/monitor";
fn root() -> PathBuf {
    PathBuf::from(ROOT)
}
async fn owned() -> Result<(), String> {
    for p in [
        root().join("config"),
        root().join("mita"),
        root().join(".mieru-managed"),
        root().join("config/mieru.json"),
        root().join("config/mieru-state.json"),
    ] {
        if fs::symlink_metadata(&p).await.map(|m| m.file_type().is_symlink()).unwrap_or(false) {
            return Err("mieru managed path is a symlink".into());
        }
    }
    if !root().join(".mieru-managed").is_file() || !root().join("mita").is_file() {
        return Err("请先安装本安装器管理的 mieru".into());
    }
    Ok(())
}
async fn run(program: &Path, args: &[&str], config: Option<&Path>) -> Result<String, String> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .kill_on_drop(true)
        .env("MITA_UDS_PATH", "/run/monitor-mieru/mita.sock")
        .env("MITA_CONFIG_JSON_FILE", root().join("config/mieru.json"));
    if let Some(config) = config {
        cmd.env_remove("MITA_CONFIG_JSON_FILE").env("MITA_CONFIG_FILE", config);
    }
    let out = tokio::time::timeout(Duration::from_secs(3), cmd.output())
        .await
        .map_err(|_| "mita command timed out")?
        .map_err(|e| e.to_string())?;
    if out.stdout.len() > 1024 * 1024 || out.stderr.len() > 4096 {
        return Err("mita output exceeds limit".into());
    }
    if !out.status.success() {
        return Err("mita command failed; check service logs".into());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into())
}
async fn cli(args: &[&str]) -> Result<String, String> {
    run(&root().join("mita"), args, None).await
}
async fn service(action: &str) -> Result<(), String> {
    if Path::new("/run/systemd/system").exists() {
        run(
            Path::new("/usr/bin/systemctl"),
            &[if action == "status" { "is-active" } else { action }, "monitor-mieru"],
            None,
        )
        .await?;
    } else if Path::new("/run/openrc/softlevel").exists() {
        run(Path::new("/sbin/rc-service"), &["monitor-mieru", action], None).await?;
    } else {
        return Err("unsupported init system".into());
    }
    Ok(())
}
async fn enable(on: bool) -> Result<(), String> {
    if Path::new("/run/systemd/system").exists() {
        run(Path::new("/usr/bin/systemctl"), &[if on { "enable" } else { "disable" }, "monitor-mieru"], None)
            .await?;
    } else {
        let out = run(
            Path::new("/sbin/rc-update"),
            &[if on { "add" } else { "del" }, "monitor-mieru", "default"],
            None,
        )
        .await;
        if on {
            out?;
        }
    }
    Ok(())
}
async fn state() -> Value {
    fs::read(root().join("config/mieru-state.json"))
        .await
        .ok()
        .and_then(|s| serde_json::from_slice(&s).ok())
        .unwrap_or(json!({"revision":0,"digest":""}))
}
async fn running() -> bool {
    cli(&["status"]).await.is_ok_and(|s| s.contains("\"RUNNING\""))
}
async fn enabled() -> bool {
    if Path::new("/run/systemd/system").exists() {
        run(Path::new("/usr/bin/systemctl"), &["is-enabled", "monitor-mieru"], None).await.is_ok()
    } else {
        run(Path::new("/sbin/rc-update"), &["show", "default"], None)
            .await
            .is_ok_and(|s| s.lines().any(|l| l.split_whitespace().next() == Some("monitor-mieru")))
    }
}
async fn status() -> Value {
    let stored = state().await;
    let mut s = json!({"revision":stored["revision"],"digest":stored["digest"],"proxy_id":stored["proxy_id"],"installed":root().join("mita").is_file(),"managed":root().join(".mieru-managed").is_file(),"version":"3.39.0"});
    s["running"] = json!(s["managed"] == true && running().await);
    s["enabled"] = json!(enabled().await);
    let wanted = stored["content"].as_str().and_then(|v| serde_json::from_str::<Value>(v).ok());
    let actual = fs::read(root().join("config/mieru.json"))
        .await
        .ok()
        .and_then(|v| serde_json::from_slice::<Value>(&v).ok());
    if wanted.as_ref().map(|v| &v["config"]) != actual.as_ref() {
        s["digest"] = json!("");
    }
    s
}
async fn atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncWriteExt;
    let tmp = path.with_extension(format!("{}.tmp", rand::random::<u64>()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&tmp).await.map_err(|e| e.to_string())?;
    file.write_all(bytes).await.map_err(|e| e.to_string())?;
    file.sync_all().await.map_err(|e| e.to_string())?;
    // mita's unprivileged service must be able to read its configuration.
    if path.ends_with("mieru.json") {
        run(Path::new("/usr/bin/chown"), &["mita:mita", tmp.to_str().ok_or("invalid path")?], None).await?;
    }
    fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await.map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).await.map_err(|e| e.to_string())
}
/// mita has no read-only config-check command: replace config mutates its live daemon.
/// Limit this adapter to the exact schema generated by the Hub, then verify native startup.
async fn validate(config: &Value) -> Result<(), String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Binding {
        port: u16,
        protocol: String,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct User {
        name: String,
        password: String,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Dns {
        #[serde(rename = "dualStack")]
        dual_stack: String,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        #[serde(rename = "portBindings")]
        ports: Vec<Binding>,
        users: Vec<User>,
        #[serde(rename = "loggingLevel")]
        logging: String,
        mtu: Option<u16>,
        dns: Option<Dns>,
    }
    let parsed: Config =
        serde_json::from_value(config.clone()).map_err(|_| "invalid mieru configuration schema")?;
    if parsed.ports.len() != 1
        || parsed.ports[0].port < 1025
        || parsed.ports[0].protocol != "TCP"
        || parsed.users.is_empty()
        || parsed.users.len() > 4096
        || parsed.logging != "WARN"
        || parsed.mtu.is_some_and(|n| n != 1400)
        || parsed.dns.is_some_and(|d| d.dual_stack != "PREFER_IPv4")
    {
        return Err("unsupported mieru configuration".into());
    }
    let mut seen = std::collections::HashSet::new();
    for user in parsed.users {
        if user.name.strip_prefix("user-").and_then(|s| s.parse::<i64>().ok()).is_none_or(|n| n <= 0)
            || !seen.insert(user.name)
            || user.password.is_empty()
            || user.password.len() > 1024
        {
            return Err("invalid mieru user".into());
        }
    }
    Ok(())
}
pub async fn execute(action: &str, params: &Value) -> Result<Value, String> {
    let _lock = LOCK.lock().await;
    if action == "mieru.status" {
        return Ok(status().await);
    }
    owned().await?;
    match action {
        "mieru.config.get" => {
            let raw = fs::read(root().join("config/mieru.json")).await.map_err(|e| e.to_string())?;
            serde_json::from_slice(&raw).map_err(|e| e.to_string())
        }
        "mieru.restart" => {
            let state = state().await;
            if state["content"]
                .as_str()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .is_none_or(|v| v["run"] != true)
            {
                return Err("mieru is administratively stopped".into());
            }
            service("restart").await?;
            if !running().await {
                return Err("mita did not enter RUNNING".into());
            }
            Ok(status().await)
        }
        "mieru.config.check" | "mieru.config.apply" => {
            let content = params["content"]
                .as_str()
                .filter(|s| s.len() <= 1024 * 1024)
                .ok_or("invalid or oversized content")?;
            let desired: Value = serde_json::from_str(content).map_err(|_| "invalid JSON")?;
            let config = &desired["config"];
            let run = desired["run"].as_bool().ok_or("missing run")?;
            if action == "mieru.config.check" {
                validate(config).await?;
                return Ok(json!({"valid":true}));
            }
            let revision = params["revision"].as_i64().filter(|r| *r > 0).ok_or("invalid revision")?;
            let digest = params["digest"]
                .as_str()
                .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or("invalid digest")?;
            let old = state().await;
            let previous = old["revision"].as_i64().unwrap_or(0);
            if revision < previous
                || (revision == previous && (old["digest"] != digest || old["content"] != content))
            {
                return Err("stale mieru revision".into());
            }
            let path = root().join("config/mieru.json");
            let before = fs::read(&path).await.map_err(|e| e.to_string())?;
            let same = old["digest"] == digest && old["content"] == content;
            if same
                && serde_json::from_slice::<Value>(&before).ok().as_ref() == Some(config)
                && running().await == run
                && enabled().await == run
            {
                return Ok(status().await);
            }
            if run {
                validate(config).await?;
            }
            let was_running = running().await;
            let newconfig = config.to_string();
            atomic(&path, newconfig.as_bytes()).await?;
            let old_config: Value = serde_json::from_slice(&before).unwrap_or(Value::Null);
            let reload = was_running && run && old_config["portBindings"] == config["portBindings"];
            let result = async {
                if run {
                    enable(true).await?;
                    if reload { cli(&["reload"]).await?; } else { service("restart").await?; }
                    if !running().await { return Err("mita did not enter RUNNING".into()); }
                } else {
                    if service("status").await.is_ok() { service("stop").await?; }
                    enable(false).await?;
                }
                atomic(&root().join("config/mieru-state.json"), json!({"revision":revision,"digest":digest,"content":content,"proxy_id":desired["proxy_id"]}).to_string().as_bytes()).await?;
                Ok::<_,String>(())
            }.await;
            if let Err(error) = result {
                atomic(&path, &before).await?;
                let restore = if was_running {
                    enable(true).await?;
                    service("restart").await
                } else {
                    let _ = enable(false).await;
                    service("stop").await
                };
                if restore.is_err() {
                    return Err(format!("{error}; rollback failed"));
                }
                return Err(error);
            }
            Ok(status().await)
        }
        _ => Err("unsupported mieru command".into()),
    }
}
/// Counter epoch follows the daemon socket, surviving Agent reconnects but changing on mita restarts.
pub async fn collect(seq: i64) -> Option<Value> {
    use std::os::unix::fs::MetadataExt;
    if !root().join(".mieru-managed").is_file() {
        return None;
    }
    let state = state().await;
    if state["content"]
        .as_str()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .is_none_or(|v| v["run"] != true)
    {
        return None;
    }
    let proxy = state["proxy_id"].as_i64().filter(|n| *n > 0)?;
    let meta = fs::metadata("/run/monitor-mieru/mita.sock").await.ok();
    static BOOT: OnceLock<String> = OnceLock::new();
    let boot = BOOT.get_or_init(|| {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap_or_default().trim().into()
    });
    let epoch = meta
        .map(|m| format!("{boot}:{}:{}:{}", m.ino(), m.ctime(), m.ctime_nsec()))
        .unwrap_or_else(|| format!("{boot}:stopped"));
    let mut out = json!({"proxy_id":proxy,"epoch":epoch,"seq":seq});
    match cli(&["get", "metrics"])
        .await
        .and_then(|s| serde_json::from_str::<Value>(&s).map_err(|_| "invalid metrics JSON".into()))
    {
        Ok(v) => {
            let convert = |v: &Value| json!({"uplink":v["UploadBytes"].as_i64().unwrap_or(0),"downlink":v["DownloadBytes"].as_i64().unwrap_or(0)});
            if !v["traffic"].is_object() {
                out["error"] = json!("missing mita traffic metrics");
                return Some(out);
            }
            let users = v["users"]
                .as_object()
                .map(|u| {
                    u.iter()
                        .filter(|(k, _)| k.starts_with("user-"))
                        .map(|(k, v)| (k.clone(), convert(v)))
                        .collect::<serde_json::Map<_, _>>()
                })
                .unwrap_or_default();
            out["users"] = json!(users);
            out["total"] = convert(&v["traffic"]);
        }
        Err(e) => out["error"] = json!(e),
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn offline_check_rejects_unsupported_transports_and_unknown_fields() {
        let good = json!({"portBindings":[{"port":24061,"protocol":"TCP"}],"users":[{"name":"user-1","password":"test"}],"loggingLevel":"WARN"});
        assert!(validate(&good).await.is_ok());
        let mut bad = good.clone();
        bad["portBindings"][0]["protocol"] = json!("UDP");
        assert!(validate(&bad).await.is_err());
        bad = good.clone();
        bad["portBindings"][0]["port"] = json!(80);
        assert!(validate(&bad).await.is_err());
        bad = good.clone();
        bad["users"][0]["allowLoopbackIP"] = json!(true);
        assert!(validate(&bad).await.is_err());
        bad = good.clone();
        bad["users"].as_array_mut().unwrap().push(good["users"][0].clone());
        assert!(validate(&bad).await.is_err());
    }
    #[tokio::test]
    #[ignore = "requires disposable Linux container with managed mita and test service wrapper"]
    async fn real_mita_apply_reload_rollback_and_traffic() {
        assert_eq!(std::env::var("MIERU_E2E").as_deref(), Ok("1"));
        let config = |port| json!({"portBindings":[{"port":port,"protocol":"TCP"}],"users":[{"name":"user-1","password":"e2e-test-only-password"}],"loggingLevel":"WARN"});
        let params = |rev, port| json!({"revision":rev,"digest":format!("{:064x}",rev),"content":json!({"proxy_id":1,"run":true,"config":config(port)}).to_string()});
        assert!(execute("mieru.config.apply", &params(1, 24061)).await.unwrap()["running"] == true);
        assert!(execute("mieru.config.apply", &params(1, 24061)).await.is_ok());
        assert!(
            execute("mieru.config.apply", &params(1, 24062)).await.is_err(),
            "same revision cannot contain different config"
        );
        assert!(execute("mieru.config.apply", &params(2, 24061)).await.is_ok());
        assert!(execute("mieru.config.apply", &params(1, 24061)).await.is_err());
        let occupied = tokio::net::TcpListener::bind("0.0.0.0:24062").await.unwrap();
        assert!(execute("mieru.config.apply", &params(3, 24062)).await.is_err());
        assert!(running().await, "failed listener update must restore working service");
        drop(occupied);
        // An external Python driver starts the native client and generates traffic now.
        fs::write("/tmp/mieru-ready", b"ready").await.unwrap();
        for _ in 0..100 {
            if Path::new("/tmp/mieru-traffic-done").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(Path::new("/tmp/mieru-traffic-done").exists());
        let sample = collect(1).await.unwrap();
        assert!(sample.get("error").is_none(), "{sample}");
        assert!(sample["users"]["user-1"]["downlink"].as_i64().unwrap_or(0) > 0, "{sample}");
        let off = json!({"revision":4,"digest":format!("{:064x}",4),"content":json!({"proxy_id":1,"run":false,"config":{"users":[],"portBindings":[]}}).to_string()});
        assert_eq!(execute("mieru.config.apply", &off).await.unwrap()["running"], false);
    }
}
