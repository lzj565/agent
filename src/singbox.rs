use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;

const MAX_CONFIG: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 4096;
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct Manager {
    binary: PathBuf,
    config: PathBuf,
    systemctl: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

struct Output {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Default for Manager {
    fn default() -> Self {
        Self::new("/opt/monitor/sing-box", "/etc/sing-box/config.json", "systemctl")
    }
}

impl Manager {
    fn new(binary: impl Into<PathBuf>, config: impl Into<PathBuf>, systemctl: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            config: config.into(),
            systemctl: systemctl.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn execute(&self, action: &str, params: &Value) -> Result<Value, String> {
        match action {
            "singbox.status" => self.status().await,
            "singbox.config.get" => self.read_config().await.map(|content| json!({"content": content})),
            "singbox.config.check" => {
                let content = content(params)?;
                self.check_config(content).await?;
                Ok(json!({"valid": true}))
            }
            "singbox.config.apply" => {
                let content = content(params)?;
                self.apply_config(content).await?;
                Ok(json!({"applied": true}))
            }
            "singbox.restart" => {
                self.restart().await?;
                Ok(json!({"running": true}))
            }
            _ => Err(format!("unknown action: {action}")),
        }
    }

    async fn status(&self) -> Result<Value, String> {
        let installed = fs::metadata(&self.binary)
            .await
            .map(|m| {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    m.is_file() && m.permissions().mode() & 0o111 != 0
                }
                #[cfg(not(unix))]
                {
                    m.is_file()
                }
            })
            .unwrap_or(false);
        let version = if installed {
            self.output(&self.binary, &["version"])
                .await
                .ok()
                .map(|o| limited(&o.stdout).lines().next().unwrap_or("").to_owned())
                .unwrap_or_default()
        } else {
            String::new()
        };
        Ok(json!({"installed": installed, "running": self.is_running().await, "version": version}))
    }

    async fn read_config(&self) -> Result<String, String> {
        let file = fs::File::open(&self.config).await.map_err(|e| format!("open config: {e}"))?;
        let mut bytes = Vec::new();
        use tokio::io::AsyncReadExt;
        file.take((MAX_CONFIG + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| format!("read config: {e}"))?;
        if bytes.len() > MAX_CONFIG {
            return Err("config exceeds 1 MiB".into());
        }
        String::from_utf8(bytes).map_err(|e| format!("config is not UTF-8: {e}"))
    }

    async fn check_config(&self, content: &str) -> Result<(), String> {
        size(content)?;
        let temp = self.write_temp(content).await?;
        let result = self.check_file(&temp).await;
        let _ = fs::remove_file(temp).await;
        result
    }

    async fn apply_config(&self, content: &str) -> Result<(), String> {
        size(content)?;
        let _guard = self.write_lock.lock().await;
        let temp = self.write_temp(content).await?;
        let result = self.apply_temp(&temp).await;
        let _ = fs::remove_file(temp).await;
        result
    }

    async fn apply_temp(&self, temp: &Path) -> Result<(), String> {
        self.check_file(temp).await?;
        let permissions =
            fs::metadata(&self.config).await.map_err(|e| format!("stat config: {e}"))?.permissions();
        fs::set_permissions(temp, permissions).await.map_err(|e| format!("set config permissions: {e}"))?;
        let backup = unique_path(&self.config, "bak");
        if let Err(e) = fs::copy(&self.config, &backup).await {
            let _ = fs::remove_file(&backup).await;
            return Err(format!("backup config: {e}"));
        }
        // The candidate was created beside the target, so rename is atomic.
        if let Err(e) = fs::rename(temp, &self.config).await {
            let _ = fs::remove_file(&backup).await;
            return Err(format!("replace config: {e}"));
        }
        match self.restart_service().await {
            Ok(()) => {
                let _ = fs::remove_file(backup).await;
                Ok(())
            }
            Err(apply_error) => {
                let restore =
                    fs::rename(&backup, &self.config).await.map_err(|e| format!("restore config: {e}"));
                let rollback = match restore {
                    Ok(()) => self.restart_service().await.map(|_| "succeeded".to_owned()),
                    Err(e) => Err(e),
                };
                Err(format!(
                    "apply failed: {apply_error}; rollback: {}",
                    rollback.unwrap_or_else(|e| format!("failed: {e}"))
                ))
            }
        }
    }

    async fn write_temp(&self, content: &str) -> Result<PathBuf, String> {
        let path = unique_path(&self.config, "tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .await
            .map_err(|e| format!("create temp config: {e}"))?;
        let result = async {
            file.write_all(content.as_bytes()).await.map_err(|e| e.to_string())?;
            file.sync_all().await.map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        }
        .await;
        if let Err(e) = result {
            let _ = fs::remove_file(&path).await;
            return Err(format!("write temp config: {e}"));
        }
        Ok(path)
    }

    async fn check_file(&self, path: &Path) -> Result<(), String> {
        let output = self
            .output(&self.binary, &["check", "-c", path.to_str().ok_or("non-UTF-8 config path")?])
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!("sing-box check failed: {}", limited(&output.stderr)))
        }
    }

    async fn restart(&self) -> Result<(), String> {
        let _guard = self.write_lock.lock().await;
        self.restart_service().await
    }

    async fn restart_service(&self) -> Result<(), String> {
        let output = self.output(&self.systemctl, &["restart", "sing-box.service"]).await?;
        if !output.status.success() {
            return Err(format!("systemctl restart failed: {}", limited(&output.stderr)));
        }
        if self.is_running().await {
            Ok(())
        } else {
            Err("sing-box.service is not active after restart".into())
        }
    }

    async fn is_running(&self) -> bool {
        self.output(&self.systemctl, &["is-active", "--quiet", "sing-box.service"])
            .await
            .is_ok_and(|o| o.status.success())
    }

    async fn output(&self, program: &Path, args: &[&str]) -> Result<Output, String> {
        use tokio::io::AsyncReadExt;
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("{}: {e}", program.display()))?;
        let stdout = child.stdout.take().ok_or("missing stdout")?;
        let stderr = child.stderr.take().ok_or("missing stderr")?;
        let run = async {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let mut stdout = stdout.take((MAX_OUTPUT + 1) as u64);
            let mut stderr = stderr.take((MAX_OUTPUT + 1) as u64);
            let (a, b) = tokio::join!(stdout.read_to_end(&mut out), stderr.read_to_end(&mut err));
            a.map_err(|e| e.to_string())?;
            b.map_err(|e| e.to_string())?;
            if out.len() > MAX_OUTPUT || err.len() > MAX_OUTPUT {
                child.kill().await.map_err(|e| e.to_string())?;
                return Err(format!("{} output exceeds 4 KiB", program.display()));
            }
            let status = child.wait().await.map_err(|e| e.to_string())?;
            Ok(Output { status, stdout: out, stderr: err })
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), run)
            .await
            .map_err(|_| format!("{} timed out", program.display()))?
    }
}

fn content(params: &Value) -> Result<&str, String> {
    params.get("content").and_then(Value::as_str).ok_or_else(|| "params.content must be a string".into())
}

fn size(content: &str) -> Result<(), String> {
    if content.len() > MAX_CONFIG {
        Err("config exceeds 1 MiB".into())
    } else {
        Ok(())
    }
}

fn limited(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_OUTPUT)]).into_owned()
}

fn unique_path(config: &Path, suffix: &str) -> PathBuf {
    let n = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
    config.with_extension(format!("json.{suffix}.{}.{}", std::process::id(), n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        dir: PathBuf,
        manager: Manager,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "singbox-agent-test-{}-{}",
                std::process::id(),
                NEXT_FILE.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&dir).unwrap();
            let binary = dir.join("sing-box");
            let service = dir.join("systemctl");
            std::fs::write(&binary, "#!/bin/sh\ncase \"$1\" in version) echo 'sing-box version 1.0';; check) case \"$(cat \"$3\")\" in *invalid*) echo 'invalid config' >&2; exit 1;; esac;; esac\n").unwrap();
            std::fs::write(&service, format!("#!/bin/sh\ncase \"$1\" in restart) if test -e '{0}/fail-once'; then rm '{0}/fail-once'; exit 1; fi; test ! -e '{0}/fail';; is-active) test ! -e '{0}/inactive';; esac\n", dir.display())).unwrap();
            for path in [&binary, &service] {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            let config = dir.join("config.json");
            std::fs::write(&config, "old").unwrap();
            Self { manager: Manager::new(binary, config, service), dir }
        }
        fn config(&self) -> String {
            std::fs::read_to_string(&self.manager.config).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn get_check_apply_and_rollback() {
        let f = Fixture::new();
        assert_eq!(f.manager.read_config().await.unwrap(), "old");
        assert!(f.manager.check_config("new").await.is_ok());
        assert!(f.manager.check_config("invalid").await.is_err());
        assert_eq!(f.config(), "old");
        assert!(f.manager.apply_config("invalid").await.is_err());
        assert_eq!(f.config(), "old");
        f.manager.apply_config("new").await.unwrap();
        assert_eq!(f.config(), "new");
        std::fs::write(f.dir.join("fail-once"), "").unwrap();
        let err = f.manager.apply_config("next").await.unwrap_err();
        assert!(err.contains("rollback: succeeded"));
        assert_eq!(f.config(), "new");
        std::fs::write(f.dir.join("fail"), "").unwrap();
        let err = f.manager.apply_config("next").await.unwrap_err();
        assert!(err.contains("rollback: failed"));
        assert_eq!(f.config(), "new");
        std::fs::remove_file(f.dir.join("fail")).unwrap();
        assert!(f.manager.apply_config(&"x".repeat(MAX_CONFIG + 1)).await.is_err());
        assert_eq!(f.config(), "new");
    }

    #[tokio::test]
    async fn routes_all_actions() {
        let f = Fixture::new();
        for action in [
            "singbox.status",
            "singbox.config.get",
            "singbox.config.check",
            "singbox.config.apply",
            "singbox.restart",
        ] {
            let params = json!({"content":"new"});
            assert!(f.manager.execute(action, &params).await.is_ok(), "{action}");
        }
        assert!(f.manager.execute("singbox.stop", &json!({})).await.unwrap_err().contains("unknown action"));
    }

    #[tokio::test]
    async fn status_and_restart_require_active_service() {
        let f = Fixture::new();
        let status = f.manager.status().await.unwrap();
        assert_eq!(status["installed"], true);
        assert_eq!(status["running"], true);
        assert_eq!(status["version"], "sing-box version 1.0");
        std::fs::write(f.dir.join("inactive"), "").unwrap();
        assert_eq!(f.manager.status().await.unwrap()["running"], false);
        assert!(f.manager.restart().await.is_err());
    }
}
