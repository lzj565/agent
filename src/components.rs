//! Bounded inventory polling, independent of proxy configuration and install intent.
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, process::Command, sync::mpsc};
const INTERVAL: Duration = Duration::from_secs(30);
pub struct Worker {
    rx: mpsc::Receiver<Value>,
    task: tokio::task::JoinHandle<()>,
}
impl Worker {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let value = collect().await;
                if tx.send(value).await.is_err() {
                    break;
                }
            }
        });
        Self { rx, task }
    }
    pub async fn recv(&mut self) -> Option<Value> {
        self.rx.recv().await
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort()
    }
}
async fn run(program: &Path, args: &[&str]) -> Result<(i32, String), String> {
    let output = tokio::time::timeout(
        Duration::from_secs(3),
        Command::new(program).args(args).kill_on_drop(true).output(),
    )
    .await
    .map_err(|_| "component check timed out")?
    .map_err(|_| "component check could not start")?;
    if output.stdout.len() > 4096 || output.stderr.len() > 4096 {
        return Err("component check output exceeds limit".into());
    }
    Ok((output.status.code().unwrap_or(-1), String::from_utf8_lossy(&output.stdout).into_owned()))
}
async fn observe(root: &Path) -> Value {
    use std::os::unix::fs::PermissionsExt;
    let binary = root.join("sing-box");
    let metadata = fs::metadata(&binary).await;
    let installed = metadata.as_ref().is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0);
    let systemd = Path::new("/run/systemd/system").exists();
    let openrc = Path::new("/run/openrc/softlevel").exists();
    let service_file = if systemd {
        PathBuf::from("/etc/systemd/system/sing-box.service")
    } else {
        PathBuf::from("/etc/init.d/sing-box")
    };
    let service_owned =
        fs::read_to_string(service_file).await.is_ok_and(|s| s.contains(binary.to_string_lossy().as_ref()));
    let managed = installed && service_owned;
    let mut out = json!({"installed":installed,"managed":managed,"version":""});
    if metadata.as_ref().is_err_and(|e| e.kind() != std::io::ErrorKind::NotFound) {
        return json!({"error":"cannot inspect component binary"});
    }
    if installed {
        match run(&binary, &["version"]).await {
            Ok((0, text)) => {
                out["version"] =
                    json!(text.lines().next().unwrap_or("").chars().take(128).collect::<String>())
            }
            _ => {
                out["error"] = json!("component version check failed");
            }
        }
    }
    let service = if systemd {
        run(Path::new("/usr/bin/systemctl"), &["is-active", "--quiet", "sing-box"]).await
    } else if openrc {
        run(Path::new("/sbin/rc-service"), &["sing-box", "status"]).await
    } else {
        Err("unsupported init system".into())
    };
    match service {
        Ok((code, _)) if matches!(code, 0 | 1 | 3 | 4) => out["running"] = json!(code == 0),
        _ => out["error"] = json!("component service check failed"),
    }
    out
}
async fn collect() -> Value {
    let reading = tokio::time::timeout(Duration::from_secs(8), observe(Path::new("/opt/monitor")))
        .await
        .unwrap_or_else(|_| json!({"error":"component inventory timed out"}));
    json!({"singbox": reading})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn inventory_worker_is_cancelled_on_session_drop() {
        let worker = Worker::new();
        let handle = worker.task.abort_handle();
        drop(worker);
        tokio::task::yield_now().await;
        assert!(handle.is_finished());
    }
    #[tokio::test]
    async fn absent_binary_is_not_inferred_installed_from_capability() {
        let v =
            observe(&std::env::temp_dir().join(format!("absent-components-{}", rand::random::<u64>()))).await;
        assert_eq!(v["installed"], false);
        assert_eq!(v["managed"], false);
    }
}
