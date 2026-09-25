//! sing-box 状态查询与本地固定命令调用。

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::process::Command;

const SING_BOX_PATH: &str = "/opt/monitor/sing-box";
const SING_BOX_LIB_DIR: &str = "/opt/monitor";
const CONFIG_PATH: &str = "/etc/sing-box/config.json";
const SING_BOX_SERVICE: &str = "sing-box.service";
const STATUS_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);

/// 查询本机 sing-box 安装、systemd 服务和配置文件状态。
pub async fn status() -> Value {
    let installed = Path::new(SING_BOX_PATH)
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
    let version = if installed { sing_box_version().await } else { None };
    let (service_exists, running) = systemd_service_status().await;
    serde_json::json!({
        "installed": installed,
        "version": version,
        "service_exists": service_exists,
        "running": running,
        "config_exists": Path::new(CONFIG_PATH).is_file(),
        "config_path": CONFIG_PATH,
    })
}

async fn sing_box_version() -> Option<String> {
    let output =
        bounded_command(SING_BOX_PATH, &["version"], Some(("LD_LIBRARY_PATH", SING_BOX_LIB_DIR))).await?;
    if !output.status.success() {
        return None;
    }
    parse_sing_box_version(&String::from_utf8_lossy(&output.stdout))
}

async fn systemd_service_status() -> (bool, bool) {
    let Some(output) = bounded_command(
        "systemctl",
        &["show", "--property=LoadState", "--property=ActiveState", SING_BOX_SERVICE],
        None,
    )
    .await
    else {
        return (false, false);
    };
    if !output.status.success() {
        return (false, false);
    }
    parse_systemd_service_status(&String::from_utf8_lossy(&output.stdout))
}

/// 通过固定程序和参数执行本地查询，并在超时后终止子进程。
async fn bounded_command(
    program: &str,
    args: &[&str],
    environment: Option<(&str, &str)>,
) -> Option<std::process::Output> {
    let mut command = Command::new(program);
    command.args(args).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true);
    if let Some((key, value)) = environment {
        command.env(key, value);
    }
    let child = command.spawn().ok()?;
    tokio::time::timeout(STATUS_COMMAND_TIMEOUT, child.wait_with_output()).await.ok()?.ok()
}

fn parse_sing_box_version(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        line.trim()
            .strip_prefix("sing-box version ")
            .and_then(|version| version.split_whitespace().next())
            .filter(|version| !version.is_empty())
            .map(str::to_owned)
    })
}

fn parse_systemd_service_status(output: &str) -> (bool, bool) {
    let mut load_state = None;
    let mut active_state = None;
    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else { continue };
        match key {
            "LoadState" => load_state = Some(value.trim()),
            "ActiveState" => active_state = Some(value.trim()),
            _ => {}
        }
    }
    let service_exists = load_state.is_some_and(|state| !state.is_empty() && state != "not-found");
    let running = service_exists && active_state == Some("active");
    (service_exists, running)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sing_box_version() {
        assert_eq!(
            parse_sing_box_version("sing-box version 1.12.0\nEnvironment: go1.24"),
            Some("1.12.0".into())
        );
        assert_eq!(parse_sing_box_version("unexpected output"), None);
    }

    #[test]
    fn parses_systemd_service_state() {
        assert_eq!(parse_systemd_service_status("LoadState=loaded\nActiveState=active\n"), (true, true));
        assert_eq!(parse_systemd_service_status("LoadState=loaded\nActiveState=inactive\n"), (true, false));
        assert_eq!(
            parse_systemd_service_status("LoadState=not-found\nActiveState=inactive\n"),
            (false, false)
        );
    }
}
