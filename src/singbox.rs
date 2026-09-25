//! sing-box 状态查询与本地固定命令调用。

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use std::{fs::File, io::Read, time::UNIX_EPOCH};

use serde_json::Value;
use tokio::process::Command;

const SING_BOX_PATH: &str = "/opt/monitor/sing-box";
const SING_BOX_LIB_DIR: &str = "/opt/monitor";
const CONFIG_PATH: &str = "/etc/sing-box/config.json";
const SING_BOX_SERVICE: &str = "sing-box.service";
const STATUS_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONFIG_BYTES: usize = 32 * 1024;
const MAX_CONFIG_RESPONSE_BYTES: usize = 60 * 1024;

/// 在阻塞线程中读取固定配置，限制原文和编码后响应的大小。
pub async fn config_get() -> Result<Value, String> {
    tokio::task::spawn_blocking(|| read_config_at(Path::new(CONFIG_PATH)))
        .await
        .map_err(|_| "读取 sing-box 配置失败".to_owned())?
}

fn read_config_at(path: &Path) -> Result<Value, String> {
    let file = File::open(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => "sing-box 配置文件不存在".to_owned(),
        _ => "无法读取 sing-box 配置文件".to_owned(),
    })?;
    let metadata = file.metadata().map_err(|_| "无法读取 sing-box 配置文件信息".to_owned())?;
    if !metadata.is_file() {
        return Err("sing-box 配置路径不是普通文件".to_owned());
    }
    if metadata.len() > MAX_CONFIG_BYTES as u64 {
        return Err("sing-box 配置文件超过 32 KiB 限制".to_owned());
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "无法读取 sing-box 配置文件".to_owned())?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err("sing-box 配置文件超过 32 KiB 限制".to_owned());
    }
    let size_bytes = bytes.len();
    let content = String::from_utf8(bytes).map_err(|_| "sing-box 配置文件不是 UTF-8 文本".to_owned())?;
    let modified_at =
        metadata.modified().ok().and_then(|time| time.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_secs());
    let result = serde_json::json!({
        "content": content,
        "size_bytes": size_bytes,
        "modified_at": modified_at,
        "config_path": CONFIG_PATH,
    });
    if result.to_string().len() > MAX_CONFIG_RESPONSE_BYTES {
        return Err("sing-box 配置编码后超过 WebSocket 响应限制".to_owned());
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlAction {
    Start,
    Stop,
    Restart,
    Reload,
}

impl ControlAction {
    pub fn from_method(method: &str) -> Option<Self> {
        match method {
            "singbox.start" => Some(Self::Start),
            "singbox.stop" => Some(Self::Stop),
            "singbox.restart" => Some(Self::Restart),
            "singbox.reload" => Some(Self::Reload),
            _ => None,
        }
    }

    pub fn method(self) -> &'static str {
        match self {
            Self::Start => "singbox.start",
            Self::Stop => "singbox.stop",
            Self::Restart => "singbox.restart",
            Self::Reload => "singbox.reload",
        }
    }

    fn systemctl_verb(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::Reload => "reload",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ControlError {
    Failed(String),
    OutcomeUnknown(String),
}

/// 查询本机 sing-box 安装、systemd 服务和配置文件状态。
pub async fn status() -> Value {
    let service_state = systemd_service_state().await.unwrap_or((false, false));
    snapshot(service_state).await
}

async fn snapshot((service_exists, running): (bool, bool)) -> Value {
    let installed = Path::new(SING_BOX_PATH)
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
    let version = if installed { sing_box_version().await } else { None };
    serde_json::json!({
        "installed": installed,
        "version": version,
        "service_exists": service_exists,
        "running": running,
        "config_exists": Path::new(CONFIG_PATH).is_file(),
        "config_path": CONFIG_PATH,
    })
}

/// 固定服务名执行控制操作；超时后无法确认 systemd 是否已经接收并完成操作。
pub async fn control(action: ControlAction) -> Result<Value, ControlError> {
    match tokio::time::timeout(CONTROL_COMMAND_TIMEOUT, control_inner(action)).await {
        Ok(result) => result,
        Err(_) => Err(ControlError::OutcomeUnknown("服务操作超时，结果可能已经生效".into())),
    }
}

async fn control_inner(action: ControlAction) -> Result<Value, ControlError> {
    let (service_exists, running) =
        systemd_service_state().await.map_err(|message| ControlError::Failed(message.into()))?;
    check_preconditions(action, service_exists, running, Path::new(CONFIG_PATH).is_file())?;

    let mut command = Command::new("systemctl");
    command
        .args([action.systemctl_verb(), SING_BOX_SERVICE])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| ControlError::Failed("无法启动 systemctl".into()))?;
    let exit = child
        .wait()
        .await
        .map_err(|_| ControlError::OutcomeUnknown("无法确认 systemctl 是否完成服务操作".into()))?;
    if !exit.success() {
        return Err(ControlError::Failed(format!(
            "systemctl {} {} 失败（退出码 {}）",
            action.systemctl_verb(),
            SING_BOX_SERVICE,
            exit.code().map_or_else(|| "信号终止".to_owned(), |code| code.to_string())
        )));
    }
    let service_state = systemd_service_state()
        .await
        .map_err(|_| ControlError::OutcomeUnknown("服务操作已提交，但无法确认当前状态".into()))?;
    Ok(snapshot(service_state).await)
}

fn check_preconditions(
    action: ControlAction,
    service_exists: bool,
    running: bool,
    config_exists: bool,
) -> Result<(), ControlError> {
    if !service_exists {
        return Err(ControlError::Failed("sing-box.service 不存在".into()));
    }
    if action != ControlAction::Stop && !config_exists {
        return Err(ControlError::Failed(format!("配置文件 {CONFIG_PATH} 不存在")));
    }
    if action == ControlAction::Reload && !running {
        return Err(ControlError::Failed("sing-box.service 当前未运行，无法 reload".into()));
    }
    Ok(())
}

async fn sing_box_version() -> Option<String> {
    let output =
        bounded_command(SING_BOX_PATH, &["version"], Some(("LD_LIBRARY_PATH", SING_BOX_LIB_DIR))).await?;
    if !output.status.success() {
        return None;
    }
    parse_sing_box_version(&String::from_utf8_lossy(&output.stdout))
}

async fn systemd_service_state() -> Result<(bool, bool), &'static str> {
    let Some(output) = bounded_command(
        "systemctl",
        &["show", "--property=LoadState", "--property=ActiveState", SING_BOX_SERVICE],
        None,
    )
    .await
    else {
        return Err("无法查询 systemd 服务状态");
    };
    if !output.status.success() {
        return Err("无法查询 systemd 服务状态");
    }
    Ok(parse_systemd_service_status(&String::from_utf8_lossy(&output.stdout)))
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
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

    fn test_config_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "monitor-agent-config-get-{}-{}",
            std::process::id(),
            TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn config_get_reads_exact_bytes_and_reports_metadata() {
        let path = test_config_path();
        std::fs::write(&path, b"").unwrap();
        let result = read_config_at(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(result["content"], "");
        assert_eq!(result["size_bytes"], 0);
        assert!(result["modified_at"].as_u64().is_some());
        assert_eq!(result["config_path"], CONFIG_PATH);
    }

    #[test]
    fn config_get_rejects_missing_oversize_invalid_utf8_and_encoded_oversize() {
        let path = test_config_path();
        assert!(read_config_at(&path).unwrap_err().contains("不存在"));
        for (bytes, expected) in [
            (vec![b'a'; MAX_CONFIG_BYTES + 1], "超过 32 KiB"),
            (vec![0xff], "不是 UTF-8"),
            (vec![0; MAX_CONFIG_BYTES], "WebSocket 响应限制"),
        ] {
            std::fs::write(&path, bytes).unwrap();
            assert!(read_config_at(&path).unwrap_err().contains(expected));
            std::fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn control_actions_have_fixed_methods_and_systemctl_verbs() {
        for (method, action, verb) in [
            ("singbox.start", ControlAction::Start, "start"),
            ("singbox.stop", ControlAction::Stop, "stop"),
            ("singbox.restart", ControlAction::Restart, "restart"),
            ("singbox.reload", ControlAction::Reload, "reload"),
        ] {
            assert_eq!(ControlAction::from_method(method), Some(action));
            assert_eq!(action.method(), method);
            assert_eq!(action.systemctl_verb(), verb);
        }
        assert_eq!(ControlAction::from_method("shell.exec"), None);
    }

    #[test]
    fn control_preconditions_reject_missing_service_and_required_config() {
        for action in
            [ControlAction::Start, ControlAction::Stop, ControlAction::Restart, ControlAction::Reload]
        {
            assert_eq!(
                check_preconditions(action, false, false, true),
                Err(ControlError::Failed("sing-box.service 不存在".into()))
            );
        }
        assert!(check_preconditions(ControlAction::Stop, true, false, false).is_ok());
        for action in [ControlAction::Start, ControlAction::Restart, ControlAction::Reload] {
            assert!(matches!(check_preconditions(action, true, true, false), Err(ControlError::Failed(_))));
        }
        assert!(matches!(
            check_preconditions(ControlAction::Reload, true, false, true),
            Err(ControlError::Failed(_))
        ));
    }

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
