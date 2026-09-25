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
const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

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
