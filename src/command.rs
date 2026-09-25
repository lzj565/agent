//! Agent 内置命令的能力声明、分派与 JSON-RPC 响应。

use serde_json::Value;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::collect::Collector;
use crate::singbox::{ConfigAction, ControlAction, ControlError};

/// Agent 在 hello 中声明的内置命令白名单。
pub const CAPABILITIES: &[&str] = &[
    "agent.status",
    "singbox.status",
    "singbox.config.get",
    "singbox.config.check",
    "singbox.config.apply",
    "singbox.start",
    "singbox.stop",
    "singbox.restart",
    "singbox.reload",
];
const CONTROL_FAILED_CODE: i64 = -32000;
const CONTROL_TIMEOUT_CODE: i64 = -32001;
const CONTROL_BUSY_CODE: i64 = -32002;

/// 生成与请求 ID 对应的 JSON-RPC 成功响应。
fn command_result(id: &str, result: Value) -> Message {
    Message::Text(serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string().into())
}

/// 生成与请求 ID 对应的 JSON-RPC 错误响应。
fn command_error(id: &str, code: i64, message: &str) -> Message {
    Message::Text(
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message}
        })
        .to_string()
        .into(),
    )
}

/// 控制命令在占用执行许可前先校验参数，避免错误请求阻塞其他操作。
pub fn control_request(id: &str, method: &str, params: &Value) -> Option<Result<ControlAction, Message>> {
    let action = ControlAction::from_method(method)?;
    Some(if params.as_object().is_some_and(|values| values.is_empty()) {
        Ok(action)
    } else {
        Err(command_error(id, -32602, &format!("{} 不接收参数", action.method())))
    })
}

pub fn busy_response(id: &str) -> Message {
    command_error(id, CONTROL_BUSY_CODE, "sing-box 操作正在进行")
}

pub struct ConfigRequest {
    pub action: ConfigAction,
    pub content: String,
}

/// 配置命令只接受一份受限大小的 UTF-8 JSON 文本。
pub fn config_request(id: &str, method: &str, params: &Value) -> Option<Result<ConfigRequest, Message>> {
    let action = ConfigAction::from_method(method)?;
    Some((|| {
        let Some(values) = params.as_object() else {
            return Err(command_error(id, -32602, "配置命令参数必须是对象"));
        };
        if values.len() != 1 {
            return Err(command_error(id, -32602, "配置命令只接受 content 参数"));
        }
        let Some(content) = values.get("content").and_then(Value::as_str) else {
            return Err(command_error(id, -32602, "配置命令 content 必须是字符串"));
        };
        if content.len() > 32 * 1024 {
            return Err(command_error(id, -32602, "sing-box 配置超过 32 KiB 限制"));
        }
        Ok(ConfigRequest { action, content: content.to_owned() })
    })())
}

pub async fn config_response(id: &str, request: ConfigRequest) -> Message {
    let started = Instant::now();
    let result = match request.action {
        ConfigAction::Check => crate::singbox::config_check(request.content).await,
        ConfigAction::Apply => crate::singbox::config_apply(request.content).await,
    };
    eprintln!(
        "command id={id} method={} success={} elapsed_ms={}",
        request.action.method(),
        result.is_ok(),
        started.elapsed().as_millis()
    );
    control_result(id, result)
}

pub async fn control_response(id: &str, action: ControlAction) -> Message {
    let started = Instant::now();
    let result = crate::singbox::control(action).await;
    // 记录关联 ID、方法和耗时，便于按 Monitor 的命令记录定位本地操作。
    eprintln!(
        "command id={id} method={} success={} elapsed_ms={}",
        action.method(),
        result.is_ok(),
        started.elapsed().as_millis()
    );
    control_result(id, result)
}

pub async fn config_get_response(id: &str) -> Message {
    let started = Instant::now();
    let result = crate::singbox::config_get().await;
    eprintln!(
        "command id={id} method=singbox.config.get success={} elapsed_ms={}",
        result.is_ok(),
        started.elapsed().as_millis()
    );
    match result {
        Ok(value) => command_result(id, value),
        Err(message) => command_error(id, CONTROL_FAILED_CODE, &message),
    }
}

fn control_result(id: &str, result: Result<Value, ControlError>) -> Message {
    match result {
        Ok(value) => command_result(id, value),
        Err(ControlError::Failed(message)) => command_error(id, CONTROL_FAILED_CODE, &message),
        Err(ControlError::OutcomeUnknown(message)) => command_error(id, CONTROL_TIMEOUT_CODE, &message),
    }
}

/// 按白名单分派命令；参数错误或未知方法只返回错误帧，不结束 WebSocket 会话。
pub async fn respond(
    id: &str,
    method: &str,
    params: &Value,
    collector: &Collector,
    interval: u64,
    process_started: Instant,
) -> Message {
    match method {
        "agent.status" if params.as_object().is_some_and(|values| values.is_empty()) => command_result(
            id,
            serde_json::json!({
                "agent_version": env!("CARGO_PKG_VERSION"),
                "process_uptime_secs": process_started.elapsed().as_secs(),
                "report_interval_secs": interval,
                "counted_ifaces": collector.counted_ifaces(),
            }),
        ),
        "agent.status" => command_error(id, -32602, "agent.status 不接收参数"),
        "singbox.status" if params.as_object().is_some_and(|values| values.is_empty()) => {
            command_result(id, crate::singbox::status().await)
        }
        "singbox.status" => command_error(id, -32602, "singbox.status 不接收参数"),
        "singbox.config.get" => command_error(id, -32602, "singbox.config.get 不接收参数"),
        _ => command_error(id, -32601, "不支持的命令方法"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn control_requests_validate_methods_and_params_before_running() {
        for method in ["singbox.start", "singbox.stop", "singbox.restart", "singbox.reload"] {
            let action = control_request("control-1", method, &json!({})).unwrap().unwrap();
            assert_eq!(action.method(), method);
            let error = control_request("control-1", method, &json!({"extra": true})).unwrap().unwrap_err();
            let Message::Text(text) = error else { panic!("命令错误必须是文本帧") };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["id"], "control-1");
            assert_eq!(value["error"]["code"], -32602);
        }
        assert!(control_request("control-1", "shell.exec", &json!({})).is_none());
    }

    #[test]
    fn config_requests_require_one_bounded_content_string() {
        let request =
            config_request("config-1", "singbox.config.check", &json!({"content": "{}"})).unwrap().unwrap();
        assert_eq!(request.action, ConfigAction::Check);
        assert_eq!(request.content, "{}");
        let error =
            match config_request("config-2", "singbox.config.apply", &json!({"content": "{}", "extra": 1})) {
                Some(Err(error)) => error,
                _ => panic!("多余参数必须被拒绝"),
            };
        let Message::Text(text) = error else { panic!("命令错误必须是文本帧") };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["error"]["message"], "配置命令只接受 content 参数");

        let error = match config_request(
            "config-3",
            "singbox.config.check",
            &json!({"content": "x".repeat(32 * 1024 + 1)}),
        ) {
            Some(Err(error)) => error,
            _ => panic!("超限配置必须被拒绝"),
        };
        let Message::Text(text) = error else { panic!("命令错误必须是文本帧") };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["error"]["message"], "sing-box 配置超过 32 KiB 限制");
        assert!(config_request("config-4", "shell.exec", &json!({})).is_none());
    }

    #[test]
    fn control_outcomes_use_distinct_json_rpc_error_codes() {
        let cases = [
            (control_result("control-2", Ok(json!({"running": true}))), "result", None),
            (
                control_result("control-2", Err(ControlError::Failed("操作失败".into()))),
                "error",
                Some(CONTROL_FAILED_CODE),
            ),
            (
                control_result("control-2", Err(ControlError::OutcomeUnknown("结果未知".into()))),
                "error",
                Some(CONTROL_TIMEOUT_CODE),
            ),
            (busy_response("control-2"), "error", Some(CONTROL_BUSY_CODE)),
        ];
        for (reply, field, code) in cases {
            let Message::Text(text) = reply else { panic!("命令响应必须是文本帧") };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["id"], "control-2");
            assert!(value.get(field).is_some());
            if let Some(code) = code {
                assert_eq!(value["error"]["code"], code);
            }
        }
    }

    #[tokio::test]
    async fn agent_status_returns_process_and_collection_details() {
        let collector = Collector::default();
        let started = Instant::now() - Duration::from_secs(4);
        let reply = respond("request-1", "agent.status", &json!({}), &collector, 15, started).await;
        let Message::Text(text) = reply else { panic!("命令响应必须是文本帧") };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], "request-1");
        assert_eq!(value["result"]["agent_version"], env!("CARGO_PKG_VERSION"));
        assert!(value["result"]["process_uptime_secs"].as_u64().unwrap() >= 4);
        assert_eq!(value["result"]["report_interval_secs"], 15);
        assert!(value["result"]["counted_ifaces"].is_array());
    }

    #[tokio::test]
    async fn singbox_status_returns_a_correlated_response() {
        let reply =
            respond("request-2", "singbox.status", &json!({}), &Collector::default(), 1, Instant::now())
                .await;
        let Message::Text(text) = reply else { panic!("命令响应必须是文本帧") };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["id"], "request-2");
        assert!(value["result"]["installed"].is_boolean());
        assert!(value["result"]["version"].is_string() || value["result"]["version"].is_null());
        assert!(value["result"]["service_exists"].is_boolean());
        assert!(value["result"]["running"].is_boolean());
        assert!(value["result"]["config_exists"].is_boolean());
        assert_eq!(value["result"]["config_path"], "/etc/sing-box/config.json");
    }

    #[tokio::test]
    async fn unsupported_methods_and_invalid_params_return_json_rpc_errors() {
        let collector = Collector::default();
        let started = Instant::now();
        for (method, params, code) in [
            ("not.registered", json!({}), -32601),
            ("agent.status", json!({"extra": true}), -32602),
            ("singbox.status", json!({"extra": true}), -32602),
            ("singbox.config.get", json!({"extra": true}), -32602),
        ] {
            let reply = respond("request-3", method, &params, &collector, 1, started).await;
            let Message::Text(text) = reply else { panic!("命令错误必须是文本帧") };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["id"], "request-3");
            assert_eq!(value["error"]["code"], code);
        }
    }
}
