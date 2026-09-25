//! Agent 内置命令的能力声明、分派与 JSON-RPC 响应。

use serde_json::Value;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::collect::Collector;

/// Agent 在 hello 中声明的内置命令白名单。
pub const CAPABILITIES: &[&str] = &["agent.status", "singbox.status"];

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
        _ => command_error(id, -32601, "不支持的命令方法"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

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
        ] {
            let reply = respond("request-3", method, &params, &collector, 1, started).await;
            let Message::Text(text) = reply else { panic!("命令错误必须是文本帧") };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["id"], "request-3");
            assert_eq!(value["error"]["code"], code);
        }
    }
}
