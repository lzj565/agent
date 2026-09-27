//! The local sing-box V2Ray API address shared by configuration apply and
//! traffic collection. The installer also calls the small internal commands
//! in `main.rs` to read and update the on-disk JSON without external parsers.

use std::path::Path;

use serde_json::{json, Value};

pub const DEFAULT_API_PORT: u16 = 9001;

pub fn port_from_env() -> Result<u16, String> {
    match std::env::var("SINGBOX_API_PORT") {
        Ok(value) => parse_port(&value).ok_or_else(|| "invalid SINGBOX_API_PORT".into()),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_API_PORT),
        Err(error) => Err(format!("read SINGBOX_API_PORT: {error}")),
    }
}

pub fn parse_port(value: &str) -> Option<u16> {
    let port = value.parse::<u16>().ok()?;
    (port != 0).then_some(port)
}

pub fn listen_address(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Extract a numeric port from a valid host:port address. Only the port is
/// carried forward; the installer always rewrites the host to loopback.
pub fn port_from_listen(listen: &str) -> Option<u16> {
    let (host, port) = listen.rsplit_once(':')?;
    if host.is_empty() || host.chars().any(char::is_whitespace) {
        return None;
    }
    if host.starts_with('[') != host.ends_with(']') {
        return None;
    }
    parse_port(port)
}

pub fn port_from_config(content: &str) -> Result<Option<u16>, String> {
    let config: Value = serde_json::from_str(content).map_err(|error| error.to_string())?;
    Ok(config.pointer("/experimental/v2ray_api/listen").and_then(Value::as_str).and_then(port_from_listen))
}

/// Update only the API listener and stats enabled flag, preserving all
/// inbounds, outbounds, filters, and other configuration fields.
pub fn set_config_port(content: &str, port: u16) -> Result<String, String> {
    if port == 0 {
        return Err("API port must be between 1 and 65535".into());
    }
    let mut config: Value = serde_json::from_str(content).map_err(|error| error.to_string())?;
    let root = config.as_object_mut().ok_or("sing-box config must be a JSON object")?;
    if !root.get("experimental").is_some_and(Value::is_object) {
        root.insert("experimental".into(), json!({}));
    }
    let experimental =
        root.get_mut("experimental").and_then(Value::as_object_mut).ok_or("invalid experimental config")?;
    if !experimental.get("v2ray_api").is_some_and(Value::is_object) {
        experimental.insert("v2ray_api".into(), json!({}));
    }
    let api =
        experimental.get_mut("v2ray_api").and_then(Value::as_object_mut).ok_or("invalid v2ray_api config")?;
    api.insert("listen".into(), Value::String(listen_address(port)));
    if !api.get("stats").is_some_and(Value::is_object) {
        api.insert("stats".into(), json!({}));
    }
    api.get_mut("stats")
        .and_then(Value::as_object_mut)
        .ok_or("invalid v2ray_api.stats config")?
        .insert("enabled".into(), Value::Bool(true));
    serde_json::to_string_pretty(&config).map_err(|error| error.to_string())
}

/// Linux TCP tables list every listening local address, including IPv4 and
/// IPv6 wildcards. Return an error if the kernel tables cannot be inspected.
pub fn tcp_port_listening(port: u16) -> Result<bool, String> {
    let mut read_any = false;
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                read_any = true;
                if table_has_listener(&contents, port) {
                    return Ok(true);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("read {path}: {error}")),
        }
    }
    if read_any {
        Ok(false)
    } else {
        Err("no readable Linux TCP socket table".into())
    }
}

fn table_has_listener(contents: &str, port: u16) -> bool {
    contents.lines().skip(1).any(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        fields.get(3) == Some(&"0A")
            && fields
                .get(1)
                .and_then(|local| local.rsplit_once(':'))
                .and_then(|(_, hex)| u16::from_str_radix(hex, 16).ok())
                == Some(port)
    })
}

/// Atomically rewrite the config in place, retaining its permission bits.
pub fn set_config_file_port(path: &Path, port: u16) -> Result<(), String> {
    let path = std::fs::canonicalize(path).map_err(|error| format!("resolve {}: {error}", path.display()))?;
    let content =
        std::fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let updated = set_config_port(&content, port)?;
    let metadata = std::fs::metadata(&path).map_err(|error| format!("stat {}: {error}", path.display()))?;
    let temp = path.with_extension(format!("json.api-port.{}", std::process::id()));
    let result = (|| {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(updated.as_bytes()).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        std::fs::set_permissions(&temp, metadata.permissions()).map_err(|error| error.to_string())?;
        std::fs::rename(&temp, &path).map_err(|error| error.to_string())?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_legacy_api_port_and_ignores_invalid_addresses() {
        let config = r#"{"experimental":{"v2ray_api":{"listen":"127.0.0.1:9004"}}}"#;
        assert_eq!(port_from_config(config).unwrap(), Some(9004));
        assert_eq!(port_from_listen("0.0.0.0:9004"), Some(9004));
        assert_eq!(port_from_listen("[::]:9004"), Some(9004));
        assert_eq!(port_from_listen("0.0.0.0:0"), None);
        assert_eq!(port_from_listen(":9004"), None);
    }

    #[test]
    fn writes_loopback_port_and_preserves_other_config() {
        let input = r#"{"inbounds":[{"listen_port":24060}],"experimental":{"v2ray_api":{"listen":"0.0.0.0:9001","stats":{"enabled":false,"users":["u"]}}}}"#;
        let output: Value = serde_json::from_str(&set_config_port(input, 9002).unwrap()).unwrap();
        assert_eq!(output["experimental"]["v2ray_api"]["listen"], "127.0.0.1:9002");
        assert_eq!(output["experimental"]["v2ray_api"]["stats"]["enabled"], true);
        assert_eq!(output["experimental"]["v2ray_api"]["stats"]["users"][0], "u");
        assert_eq!(output["inbounds"][0]["listen_port"], 24060);
    }

    #[test]
    fn detects_listeners_on_any_local_address() {
        let table = "sl local_address rem_address st\n 0: 0100007F:2329 00000000:0000 0A\n 1: 00000000:232A 00000000:0000 0A\n 2: 00000000000000000000000000000000:232B 00000000000000000000000000000000:0000 0A\n";
        assert!(table_has_listener(table, 9001));
        assert!(table_has_listener(table, 9002));
        assert!(table_has_listener(table, 9003));
        assert!(!table_has_listener(table, 9004));
    }
}
