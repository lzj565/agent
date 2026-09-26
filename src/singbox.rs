//! sing-box 状态查询与本地固定命令调用。

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, UNIX_EPOCH};

use rustix::fs::{fchmod, fchown, Gid, Mode, Uid};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const SING_BOX_PATH: &str = "/opt/monitor/sing-box";
const SING_BOX_LIB_DIR: &str = "/opt/monitor";
const CONFIG_PATH: &str = "/etc/sing-box/config.json";
const SING_BOX_SERVICE: &str = "sing-box.service";
const OPENRC_SERVICE: &str = "sing-box";
const OPENRC_SERVICE_FILE: &str = "/etc/init.d/sing-box";
const STATUS_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONFIG_BYTES: usize = 32 * 1024;
const MAX_CONFIG_RESPONSE_BYTES: usize = 60 * 1024;
const CONFIG_CHECK_TIMEOUT: Duration = Duration::from_secs(15);
const CONFIG_RESTART_TIMEOUT: Duration = Duration::from_secs(20);
const CONFIG_HEALTH_TIMEOUT: Duration = Duration::from_secs(15);
const CONFIG_HEALTH_STABLE: Duration = Duration::from_secs(6);
const CONFIG_HEALTH_POLL: Duration = Duration::from_millis(500);
const MAX_COMMAND_OUTPUT_BYTES: usize = 4096;
const MAX_DIAGNOSTIC_CHARS: usize = 512;

static CONFIG_FILE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
pub enum ConfigAction {
    Check,
    Apply,
}

impl ConfigAction {
    pub fn from_method(method: &str) -> Option<Self> {
        match method {
            "singbox.config.check" => Some(Self::Check),
            "singbox.config.apply" => Some(Self::Apply),
            _ => None,
        }
    }

    pub fn method(self) -> &'static str {
        match self {
            Self::Check => "singbox.config.check",
            Self::Apply => "singbox.config.apply",
        }
    }
}

/// 检查候选配置，不接触当前生产配置。
pub async fn config_check(content: String) -> Result<Value, ControlError> {
    validate_config_content(&content).map_err(ControlError::Failed)?;
    let size_bytes = content.len();
    let path = PathBuf::from(CONFIG_PATH);
    let content = content.into_bytes();
    let candidate =
        run_fs(move || create_candidate(&path, &content, None)).await.map_err(ControlError::Failed)?;
    let result = LocalApplyOps { manager: ServiceManager::current() }.validate(&candidate).await;
    let candidate_for_cleanup = candidate.clone();
    let cleanup = run_fs(move || remove_file_if_exists(&candidate_for_cleanup)).await;
    if let Err(message) = cleanup {
        return Err(ControlError::OutcomeUnknown(cleanup_description(&candidate, "临时配置", &message)));
    }
    result?;
    Ok(serde_json::json!({
        "valid": true,
        "size_bytes": size_bytes,
        "config_path": CONFIG_PATH,
    }))
}

/// 校验、原子替换并重启；任一步健康检查失败都会尝试恢复旧配置。
pub async fn config_apply(content: String) -> Result<Value, ControlError> {
    validate_config_content(&content).map_err(ControlError::Failed)?;
    let manager = ServiceManager::current();
    let (service_exists, _) =
        service_state(manager).await.map_err(|message| ControlError::Failed(message.into()))?;
    if !service_exists {
        return Err(ControlError::Failed("sing-box 服务不存在".into()));
    }
    apply_config_transaction(Path::new(CONFIG_PATH), &content, &LocalApplyOps { manager }).await
}

fn validate_config_content(content: &str) -> Result<(), String> {
    if content.len() > MAX_CONFIG_BYTES {
        return Err("sing-box 配置超过 32 KiB 限制".into());
    }
    let value: Value = serde_json::from_str(content).map_err(|_| "sing-box 配置不是有效 JSON".to_owned())?;
    if !value.is_object() {
        return Err("sing-box 配置顶层必须是 JSON 对象".into());
    }
    Ok(())
}

async fn run_fs<T, F>(operation: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(operation).await.map_err(|_| "配置文件操作线程失败".to_owned())?
}

#[derive(Clone, Copy)]
struct FileMode {
    uid: Uid,
    gid: Gid,
    mode: Mode,
}

fn config_file_mode(path: &Path) -> Result<FileMode, String> {
    let metadata = fs::symlink_metadata(path).map_err(|_| "无法读取当前 sing-box 配置文件".to_owned())?;
    if !metadata.file_type().is_file() {
        return Err("sing-box 配置路径必须是普通文件，不能是符号链接".into());
    }
    if metadata.len() > MAX_CONFIG_BYTES as u64 {
        return Err("当前 sing-box 配置超过 32 KiB 限制，拒绝替换".into());
    }
    Ok(FileMode {
        uid: Uid::from_raw(metadata.uid()),
        gid: Gid::from_raw(metadata.gid()),
        mode: Mode::from_raw_mode((metadata.mode() & 0o777) as _),
    })
}

fn ensure_config_directory(path: &Path) -> Result<&Path, String> {
    let parent = path.parent().ok_or_else(|| "sing-box 配置目录无效".to_owned())?;
    let metadata = fs::symlink_metadata(parent).map_err(|_| "sing-box 配置目录不存在".to_owned())?;
    if !metadata.file_type().is_dir() {
        return Err("sing-box 配置目录必须是普通目录".into());
    }
    Ok(parent)
}

fn create_candidate(path: &Path, content: &[u8], file_mode: Option<FileMode>) -> Result<PathBuf, String> {
    let parent = ensure_config_directory(path)?;
    for _ in 0..8 {
        let id = CONFIG_FILE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let candidate = parent.join(format!(".config.json.new-{}-{id}", std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        let mut file = match options.open(&candidate) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err("无法创建 sing-box 临时配置".into()),
        };
        let created = (|| {
            file.write_all(content).map_err(|_| "无法写入 sing-box 临时配置".to_owned())?;
            file.sync_all().map_err(|_| "无法同步 sing-box 临时配置".to_owned())?;
            if let Some(mode) = file_mode {
                fchown(&file, Some(mode.uid), Some(mode.gid))
                    .map_err(|_| "无法保留 sing-box 配置文件所有者".to_owned())?;
                fchmod(&file, mode.mode).map_err(|_| "无法保留 sing-box 配置文件权限".to_owned())?;
                file.sync_all().map_err(|_| "无法同步 sing-box 配置文件权限".to_owned())?;
            }
            Ok(())
        })();
        if let Err(message) = created {
            drop(file);
            return match fs::remove_file(&candidate) {
                Ok(()) => Err(message),
                Err(_) => Err(format!("{message}；临时文件清理失败，文件保留于 {}", candidate.display())),
            };
        }
        return Ok(candidate);
    }
    Err("无法分配 sing-box 临时配置文件名".into())
}

fn create_backup(path: &Path) -> Result<PathBuf, String> {
    config_file_mode(path)?;
    let bytes = fs::read(path).map_err(|_| "无法备份当前 sing-box 配置".to_owned())?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err("当前 sing-box 配置超过 32 KiB 限制，拒绝替换".into());
    }
    let parent = ensure_config_directory(path)?;
    for _ in 0..8 {
        let id = CONFIG_FILE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let backup = parent.join(format!(".config.json.backup-{}-{id}", std::process::id()));
        match create_new_private_file(&backup, &bytes) {
            Ok(()) => {
                if let Err(message) = sync_directory(parent) {
                    return match fs::remove_file(&backup) {
                        Ok(()) => match sync_directory(parent) {
                            Ok(()) => Err(message),
                            Err(cleanup) => Err(format!(
                                "{message}；备份已删除但目录同步失败：{cleanup}；请检查 {}",
                                backup.display()
                            )),
                        },
                        Err(_) => Err(format!("{message}；备份清理失败，备份保留于 {}", backup.display())),
                    };
                }
                return Ok(backup);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("无法创建 sing-box 配置事务备份：{error}")),
        }
    }
    Err("无法分配 sing-box 备份文件名".into())
}

fn create_new_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fchmod(&file, Mode::from_raw_mode(0o600)).map_err(std::io::Error::from)?;
        file.sync_all()
    })();
    if result.is_err() {
        drop(file);
        if fs::remove_file(path).is_err() {
            return Err(std::io::Error::other(format!("部分备份清理失败，备份保留于 {}", path.display())));
        }
    }
    result
}

enum ReplaceState {
    Synced,
    ReplacedButUnsynced(String),
}

fn atomic_replace(source: &Path, destination: &Path) -> Result<ReplaceState, String> {
    fs::rename(source, destination).map_err(|_| "sing-box 配置原子替换失败".to_owned())?;
    match sync_directory(destination.parent().ok_or_else(|| "sing-box 配置目录无效".to_owned())?) {
        Ok(()) => Ok(ReplaceState::Synced),
        Err(message) => Ok(ReplaceState::ReplacedButUnsynced(message)),
    }
}

fn sync_directory(directory: &Path) -> Result<(), String> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|_| "无法同步 sing-box 配置目录".to_owned())
}

fn remove_file_if_exists(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => sync_directory(path.parent().ok_or_else(|| "sing-box 配置目录无效".to_owned())?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("无法清理 sing-box 临时文件".into()),
    }
}

fn cleanup_description(path: &Path, label: &str, error: &str) -> String {
    if path.exists() {
        format!("{label}清理失败：{error}；文件保留于 {}", path.display())
    } else {
        format!("{label}已删除，但目录同步失败：{error}")
    }
}

trait ApplyOps {
    async fn validate(&self, candidate: &Path) -> Result<(), ControlError>;
    async fn restart(&self) -> Result<(), ControlError>;
    async fn wait_healthy(&self) -> Result<(), ControlError>;
}

struct LocalApplyOps {
    manager: ServiceManager,
}

impl ApplyOps for LocalApplyOps {
    async fn validate(&self, candidate: &Path) -> Result<(), ControlError> {
        validate_candidate(candidate).await.map_err(ControlError::Failed)
    }

    async fn restart(&self) -> Result<(), ControlError> {
        restart_service(self.manager).await
    }

    async fn wait_healthy(&self) -> Result<(), ControlError> {
        wait_for_healthy_service(self.manager).await
    }
}

async fn apply_config_transaction<O: ApplyOps>(
    config_path: &Path,
    content: &str,
    operations: &O,
) -> Result<Value, ControlError> {
    let path = config_path.to_path_buf();
    let mode = run_fs(move || config_file_mode(&path)).await.map_err(ControlError::Failed)?;
    let candidate_path = config_path.to_path_buf();
    let candidate_content = content.as_bytes().to_vec();
    let candidate = run_fs(move || create_candidate(&candidate_path, &candidate_content, Some(mode)))
        .await
        .map_err(ControlError::Failed)?;
    if let Err(error) = operations.validate(&candidate).await {
        let cleanup_candidate = candidate.clone();
        return match run_fs(move || remove_file_if_exists(&cleanup_candidate)).await {
            Ok(()) => Err(error),
            Err(message) => Err(ControlError::OutcomeUnknown(format!(
                "sing-box 配置校验失败；{}",
                cleanup_description(&candidate, "临时配置", &message)
            ))),
        };
    }

    let backup_path = config_path.to_path_buf();
    let backup = match run_fs(move || create_backup(&backup_path)).await {
        Ok(path) => path,
        Err(message) => {
            let cleanup_candidate = candidate.clone();
            return match run_fs(move || remove_file_if_exists(&cleanup_candidate)).await {
                Ok(()) => Err(ControlError::Failed(message)),
                Err(cleanup) => Err(ControlError::OutcomeUnknown(format!(
                    "无法创建配置事务备份：{message}；{}",
                    cleanup_description(&candidate, "临时配置", &cleanup)
                ))),
            };
        }
    };
    let replace_source = candidate.clone();
    let replace_target = config_path.to_path_buf();
    let mut replacement_durability_error = None;
    match run_fs(move || atomic_replace(&replace_source, &replace_target)).await {
        Err(message) => {
            let cleanup_backup = backup.clone();
            let backup_cleanup = run_fs(move || remove_file_if_exists(&cleanup_backup)).await;
            let cleanup_candidate = candidate.clone();
            let candidate_cleanup = run_fs(move || remove_file_if_exists(&cleanup_candidate)).await;
            if backup_cleanup.is_err() || candidate_cleanup.is_err() {
                let backup_message = backup_cleanup
                    .as_ref()
                    .err()
                    .map(|message| cleanup_description(&backup, "配置备份", message));
                let candidate_message = candidate_cleanup
                    .as_ref()
                    .err()
                    .map(|message| cleanup_description(&candidate, "候选配置", message));
                return Err(ControlError::OutcomeUnknown(format!(
                    "配置未能替换：{message}；{}{}",
                    backup_message.unwrap_or_default(),
                    candidate_message.map_or_else(String::new, |message| format!("；{message}"))
                )));
            }
            return Err(ControlError::Failed(message));
        }
        Ok(ReplaceState::ReplacedButUnsynced(message)) => {
            replacement_durability_error = Some(message);
        }
        Ok(ReplaceState::Synced) => {}
    }

    let apply_result = async {
        operations.restart().await?;
        operations.wait_healthy().await
    }
    .await;
    if apply_result.is_ok() {
        if let Some(message) = replacement_durability_error {
            return Err(ControlError::OutcomeUnknown(format!(
                "sing-box 已运行新配置，但配置目录同步失败：{message}；备份保留于 {}",
                backup.display()
            )));
        }
        let cleanup_backup = backup.clone();
        if run_fs(move || remove_file_if_exists(&cleanup_backup)).await.is_err() {
            return Err(ControlError::OutcomeUnknown(format!(
                "sing-box 已运行新配置；{}",
                cleanup_description(&backup, "配置备份", "无法确认备份清理结果")
            )));
        }
        return Ok(serde_json::json!({
            "config_path": CONFIG_PATH,
            "size_bytes": content.len(),
            "running": true,
        }));
    }

    let restore_path = backup.clone();
    let restore_target = config_path.to_path_buf();
    let restore_mode_path = config_path.to_path_buf();
    let restore_result = run_fs(move || {
        let mode = config_file_mode(&restore_mode_path)?;
        let old_bytes = fs::read(&restore_path).map_err(|_| "无法读取 sing-box 事务备份".to_owned())?;
        let restored = create_candidate(&restore_target, &old_bytes, Some(mode))?;
        let replacement = match atomic_replace(&restored, &restore_target) {
            Ok(state) => Ok(state),
            Err(message) => match fs::remove_file(&restored) {
                Ok(()) => Err(message),
                Err(_) => Err(format!("{message}；恢复临时文件保留于 {}", restored.display())),
            },
        };
        match replacement? {
            ReplaceState::Synced => Ok(()),
            ReplaceState::ReplacedButUnsynced(message) => Err(message),
        }
    })
    .await;
    if let Err(message) = restore_result {
        return Err(ControlError::OutcomeUnknown(format!(
            "sing-box 新配置未通过健康检查，旧配置恢复状态无法确认：{message}；备份保留于 {}",
            backup.display(),
        )));
    }

    let rollback_result = async {
        operations.restart().await?;
        operations.wait_healthy().await
    }
    .await;
    if rollback_result.is_err() {
        return Err(ControlError::OutcomeUnknown(format!(
            "sing-box 新配置失败，旧配置已写回但服务健康状态无法确认；备份保留于 {}",
            backup.display()
        )));
    }

    let cleanup_backup = backup.clone();
    if run_fs(move || remove_file_if_exists(&cleanup_backup)).await.is_err() {
        return Err(ControlError::Failed(format!(
            "sing-box 新配置失败，旧配置已恢复；{}",
            cleanup_description(&backup, "配置备份", "无法确认备份清理结果")
        )));
    }
    Err(ControlError::Failed("sing-box 新配置未通过启动健康检查，旧配置已恢复".into()))
}

async fn validate_candidate(candidate: &Path) -> Result<(), String> {
    let mut command = Command::new(SING_BOX_PATH);
    command.arg("check").arg("-c").arg(candidate).env("LD_LIBRARY_PATH", SING_BOX_LIB_DIR).kill_on_drop(true);
    let (status, stdout, stderr) = run_bounded_output(command, CONFIG_CHECK_TIMEOUT)
        .await
        .map_err(|message| format!("sing-box 配置校验超时或无法启动：{message}"))?;
    if status.success() {
        return Ok(());
    }
    let diagnostic = if stderr.is_empty() { stdout.as_slice() } else { stderr.as_slice() };
    Err(format!("sing-box 配置校验失败：{}", safe_diagnostic(diagnostic, status.code())))
}

fn safe_diagnostic(stderr: &[u8], exit_code: Option<i32>) -> String {
    let text = String::from_utf8_lossy(stderr);
    let line = text.lines().find(|line| !line.trim().is_empty()).unwrap_or("");
    let lower = line.to_ascii_lowercase();
    let hidden = [
        "private_key",
        "private key",
        "privatekey",
        "password",
        "passwd",
        "token",
        "secret",
        "uuid",
        "authorization",
        "api_key",
        "apikey",
        "client_secret",
    ]
    .iter()
    .any(|sensitive| lower.contains(sensitive));
    let diagnostic = if hidden || line.trim().is_empty() {
        "校验器没有提供可安全显示的详细信息".to_owned()
    } else {
        line.chars().filter(|character| !character.is_control()).take(MAX_DIAGNOSTIC_CHARS).collect()
    };
    if diagnostic == "校验器没有提供可安全显示的详细信息" {
        format!("{diagnostic}（退出码 {}）", exit_code.map_or_else(|| "未知".to_owned(), |n| n.to_string()))
    } else {
        diagnostic
    }
}

async fn run_bounded_output(
    mut command: Command,
    timeout: Duration,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| "无法启动固定 sing-box 命令".to_owned())?;
    let stdout = child.stdout.take().ok_or_else(|| "无法读取 sing-box 输出".to_owned())?;
    let stderr = child.stderr.take().ok_or_else(|| "无法读取 sing-box 输出".to_owned())?;
    let stdout_task = tokio::spawn(drain_limited(stdout));
    let stderr_task = tokio::spawn(drain_limited(stderr));
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => return Err("无法确认 sing-box 命令结果".into()),
        Err(_) => {
            if let Err(error) = child.kill().await {
                if child.try_wait().ok().flatten().is_none() {
                    eprintln!("sing-box 校验超时后终止子进程失败：{error}");
                }
            }
            if let Err(error) = child.wait().await {
                eprintln!("sing-box 校验超时后回收子进程失败：{error}");
            }
            if stdout_task.await.is_err() {
                eprintln!("sing-box 校验 stdout 读取任务异常退出");
            }
            if stderr_task.await.is_err() {
                eprintln!("sing-box 校验 stderr 读取任务异常退出");
            }
            return Err("命令超时".into());
        }
    };
    let stdout = stdout_task.await.map_err(|_| "无法读取 sing-box 输出".to_owned())?;
    let stderr = stderr_task.await.map_err(|_| "无法读取 sing-box 输出".to_owned())?;
    Ok((status, stdout, stderr))
}

async fn drain_limited<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> Vec<u8> {
    let mut captured = Vec::new();
    let mut buffer = [0_u8; 1024];
    while let Ok(read) = reader.read(&mut buffer).await {
        if read == 0 {
            break;
        }
        let remaining = MAX_COMMAND_OUTPUT_BYTES.saturating_sub(captured.len());
        captured.extend_from_slice(&buffer[..read.min(remaining)]);
    }
    captured
}

async fn restart_service(manager: ServiceManager) -> Result<(), ControlError> {
    let (program, args) = service_action_command(manager, ControlAction::Restart);
    let mut command = Command::new(program);
    command.args(args).stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| ControlError::Failed(format!("无法启动 {program}")))?;
    match tokio::time::timeout(CONFIG_RESTART_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(ControlError::Failed(format!(
            "sing-box 服务重启失败（退出码 {}）",
            status.code().map_or_else(|| "信号终止".to_owned(), |code| code.to_string())
        ))),
        Ok(Err(_)) => Err(ControlError::OutcomeUnknown("无法确认 sing-box 服务重启结果".into())),
        Err(_) => Err(ControlError::OutcomeUnknown("sing-box 服务重启超时".into())),
    }
}

async fn wait_for_healthy_service(manager: ServiceManager) -> Result<(), ControlError> {
    let deadline = Instant::now() + CONFIG_HEALTH_TIMEOUT;
    let mut active_since = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ControlError::Failed("sing-box 服务未能保持 active".into()));
        }
        let remaining = deadline.saturating_duration_since(now);
        let state = tokio::time::timeout(remaining.min(STATUS_COMMAND_TIMEOUT), service_state(manager)).await;
        match state {
            Ok(Ok((true, true))) => {
                let since = *active_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= CONFIG_HEALTH_STABLE {
                    return Ok(());
                }
            }
            _ => active_since = None,
        }
        let wait = CONFIG_HEALTH_POLL.min(deadline.saturating_duration_since(Instant::now()));
        if wait.is_zero() {
            return Err(ControlError::Failed("sing-box 服务未能保持 active".into()));
        }
        tokio::time::sleep(wait).await;
    }
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceManager {
    Systemd,
    OpenRc,
}

impl ServiceManager {
    fn current() -> Self {
        let configured = std::env::var("MONITOR_INIT").ok();
        service_manager_from(
            configured.as_deref(),
            Path::new("/run/openrc/softlevel").exists(),
            Path::new("/run/systemd/system").exists(),
        )
    }
}

fn service_manager_from(
    configured: Option<&str>,
    openrc_active: bool,
    systemd_active: bool,
) -> ServiceManager {
    match configured {
        Some("openrc") => ServiceManager::OpenRc,
        Some("systemd") => ServiceManager::Systemd,
        _ if openrc_active && !systemd_active => ServiceManager::OpenRc,
        _ => ServiceManager::Systemd,
    }
}

fn service_action_command(
    manager: ServiceManager,
    action: ControlAction,
) -> (&'static str, [&'static str; 2]) {
    let verb = match (manager, action) {
        (ServiceManager::OpenRc, ControlAction::Reload) => "restart",
        (_, ControlAction::Start) => "start",
        (_, ControlAction::Stop) => "stop",
        (_, ControlAction::Restart) => "restart",
        (_, ControlAction::Reload) => "reload",
    };
    match manager {
        ServiceManager::Systemd => ("systemctl", [verb, SING_BOX_SERVICE]),
        ServiceManager::OpenRc => ("rc-service", [OPENRC_SERVICE, verb]),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ControlError {
    Failed(String),
    OutcomeUnknown(String),
}

/// 查询本机 sing-box 安装、init 服务和配置文件状态。
pub async fn status() -> Value {
    let service_state = service_state(ServiceManager::current()).await;
    let known = service_state.is_ok();
    snapshot(service_state.unwrap_or((false, false)), known).await
}

async fn snapshot((service_exists, running): (bool, bool), service_state_known: bool) -> Value {
    let installed = Path::new(SING_BOX_PATH)
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
    let version = if installed { sing_box_version().await } else { None };
    let config_metadata =
        tokio::task::spawn_blocking(|| config_metadata_at(Path::new(CONFIG_PATH))).await.ok().flatten();
    serde_json::json!({
        "installed": installed,
        "version": version,
        "service_exists": service_exists,
        "running": running,
        "service_state_known": service_state_known,
        "config_exists": Path::new(CONFIG_PATH).is_file(),
        "config_sha256": config_metadata.as_ref().map(|metadata| metadata.sha256.as_str()),
        "config_updated_at": config_metadata.as_ref().and_then(|metadata| metadata.modified_at),
        "config_path": CONFIG_PATH,
    })
}

struct ConfigMetadata {
    sha256: String,
    modified_at: Option<u64>,
}

fn config_metadata_at(path: &Path) -> Option<ConfigMetadata> {
    let mut file = File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES as u64 {
        return None;
    }
    let modified_at = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());
    let mut hasher = Sha256::new();
    let mut read_total = 0_usize;
    let mut buffer = [0_u8; 4096];
    loop {
        let read = file.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        read_total = read_total.checked_add(read)?;
        if read_total > MAX_CONFIG_BYTES {
            return None;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let digits = b"0123456789abcdef";
    let mut sha256 = String::with_capacity(64);
    for byte in digest {
        sha256.push(digits[(byte >> 4) as usize] as char);
        sha256.push(digits[(byte & 0x0f) as usize] as char);
    }
    Some(ConfigMetadata { sha256, modified_at })
}

/// 固定服务名执行控制操作；超时后无法确认服务管理器是否已经完成操作。
pub async fn control(action: ControlAction) -> Result<Value, ControlError> {
    match tokio::time::timeout(CONTROL_COMMAND_TIMEOUT, control_inner(action)).await {
        Ok(result) => result,
        Err(_) => Err(ControlError::OutcomeUnknown("服务操作超时，结果可能已经生效".into())),
    }
}

async fn control_inner(action: ControlAction) -> Result<Value, ControlError> {
    let manager = ServiceManager::current();
    let (service_exists, running) =
        service_state(manager).await.map_err(|message| ControlError::Failed(message.into()))?;
    check_preconditions(action, service_exists, running, Path::new(CONFIG_PATH).is_file())?;

    let (program, args) = service_action_command(manager, action);
    let mut command = Command::new(program);
    command.args(args).stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| ControlError::Failed(format!("无法启动 {program}")))?;
    let exit = child
        .wait()
        .await
        .map_err(|_| ControlError::OutcomeUnknown("无法确认服务管理器是否完成服务操作".into()))?;
    if !exit.success() {
        return Err(ControlError::Failed(format!(
            "{} {} {} 失败（退出码 {}）",
            program,
            args[0],
            args[1],
            exit.code().map_or_else(|| "信号终止".to_owned(), |code| code.to_string())
        )));
    }
    let service_state = service_state(manager)
        .await
        .map_err(|_| ControlError::OutcomeUnknown("服务操作已提交，但无法确认当前状态".into()))?;
    Ok(snapshot(service_state, true).await)
}

fn check_preconditions(
    action: ControlAction,
    service_exists: bool,
    running: bool,
    config_exists: bool,
) -> Result<(), ControlError> {
    if !service_exists {
        return Err(ControlError::Failed("sing-box 服务不存在".into()));
    }
    if action != ControlAction::Stop && !config_exists {
        return Err(ControlError::Failed(format!("配置文件 {CONFIG_PATH} 不存在")));
    }
    if action == ControlAction::Reload && !running {
        return Err(ControlError::Failed("sing-box 服务当前未运行，无法 reload".into()));
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

async fn service_state(manager: ServiceManager) -> Result<(bool, bool), &'static str> {
    match manager {
        ServiceManager::Systemd => systemd_service_state().await,
        ServiceManager::OpenRc => openrc_service_state().await,
    }
}

async fn openrc_service_state() -> Result<(bool, bool), &'static str> {
    let service_file_exists = Path::new(OPENRC_SERVICE_FILE).is_file();
    if !service_file_exists {
        return Ok((false, false));
    }
    let Some(output) = bounded_command("rc-service", &[OPENRC_SERVICE, "status"], None).await else {
        return Err("无法查询 OpenRC 服务状态");
    };
    Ok(parse_openrc_service_status(service_file_exists, output.status.success()))
}

fn parse_openrc_service_status(service_file_exists: bool, status_succeeded: bool) -> (bool, bool) {
    (service_file_exists, service_file_exists && status_succeeded)
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
    use std::sync::Mutex;
    use std::{collections::VecDeque, os::unix::fs::PermissionsExt};

    static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

    fn test_config_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "monitor-agent-config-get-{}-{}",
            std::process::id(),
            TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn test_config_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "monitor-agent-singbox-{}-{}",
            std::process::id(),
            TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    struct FakeApplyOps {
        validate_ok: bool,
        restarts: Mutex<VecDeque<bool>>,
        health: Mutex<VecDeque<bool>>,
    }

    impl FakeApplyOps {
        fn new(validate_ok: bool, restarts: &[bool], health: &[bool]) -> Self {
            Self {
                validate_ok,
                restarts: Mutex::new(restarts.iter().copied().collect()),
                health: Mutex::new(health.iter().copied().collect()),
            }
        }
    }

    impl ApplyOps for FakeApplyOps {
        async fn validate(&self, _: &Path) -> Result<(), ControlError> {
            if self.validate_ok {
                Ok(())
            } else {
                Err(ControlError::Failed("校验失败".into()))
            }
        }

        async fn restart(&self) -> Result<(), ControlError> {
            match self.restarts.lock().unwrap().pop_front().unwrap_or(true) {
                true => Ok(()),
                false => Err(ControlError::Failed("重启失败".into())),
            }
        }

        async fn wait_healthy(&self) -> Result<(), ControlError> {
            match self.health.lock().unwrap().pop_front().unwrap_or(true) {
                true => Ok(()),
                false => Err(ControlError::Failed("健康检查失败".into())),
            }
        }
    }

    fn write_test_config(directory: &Path, content: &str) -> PathBuf {
        let path = directory.join("config.json");
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        path
    }

    fn backup_files(directory: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.file_name().is_some_and(|name| name.to_string_lossy().contains("backup")))
            .collect()
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
    fn config_validation_rejects_invalid_json_non_objects_and_oversized_input() {
        assert!(validate_config_content("{}").is_ok());
        assert!(validate_config_content("[]").unwrap_err().contains("顶层"));
        assert!(validate_config_content("{").unwrap_err().contains("有效 JSON"));
        assert!(validate_config_content(&format!("{{\"x\":\"{}\"}}", "x".repeat(MAX_CONFIG_BYTES)))
            .unwrap_err()
            .contains("32 KiB"));
    }

    #[test]
    fn atomic_replace_keeps_file_mode_and_replaces_the_complete_file() {
        let directory = test_config_dir();
        let path = write_test_config(&directory, "{\"old\":true}");
        let mode = config_file_mode(&path).unwrap();
        let candidate = create_candidate(&path, b"{\"new\":true}", Some(mode)).unwrap();
        assert_eq!(std::fs::metadata(&candidate).unwrap().permissions().mode() & 0o777, 0o640);
        assert!(matches!(atomic_replace(&candidate, &path), Ok(ReplaceState::Synced)));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"new\":true}");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o640);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn config_transaction_validates_then_replaces_and_removes_backup() {
        let directory = test_config_dir();
        let path = write_test_config(&directory, "{\"old\":true}");
        let operations = FakeApplyOps::new(true, &[true], &[true]);
        let result = apply_config_transaction(&path, "{\"new\":true}", &operations).await.unwrap();
        assert_eq!(result["running"], true);
        assert_eq!(result["size_bytes"], 12);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"new\":true}");
        assert!(backup_files(&directory).is_empty());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn config_transaction_rolls_back_after_failed_health_check() {
        let directory = test_config_dir();
        let path = write_test_config(&directory, "{\"old\":true}");
        let operations = FakeApplyOps::new(true, &[true, true], &[false, true]);
        let error = apply_config_transaction(&path, "{\"new\":true}", &operations).await.unwrap_err();
        assert!(matches!(error, ControlError::Failed(_)));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"old\":true}");
        assert!(backup_files(&directory).is_empty());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn config_transaction_preserves_backup_when_rollback_health_is_unknown() {
        let directory = test_config_dir();
        let path = write_test_config(&directory, "{\"old\":true}");
        let operations = FakeApplyOps::new(true, &[true, false], &[false]);
        let error = apply_config_transaction(&path, "{\"new\":true}", &operations).await.unwrap_err();
        assert!(matches!(error, ControlError::OutcomeUnknown(_)));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"old\":true}");
        let backups = backup_files(&directory);
        assert_eq!(backups.len(), 1);
        assert_eq!(std::fs::metadata(&backups[0]).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::read_to_string(&backups[0]).unwrap(), "{\"old\":true}");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn config_validation_failure_keeps_current_file_and_creates_no_backup() {
        let directory = test_config_dir();
        let path = write_test_config(&directory, "{\"old\":true}");
        let operations = FakeApplyOps::new(false, &[], &[]);
        let error = apply_config_transaction(&path, "{\"new\":true}", &operations).await.unwrap_err();
        assert!(matches!(error, ControlError::Failed(_)));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"old\":true}");
        assert!(backup_files(&directory).is_empty());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn config_diagnostic_hides_secret_bearing_lines_and_caps_output() {
        let secret_line = safe_diagnostic(b"password=very-secret", Some(1));
        assert!(secret_line.contains("可安全显示"));
        assert!(!secret_line.contains("very-secret"));
        let long = vec![b'a'; MAX_DIAGNOSTIC_CHARS + 100];
        assert!(safe_diagnostic(&long, Some(1)).len() <= MAX_DIAGNOSTIC_CHARS);
    }

    #[test]
    fn control_actions_have_fixed_methods_and_init_commands() {
        for (method, action, systemd_verb, openrc_verb) in [
            ("singbox.start", ControlAction::Start, "start", "start"),
            ("singbox.stop", ControlAction::Stop, "stop", "stop"),
            ("singbox.restart", ControlAction::Restart, "restart", "restart"),
            ("singbox.reload", ControlAction::Reload, "reload", "restart"),
        ] {
            assert_eq!(ControlAction::from_method(method), Some(action));
            assert_eq!(action.method(), method);
            assert_eq!(
                service_action_command(ServiceManager::Systemd, action),
                ("systemctl", [systemd_verb, SING_BOX_SERVICE])
            );
            assert_eq!(
                service_action_command(ServiceManager::OpenRc, action),
                ("rc-service", [OPENRC_SERVICE, openrc_verb])
            );
        }
        assert_eq!(ControlAction::from_method("shell.exec"), None);
    }

    #[test]
    fn configured_init_manager_wins_and_legacy_installations_are_detected() {
        assert_eq!(service_manager_from(Some("openrc"), false, true), ServiceManager::OpenRc);
        assert_eq!(service_manager_from(Some("systemd"), true, false), ServiceManager::Systemd);
        assert_eq!(service_manager_from(None, true, false), ServiceManager::OpenRc);
        assert_eq!(service_manager_from(None, true, true), ServiceManager::Systemd);
        assert_eq!(service_manager_from(None, false, false), ServiceManager::Systemd);
    }

    #[test]
    fn control_preconditions_reject_missing_service_and_required_config() {
        for action in
            [ControlAction::Start, ControlAction::Stop, ControlAction::Restart, ControlAction::Reload]
        {
            assert_eq!(
                check_preconditions(action, false, false, true),
                Err(ControlError::Failed("sing-box 服务不存在".into()))
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
    fn config_metadata_hashes_bytes_without_returning_the_content() {
        let path = test_config_path();
        std::fs::write(&path, b"{}").unwrap();
        let metadata = config_metadata_at(&path).unwrap();
        assert_eq!(metadata.sha256, "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a");
        assert!(metadata.modified_at.is_some());
        std::fs::remove_file(path).unwrap();
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

    #[test]
    fn openrc_service_state_requires_our_service_file() {
        assert_eq!(parse_openrc_service_status(false, true), (false, false));
        assert_eq!(parse_openrc_service_status(true, true), (true, true));
        assert_eq!(parse_openrc_service_status(true, false), (true, false));
    }
}
