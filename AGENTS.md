# 协作规范

- `agent` 和 `monitor` 是两个独立项目；涉及两个项目时，分别处理并分别提交。
- 每次只做完成当前目标所需的最少修改，并按项目分别提交。
- 输出实施计划时使用中文。
- 新增或修改代码注释时使用中文，优先说明有助于理解实现或排查问题的信息，避免重复描述显而易见的代码。

## 验证环境

- 可通过 `ssh root@10.0.0.5` 登录验证服务器，并在该机运行部署或集成验证。
- 不要将服务器密码、私钥或其他凭据写入仓库文档。


# AGENTS.md

> 本文件用于约束 Codex / AI Coding Agent 在本项目中的行为。  
> 目标：**最小改动、保持兼容、禁止越权、阶段性交付、避免“为了完成任务而重构整个项目”**。
>
> 适用仓库：
>
> - `monitor`: https://github.com/lzj565/monitor.git
> - `agent`: https://github.com/lzj565/agent.git
> - `monitor-document`: https://github.com/monitor-probe/monitor-document.git
>
> 当前改造方向：
>
> **在现有 Monitor + Agent 探针体系上，逐步增加 Xray / sing-box 节点管理、配置下发、用户流量统计、订阅管理和分流规则能力。**

---

# 1. 最高优先级原则

以下规则优先级高于“快速完成需求”。

## 1.1 禁止大规模重构

除非任务明确要求，否则禁止：

- 重写现有架构。
- 替换 Axum。
- 替换 SQLite。
- 替换 React 技术栈。
- 替换 Agent WebSocket 协议。
- 引入 Redis。
- 引入 Kafka / RabbitMQ / NATS 等消息队列。
- 引入微服务。
- 引入新的 RPC 框架。
- 因“代码更优雅”而修改与当前任务无关的模块。
- 为完成一个小功能同时重命名大量文件、函数、结构体。
- 修改现有 API 返回结构，除非任务明确要求。
- 删除现有监控功能。

原则：

```text
能加一个模块解决，就不要重构三个模块。
能新增字段解决，就不要重写整个模型。
能复用现有 WebSocket，就不要创建第二套 Agent 控制通道。
```

## 1.2 每次只做当前 Phase

如果任务声明：

```text
当前只完成 Phase 1
```

则只允许实现 Phase 1。

禁止自行提前实现：

- 下一阶段数据库表。
- 下一阶段 UI。
- 下一阶段 API。
- 下一阶段配置生成器。
- “顺便把以后可能用到的东西也写了”。

完成当前 Phase 后必须停止。

## 1.3 修改前必须先读现有代码

禁止根据猜测直接编码。

每次修改前至少检查与任务有关的：

### monitor

```text
src/main.rs
src/agent_ws.rs
src/api.rs
src/auth.rs
src/db.rs
Cargo.toml
```

如果涉及前端：

```text
web-admin/
package.json
现有 API client
现有页面路由
现有组件风格
```

如果涉及安装：

```text
install.sh
install-hub.sh
scripts/
systemd 配置
```

### agent

```text
src/main.rs
src/collect.rs
Cargo.toml
安装脚本 / systemd
```

必须优先复用现有实现，而不是重新造同功能代码。

---

# 2. 项目边界

系统分为以下领域：

```text
System Monitoring
Proxy Control
Proxy Configuration
Proxy User
Proxy Traffic
Subscription
Routing Rules
```

这些领域不得随意混合。

## 2.1 Node 的含义固定

现有 `node` 代表：

```text
一台服务器 / VPS / Agent
```

禁止把以下字段大量直接塞入 `node`：

```text
singbox_port
xray_uuid
reality_private_key
subscription_token
proxy_user_id
inbound_json
user_traffic
```

代理业务必须使用独立模型。

推荐：

```text
node
  └── proxy_instance
        └── proxy_node
```

## 2.2 系统流量和用户代理流量必须分离

现有：

```text
traffic
```

表示服务器网卡流量。

未来用户流量必须使用独立模型，例如：

```text
proxy_user_traffic
```

禁止复用 `traffic` 表记录代理用户流量。

## 2.3 OS Metrics 与 Proxy Metrics 分离

保留：

```text
report
```

用于：

```text
CPU
Memory
Disk
Network
Processes
System Metrics
```

代理运行状态/用户流量使用独立消息，例如：

```text
proxy.report
```

禁止不断往原有 `report` 中追加大量 Proxy 字段。

---

# 3. Agent ↔ Monitor 通信约束

现有通信基础：

```text
WebSocket
JSON-RPC 风格消息
Bearer Token
```

必须复用。

禁止新增：

```text
Agent HTTP polling
第二条 WebSocket
gRPC
MQ
Redis Pub/Sub
```

除非后续明确证明现有通道无法满足需求，并得到人工确认。

## 3.1 Command 协议

服务端主动控制 Agent 必须通过统一 Command。

推荐格式：

```json
{
  "jsonrpc": "2.0",
  "method": "command",
  "params": {
    "request_id": "uuid",
    "action": "singbox.status",
    "payload": {}
  }
}
```

Agent 返回：

```json
{
  "jsonrpc": "2.0",
  "method": "command.result",
  "params": {
    "request_id": "uuid",
    "success": true,
    "data": {}
  }
}
```

失败：

```json
{
  "jsonrpc": "2.0",
  "method": "command.result",
  "params": {
    "request_id": "uuid",
    "success": false,
    "error": "human readable error"
  }
}
```

## 3.2 request_id 必须唯一

每次 Command：

- 必须有唯一 `request_id`。
- 返回结果必须携带原 `request_id`。
- Monitor 必须通过 `request_id` 找到等待中的请求。
- 不允许通过 action 名称匹配结果。
- 不允许默认“最后一个请求就是这个结果”。

## 3.3 Command 必须有超时

所有远程命令必须设置 timeout。

禁止：

```rust
await forever
```

超时必须：

1. 清理 `pending_commands`。
2. 返回明确 timeout error。
3. 不影响 Agent WebSocket 主循环。
4. 不影响其他命令继续执行。

## 3.4 Agent 离线必须立即失败

如果：

```text
agents[node_id]
```

不存在，则：

- 不创建 pending command。
- 不等待 timeout。
- 立即返回 `agent offline`。

## 3.5 未知 request_id 不得 panic

如果收到：

```text
command.result
```

但 request_id 已：

- 超时；
- 被清理；
- 不存在；

则只记录 debug/warn。

禁止：

```rust
unwrap()
expect()
panic!()
```

---

# 4. 严禁通用远程 Shell

这是安全红线。

禁止实现：

```text
exec
shell
run_command
execute_script
bash
sh
```

这类允许 Hub 任意发送 shell 字符串的接口。

禁止类似：

```json
{
  "action": "exec",
  "payload": {
    "command": "rm -rf ..."
  }
}
```

禁止：

```rust
Command::new("sh")
    .arg("-c")
    .arg(user_input)
```

## 4.1 只能使用白名单 Action

例如：

```text
singbox.status
singbox.start
singbox.stop
singbox.restart
singbox.reload
singbox.config.check
singbox.config.apply

xray.status
xray.start
xray.stop
xray.restart
xray.reload
xray.config.check
xray.config.apply
```

Agent 必须使用明确 dispatch，例如：

```rust
match action {
    // supported actions
    _ => return Err(CommandError::UnsupportedAction),
}
```

未知 action 必须返回：

```text
unsupported action
```

---

# 5. 外部进程调用安全规范

如果确实需要调用：

```text
systemctl
sing-box
xray
```

必须满足：

- executable 固定。
- 参数列表固定或严格验证。
- 禁止用户输入拼接 shell。
- 禁止 `sh -c`。
- 禁止 `bash -c`。
- 必须读取 exit status。
- stdout/stderr 必须受长度限制。
- 错误信息禁止泄漏 token/password/private key。
- 必须设置执行超时。

正确：

```rust
Command::new("systemctl")
    .arg("restart")
    .arg("sing-box")
```

禁止：

```rust
Command::new("sh")
    .arg("-c")
    .arg(format!("systemctl restart {}", service))
```

---

# 6. Agent 权限规范

当前 Agent 原始定位是低权限探针。

如果新增 Proxy 控制能力：

- 不允许在各业务函数中随意调用 sudo。
- 权限相关逻辑必须集中封装。
- 不得把 root 权限逻辑散落在整个代码库。
- MVP 如果暂时需要 root，必须明确标记为技术债。
- 后续目标应允许迁移到 privileged helper。

禁止为了“能跑”而：

```text
chmod 777
全目录写权限
禁用全部 systemd sandbox
关闭所有安全策略
```

---

# 7. 配置文件下发规范

禁止直接：

```text
覆盖 config.json
→ restart
```

标准流程必须是：

```text
生成新配置
    ↓
写入临时文件
    ↓
配置语法校验
    ↓
备份旧配置
    ↓
atomic replace
    ↓
reload/restart
    ↓
health check
```

失败时：

```text
rollback
```

## 7.1 必须先 check 再 apply

例如 sing-box：

```text
sing-box check -c temp.json
```

Xray 使用对应配置校验机制。

配置校验失败时：

- 禁止覆盖当前配置。
- 禁止重启服务。
- 返回 stderr 中有意义的错误。
- 对错误长度进行限制。

## 7.2 配置写入必须原子化

禁止：

```rust
File::create(real_config)
```

直接覆盖生产配置。

应该：

```text
config.json.new
↓
fsync（适用时）
↓
rename
```

尽量避免服务读取到半份配置。

---

# 8. 密钥和敏感信息

以下内容属于敏感信息：

```text
Agent Token
Subscription Token
用户密码
Reality Private Key
证书私钥
API Secret
```

禁止：

- 写入普通日志。
- Debug 输出整个请求 JSON。
- Debug 输出完整配置。
- API error 原样回显敏感内容。
- 前端 `console.log` 敏感信息。
- 测试快照包含真实 token。

日志必须脱敏。

例如：

```text
token=45ac****c667
```

## 8.1 密码禁止明文保存

用户密码必须使用可靠 password hash。

禁止：

```text
MD5
SHA1
SHA256(password)
plaintext
```

优先复用项目已有安全库/方案。

---

# 9. API 设计规则

新增 API 必须优先遵循项目已有 response/error 风格。

禁止同一项目无理由混用：

```json
{"ok": true}
```

```json
{"success": 1}
```

```json
{"code": 0}
```

## 9.1 HTTP 状态码必须合理

例如：

```text
400 参数错误
401 未认证
403 无权限
404 资源不存在
409 状态冲突
422 配置校验失败
500 内部错误
502 Agent 执行失败（如果符合项目现有规范）
504 Agent timeout
```

如项目已有规范，以现有规范为准。

## 9.2 不允许前端决定安全边界

前端隐藏按钮不等于权限控制。

后端必须重新验证：

```text
用户权限
node 是否存在
资源所有权
action 是否允许
参数是否合法
```

---

# 10. 数据库规范

现阶段继续使用：

```text
SQLite + rusqlite
```

禁止擅自换数据库。

## 10.1 Schema 变更必须 Migration

禁止：

```text
让用户手工删除 DB
启动时 DROP TABLE
直接改 CREATE TABLE 且忽略旧版本
```

必须遵循现有：

```text
PRAGMA user_version
```

migration 机制。

每个 schema 变更必须：

1. 增加 schema version。
2. 提供向前 migration。
3. 保留已有数据。
4. migration 可重复启动而不破坏数据库。

## 10.2 禁止 destructive migration

没有人工确认禁止：

```text
DROP TABLE
DROP COLUMN
清空用户数据
重建所有表
```

如确需迁移旧数据，应采用可验证的安全迁移流程。

## 10.3 数据库事务

涉及多个互相关联的写操作时必须使用 transaction。

不能出现“成功一半”的业务状态。

---

# 11. Rust 编码约束

遵循现有项目风格。

## 11.1 业务路径禁止 unwrap/expect

在以下输入上禁止：

```rust
unwrap()
expect()
```

- HTTP 输入。
- WebSocket 输入。
- JSON。
- DB 查询。
- 文件系统。
- 外部命令。
- 网络。
- 用户配置。

新代码默认使用：

```rust
?
match
ok_or_else
map_err
```

测试和可以严格证明不会失败的常量场景除外。

## 11.2 禁止 panic 处理业务错误

例如 Agent offline 必须作为普通错误返回，禁止：

```rust
panic!("agent offline");
```

## 11.3 不持锁跨 await

尤其注意：

```rust
RwLock
Mutex
```

禁止无必要地：

```rust
let guard = map.write().await;
some_network_call().await;
```

应该：

```text
取出必要数据
↓
drop guard
↓
await
```

## 11.4 Channel 必须考虑容量和关闭

禁止无理由创建无限增长队列。

必须考虑：

```text
backpressure
capacity
send failure
receiver drop
```

## 11.5 长耗时任务不得阻塞 Tokio runtime

文件或进程操作若可能明显阻塞，评估使用：

```rust
spawn_blocking
```

或 Tokio 对应异步 API。

---

# 12. WebSocket 稳定性

Agent WebSocket 是系统核心链路。

新增功能禁止破坏：

```text
hello
report
ping.tasks
ping.result
reconnect
```

## 12.1 一个 Command 失败不能断开整个 Agent

例如：

```text
singbox.status
```

失败时，只允许：

```text
command.result success=false
```

禁止导致：

```text
Agent WebSocket disconnect
Agent process exit
Hub panic
```

## 12.2 消息解析必须兼容未知字段

新增协议字段优先使用：

```rust
Option<T>
```

或合理默认值。

不能因为新 Hub 增加一个字段导致旧 Agent 反序列化失败并断线。

## 12.3 Hub / Agent 版本兼容

至少保证：

- 未识别 method 不 panic。
- 未识别 action 返回明确错误。
- 新增字段尽量 optional。
- 原 `report` 格式保持兼容。
- 原 `ping.tasks` 链路保持兼容。

---

# 13. Agent 代码结构约束

不要继续把所有功能塞进：

```text
agent/src/main.rs
```

但也禁止为了“架构漂亮”一次拆几十个文件。

推荐最小结构：

```text
src/
├── main.rs
├── collect.rs
├── command.rs
└── proxy/
    ├── mod.rs
    ├── singbox.rs
    └── xray.rs
```

职责：

```text
main.rs
    启动、连接、事件循环

collect.rs
    OS Metrics

command.rs
    Command 协议和 dispatch

proxy/singbox.rs
    sing-box 管理

proxy/xray.rs
    Xray 管理
```

---

# 14. Monitor 代码结构约束

优先复用：

```text
App.agents
Agent.tx
agent_ws.rs
api.rs
db.rs
```

建议逐步增加：

```text
command/
proxy/
subscription/
```

禁止复制一套新的 Agent registry。

系统只能有一个 Agent 在线状态来源。

---

# 15. Config Builder 规范

后续配置生成必须集中到 Config Builder。

禁止：

```text
API handler 自己拼 JSON
React 页面自己拼 sing-box config
Agent 自己猜业务配置
多个地方分别生成 Xray 配置
```

目标：

```text
DB Domain Model
      ↓
Config Builder
      ↓
Runtime Config
```

例如：

```text
Proxy Node
Proxy User
Routing Rule
      ↓
Config Builder
      ↓
Xray Config
sing-box Config
Clash Subscription
sing-box Subscription
```

---

# 16. Subscription 规范

Subscription Token 和 Agent Token 属于两个不同安全域。

禁止复用：

```text
node.token
```

作为：

```text
subscription token
```

必须独立。

## 16.1 客户端格式区别

### Clash / Mihomo

服务端可以生成：

```yaml
proxies:
proxy-groups:
rules:
```

### sing-box / SFA / SFI

可以生成：

```json
{
  "outbounds": [],
  "route": {
    "rules": []
  }
}
```

### v2rayN / Shadowrocket Base64 URI

主要生成：

```text
vless://
vmess://
trojan://
ss://
```

不要尝试把 Clash `rules` 写进 URI。

---

# 17. Proxy User 规范

Proxy 用户与 Monitor Admin 必须分离。

禁止默认：

```text
admin account == proxy user
```

建议将后台认证用户与代理用户设计为独立 domain。

---

# 18. 流量统计规范

用户流量统计必须考虑：

```text
counter reset
Agent restart
Core restart
Hub restart
重复 report
out-of-order report
```

禁止简单：

```text
total += current_counter
```

必须使用 delta/baseline 思路。

现有服务器网卡流量算法可作为参考，但不能混入同一数据模型。

---

# 19. 前端开发限制

必须复用现有：

```text
组件体系
布局
表单风格
Dialog
Button
Table
Toast
API Client
```

禁止为了一个页面引入第二套 UI framework。

例如项目已有 Tailwind / Radix，就不要再引入：

```text
Ant Design
MUI
Element Plus
```

## 19.1 前端不得伪造成功

禁止：

```text
点击 Restart
→ 立即显示“运行中”
```

必须等待后端 / Agent 实际结果。

推荐：

```text
loading
↓
command result
↓
refresh status
```

## 19.2 操作必须防重复提交

例如：

```text
restart
delete
apply config
reset token
```

执行中必须 disable 对应按钮。

---

# 20. 高风险操作

以下操作必须确认：

```text
删除服务器
删除 Proxy Node
删除 Proxy User
卸载 Core
重置 Token
清零流量
覆盖配置
```

高风险确认中应明确展示操作对象名称。

---

# 21. 日志规范

日志至少应能回答：

```text
哪个 node
哪个 request_id
哪个 action
成功还是失败
耗时多少
```

例如：

```text
command node_id=12 request_id=xxx action=singbox.status success=true elapsed=42ms
```

但禁止记录：

```text
token
password
private_key
完整 config
```

---

# 22. 错误处理

错误必须具有可操作性。

差：

```text
internal error
```

好：

```text
sing-box config validation failed: missing server_name
```

但必须避免泄漏敏感内容。

---

# 23. 测试要求

每个 Phase 至少补充与核心逻辑匹配的测试。

优先测试：

```text
protocol serialization/deserialization
command dispatch
unknown action
timeout cleanup
offline agent
unknown request_id
config builder
config validation
traffic delta
database migration
```

## 23.1 修 Bug 优先加回归测试

如果修复明确 bug，优先新增能复现该问题的 test。

禁止只改代码，不验证问题是否真正解决。

---

# 24. 完成代码后必须执行检查

Rust 修改至少执行：

```bash
cargo fmt --check
cargo check
cargo test
```

如果是 workspace，则执行相关 workspace/crate 检查。

前端修改至少执行项目已有的：

```text
build
lint
typecheck
test
```

先读取 `package.json`，不要自行猜命令。

---

# 25. 不允许为了让测试通过而降低质量

禁止：

```text
删除测试
skip test
注释断言
catch 所有错误后返回 success
关闭 lint
降低 TypeScript strict
加入 allow(dead_code) 掩盖问题
```

除非明确说明原因并得到人工确认。

---

# 26. 不修改无关文件

每次任务完成后必须检查：

```bash
git diff
git status
```

如果出现：

- 自动格式化大量无关文件；
- lockfile 无意义变化；
- IDE 文件；
- 临时文件；
- build artifact；

必须清理。

禁止提交：

```text
target/
node_modules/
dist/
.DS_Store
*.log
临时 config
真实 token
真实证书
```

---

# 27. 依赖引入规范

增加 dependency 前先确认：

```text
现有依赖是否已经能完成？
标准库能否完成？
这个依赖是否值得长期维护？
```

禁止为了一个简单功能引入大型依赖。

新增依赖必须在交付说明中写清：

```text
为什么需要
用于哪里
替代方案是什么
```

---

# 28. 禁止“假实现”

禁止：

```text
TODO 假装已经支持
固定返回 running=true
mock data 进入生产 API
前端写死运行状态/版本
创建按钮但后端实际不工作
API 返回 success 但 Agent 没执行
```

功能尚未实现时应明确返回：

```text
not supported
```

---

# 29. 禁止静默失败

关键业务中禁止：

```rust
let _ = do_something();
```

如果确实允许忽略错误，必须明确说明原因，并记录必要日志。

---

# 30. 并发操作保护

对于同一个 Node / Core：

```text
config.apply
restart
uninstall
install
```

不能毫无控制地并发。

后续实现时应考虑：

```text
per-node / per-core operation lock
```

至少避免：

```text
配置正在 apply
同时 uninstall
```

---

# 31. 幂等性

以下操作应尽量幂等：

```text
status
start
stop
install
uninstall
config.apply
```

重复请求不应导致未知状态。

---

# 32. 时间、流量和容量单位

后端内部统一使用明确单位。

推荐：

```text
时间：Unix timestamp / Duration
流量：bytes (u64)
端口：u16
```

禁止数据库核心字段混用：

```text
KB
MB
GB
```

展示层再转换。

---

# 33. API / DB 命名统一

推荐：

```text
Rust module: singbox
Rust type: SingboxStatus
JSON action: singbox.status
DB field: singbox_version
UI: sing-box
```

不要在同一层无规则混用：

```text
singBox
sing_box
Singbox
sing-box
```

---

# 34. 注释规范

注释应该解释：

```text
为什么这样做
边界条件是什么
协议为什么这样设计
```

不要注释显而易见的代码。

---

# 35. 文档同步

如果新增：

```text
API
Agent flag
Config
Database behavior
Protocol action
```

必须同步更新对应文档。

但不得提前写不存在功能的“假文档”。

---

# 36. Git 变更原则

一个 Phase 应保持：

```text
独立
可运行
可测试
可 review
可回滚
```

避免一个提交同时混入：

```text
协议
UI 大重构
数据库 migration
CSS 重写
无关 bugfix
```

---

# 37. Codex 开始任务前必须输出

开始编码前先简短说明：

```text
1. 我确认的现有调用链
2. 本次会修改哪些文件
3. 本次不会修改哪些模块
4. 实现方案
5. 潜在风险
```

不要写长篇空泛设计。

重点是证明已经阅读现有代码。

---

# 38. Codex 完成任务后必须输出

每次完成后必须给出：

## 修改文件

```text
path
path
path
```

## 实现内容

简要说明实际完成内容。

## 调用链

例如：

```text
HTTP
→ send_command
→ Agent.tx
→ WS
→ command dispatcher
→ command.result
→ pending command
→ HTTP response
```

## 测试结果

明确写：

```text
cargo fmt --check: PASS/FAIL
cargo check: PASS/FAIL
cargo test: PASS/FAIL
frontend build: PASS/FAIL
```

禁止只写：

```text
应该没问题
```

## 尚未实现

明确列出当前 Phase 之外没有完成的内容。

---

# 39. 遇到不确定情况时

如果遇到以下情况：

- 需求与现有架构冲突；
- 需要 destructive migration；
- 需要 root 权限重大调整；
- 需要改变现有 API；
- 需要改变 Agent 协议兼容性；
- 需要引入大型新依赖；
- 发现原需求存在明显设计问题；

**不要自行决定。**

先停止并说明：

```text
现状
问题
可选方案
推荐方案
影响范围
```

等待人工确认。

---

# 40. 推荐开发阶段

按以下顺序推进：

```text
Phase 1
Command + command.result

Phase 2
Xray / sing-box status

Phase 3
Core start / stop / restart / install / uninstall

Phase 4
Config get / check / apply / rollback

Phase 5
proxy_instance / proxy_node 数据模型

Phase 6
VLESS + Reality

Phase 7
Config Builder

Phase 8
节点链接 / QR / 聚合订阅

Phase 9
proxy_user

Phase 10
用户独立订阅

Phase 11
用户流量统计

Phase 12
分流规则

Phase 13
Hysteria2 / TUIC / VMess / Trojan / Shadowsocks

Phase 14
Realm 中转
```

没有明确指令不得跨 Phase。

---

# 41. 当前最重要的工程原则

始终遵守：

```text
正确性 > 完成功能数量
兼容性 > 大规模重构
明确协议 > 隐式行为
白名单命令 > 任意 Shell
校验 + 回滚 > 直接覆盖
模块边界 > 把所有逻辑塞进 main.rs
真实状态 > 前端假状态
可测试 > “看起来应该能跑”
```

---

# 42. 最终检查清单

每次提交前逐项检查：

- [ ] 是否只修改了当前任务需要的文件？
- [ ] 是否破坏现有 Agent `report`？
- [ ] 是否破坏现有 `ping.tasks`？
- [ ] 是否改变现有 API 而没有说明？
- [ ] 是否添加了任意 shell 执行能力？
- [ ] 是否存在用户输入进入 `sh -c` / `bash -c`？
- [ ] 是否泄漏 token / password / private key？
- [ ] 是否存在业务路径 `unwrap` / `expect` / `panic`？
- [ ] 是否持有锁跨 `.await`？
- [ ] 是否处理 Agent offline？
- [ ] 是否处理 timeout？
- [ ] 是否清理 pending request？
- [ ] 是否处理未知 action / request_id？
- [ ] 是否存在 destructive DB migration？
- [ ] 是否把 Proxy 业务污染进现有 `node` / `traffic` / `report`？
- [ ] 是否直接覆盖生产 config？
- [ ] 是否 config check 后才 apply？
- [ ] 是否考虑 rollback？
- [ ] 是否加入不必要依赖？
- [ ] 是否生成假数据 / 假成功状态？
- [ ] 是否运行 format / check / test / build？
- [ ] 是否查看最终 `git diff` / `git status`？
- [ ] 是否明确说明尚未实现的内容？

任一关键项不满足，不应宣称任务已经完成。
