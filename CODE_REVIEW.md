# 代码审查与修复记录

审查基线：`bdb4093`（1.1.6）；仅修改 agent 的 dev 分支，不 push、不创建 tag、不变更版本。
任务按以下顺序独立修复、验证并提交。已有活动 goal，继续使用，不重复创建。

| 序号 | 优先级 | 问题与位置 | 修复方案与验收条件 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | 高 | `src/api_config.rs`：临时配置写入后才设置权限 | 创建时 0600；验证写入前权限和最终原权限保留 | 已完成 |
| 2 | 中 | `src/singbox.rs`：只检查输入大小，转换后可能超过读取限制 | 转换后再次检查 1 MiB；超限不修改原配置，成功应用可读取 | 已完成 |
| 3 | 高 | `src/main.rs`：远程命令无限 spawn，断线后排队任务仍执行 | 最多 4 个执行、16 个排队，超限返回 BUSY；断线丢弃未开始命令，已开始配置事务完成 | 已完成 |
| 4 | 高 | `src/main.rs`：代理流量查询占用网络循环最多 3 秒 | 单个后台任务、有界结果通道；API 超时不影响上报、心跳与命令，保留日志抑制 | 已完成 |
| 5 | 中 | `src/collect.rs`：流量 epoch 只包含名称 | 纳入 ifindex，检测单网卡回退；覆盖同名重建、汇总掩盖回退和排序变化 | 已完成 |
| 6 | 中 | `.github/workflows`：dev 推送无 CI，tag 发布无质量门禁 | CI 覆盖 dev，发布前通过 fmt、Clippy、测试 | 已完成 |
| 7 | 优化 | `src/collect.rs`：同步采集阻塞单线程网络运行时 | 单个后台工作任务保持 Collector 状态、串行采样，无任务堆积；慢采集不阻塞网络 | 已完成 |
| 8 | 优化 | `src/main.rs`：退避缺少随机抖动 | 加入随机偏移，总等待 1–60 秒；覆盖边界与稳定会话重置 | 待修复 |

## 验证记录

- 初始工作区干净，main 基线：`cac7d3964ebaa98930808338add20508c677db85`。
- 本机为 macOS，PATH 无 cargo；已建立独立 OrbStack Linux ARM64 Rust 容器，构建缓存位于容器 /tmp。
- 第 1 项：Linux `cargo test --locked api_config::tests` 4/4 通过，`cargo fmt --all --check` 通过。创建失败不删除已有临时文件；替换后保留原权限。
- 最终要求：`cargo fmt --all --check`、`cargo clippy --all-targets --locked -- -D warnings`、`cargo test --locked`，Linux 专属行为需在 Linux 验证。未执行的检查不标记通过。

- 第 2 项：Linux `cargo test --locked singbox::tests` 8/8 通过，格式检查通过；覆盖恰好 1 MiB 输入转换后超限、拒绝前不启动子进程、不修改原配置，以及应用后读取与临时文件清理。

- 第 3 项：新增 `src/commands.rs`，全进程共享 4 个执行许可，每连接最多排队 16 条；完成结果等待发送期间仍占用许可。Linux 全套 43/43 测试、Clippy（-D warnings）、格式检查通过；覆盖 BUSY 与请求 ID、FIFO、取消等待、跨连接额度、断线完成应用及回滚。

- 第 4 项：Linux 挂起 API 的两项测试通过，fmt 与 Clippy 通过；真实 WebSocket 在 API 等待期间收到两份上报、Pong 和命令回复，查询 3 秒超时，连接结束清理后台任务。回归测试另发现 Close 帧后仍尝试上报，已改为收到 Close 即退出会话。

- 第 5 项：Linux 采集相关 17/17 测试、fmt、Clippy 通过；epoch 增加 ifindex 与回退序号，同名新设备不继承旧速率基线；覆盖汇总仍增长时单网卡 RX/TX 回退、顺序变化不影响摘要。保持字段不变，hub 按不透明字符串比较；README 同步说明。

- 第 6 项：actionlint v1.7.12 验证全部工作流通过；发布依赖链为 check → build → release。Linux 实际执行新增门禁：fmt、Clippy 通过，测试 46/46 通过。未 push，故没有远端 Actions 运行结果。

- 第 7 项：新增 `src/metric_worker.rs`，全进程单独线程拥有 Collector，容量 1 的请求通道；网络循环每次仅等待一个采样 Future，未开始且已取消的请求跳过。Linux 全套 49/49、fmt、Clippy 通过；覆盖单线程状态保留、跨连接基线、有界队列、取消，以及真实 WebSocket 在阻塞采样时 500ms 内完成 Pong/命令回复。
