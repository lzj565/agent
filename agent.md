# Agent 仓库协作约定

- 可以使用 GitHub CLI `gh` 管理仓库 Secret、查询/重跑 GitHub Actions、检查 Release。当前机器命令为 `/opt/homebrew/bin/gh`，使用已登录账号；不要输出认证 token 或私钥。
- 发布签名私钥由本地安全配置管理；上传 Secret 时通过标准输入读取，不把私钥或其内容写入仓库。Agent 发布使用仓库 Secret `RELEASE_SIGNING_PRIVATE_KEY`，公钥须与 Hub 一致。

- 每次修改本仓库，都在完成检查后创建 Git commit。不要把其他仓库的变更放进这个提交。
- 未收到明确的 push 要求时，不 push，也不创建 tag。
- 收到 push 要求时，将 `Cargo.toml` 的 patch 版本递增到下一个可用版本（例如 `1.1.0` → `1.1.1`），同步更新 `Cargo.lock`，提交版本变更，并创建与版本一致的 `vX.Y.Z` tag。检查远端已有 tag，避免复用或移动 tag。
- 推送当前分支及对应 tag。tag push 会触发 `.github/workflows/release.yml`；分支 push 会触发 `.github/workflows/ci.yml`。
- 推送后等待约两分钟，检查本次推送对应的 GitHub Actions 运行状态，并向用户报告结果；若仍在运行，明确报告当前状态。
