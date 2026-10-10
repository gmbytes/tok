# tok
实验性游戏服务器框架

目标平台：Windows amd64 和 Linux amd64（x86_64）。

设计约定与后续设计范围见 [设计文档](docs/design.md)。

项目命令使用 [just](https://just.systems/)，在仓库根目录执行 `just` 查看可用任务。

## 工程结构

框架采用 Cargo workspace，库位于 `crates/`，具体游戏组装最终二进制。`examples/demo-game` 展示游戏协议、角色配置和内存服务目录。

当前已提供工程骨架、配置校验、内存目录及 protobuf 构建流程。网络监听、session 执行器、Redis 所有权、Kubernetes 发现和 NATS 消费尚未实现；运行示例会输出配置及目录信息后退出。

## 开发命令

需要安装 rustup 和 just。仓库固定 Rust 1.99.0；protoc 由构建依赖提供，无需额外安装。

```text
just check
just verify
just proto
just run
just config-check examples/demo-game/config/gateway.toml
just config-check examples/demo-game/config/game.toml
just build
```

`just fmt` 格式化代码，`just verify` 检查格式、clippy 和测试。`just proto` 在协议输入变更时触发构建期生成，生成代码位于 Cargo 的 OUT_DIR，不提交生成的 Rust 文件。

`just build` 输出当前平台的 `target/release/demo-game.exe` 或 `target/release/demo-game`。同一二进制通过 `--config` 选择角色。当前 Kubernetes 配置仅支持 `--check-config` 校验，实际运行会明确报错。

CI 在 Windows 和 Linux 的 amd64 runner 上分别执行 just 检查、测试及 release 构建。
