# tok

实验性的 Rust 游戏服务器框架，目标平台为 Windows amd64 和 Linux amd64。

框架以“服务 + 处理链”为核心：Factory 初始化状态、入口和中间件，运行设施监督主任务与后台工作。RPC、事件、timer 和网关业务共用 Request / Tower 执行协议；tok-gateway 是增加连接/session 能力的内置扩展。Application 按配置组装服务，纯事件服务可以不绑定网络端点。

通用基础契约位于 tok-core，执行与生命周期位于 tok-service；玩家会话、消息、计时需求和所有权契约独立为 tok-player，网关和游戏协议适配复用它。tok-rpc 提供通用连接、端点与执行设施；tok-game-rpc 承载游戏消息、玩家事件和 timer 协议，门面分别从 rpc / game_rpc 导入，通用 RPC 和服务层不依赖玩家契约。

## 文档入口

| 要了解的内容 | 文档 |
| --- | --- |
| 文档分工与阅读顺序 | [文档导航](docs/README.md) |
| 总体架构、分层与系统边界 | [总体设计](docs/design.md) |
| 上下文、中间件、事件与动态主题契约 | [服务运行模型](docs/service-runtime.md) |
| 当前可用能力与缺口 | [实现状态](docs/status.md) |
| 方向校正的实施与验收记录 | [七批实施计划](docs/implementation-plan.md) |
| 已实现 API、组装和开发操作 | [Rust 用户指南](crates/tok/docs/guide.md) |
| Frame v3、ACK、恢复和交接 | [客户端与会话协议](docs/client-session.md) |

用户/API 文档走 Rust 原生 rustdoc 流程：`just docs`，打开 `target/doc/tok/index.html`。指南示例随文档测试验证，CI 保存生成的 HTML。架构目标不作为可运行示例，历史验收另行归档。

## 当前能力

已接通配置与服务工厂、共享 gRPC 端点、强类型 RPC、Memory/Redis 发现与所有权、请求级锁、TCP 认证与串行 session、timer、下行序号与 ACK 缓存、同网关恢复、可选跨网关交接、本地事件及 Core NATS 即时事件。示例 player 和 guild 共用内部端点，可验证客户端状态读写及服务调用。

Application::register_rpc_factory 自动将 RPC 绑定注入所属 Factory，自定义 protobuf 使用 bind_rpc，处理链未安装时不发布。Event::topic 决定路由，Pool 聚合固定及会话兴趣；网关提供状态工厂、生命周期回调、准入/执行/新下行 Layer 和显式交接快照。demo 在登录开放前通过可信策略加入区服主题。

发布成功只表示 NATS 客户端接受操作，不等于远端业务完成；仅订阅建立确认就绪。客户端不能发布服务器事件，所有 player 执行经 session FIFO。Memory 只用于单进程，跨网关需共享所有权和明确配置的签名密钥/内部端点。生产安全、Kubernetes、Scene、JetStream、离线加载及 Redis Cluster 尚未完成。

## 快速开始

仓库固定 Rust 1.99.0，使用 just 1.58.0；protoc 由构建依赖提供。工具链配置见 [rust-toolchain.toml](rust-toolchain.toml)。

```text
just check
just verify
just docs
just run
```

`just application-smoke` 验证普通 RPC 与无端点事件服务共用 Layer；`just run` 运行本地 TCP 与内部 RPC，Ctrl+C 协作退出。示例凭证为 `demo:<正整数玩家 ID>`，仅用于开发。默认参数、配置与认证/错误处理用法见 Rust 用户指南。

```text
just application-smoke
just smoke
just rpc-smoke
just client-smoke
just build
```

build 输出当前平台的 `target/release/demo-game.exe` 或 `target/release/demo-game`。同一平台使用同一游戏二进制，通过配置选择服务：

```text
just config-check examples/demo-game/config/game.toml
just config-check examples/demo-game/config/gateway.toml
just run examples/demo-game/config/local.toml
```

## 多进程与真实集成

| 场景 | 入口 | 依赖 |
| --- | --- | --- |
| Redis 开发发现 | redis-game.toml / redis-gateway.toml；`just redis-test redis-smoke` | 真实 Redis，默认本机 6379 |
| session 共享所有权 | `just ownership-test ownership-smoke` | Redis 与独立进程 |
| 请求级锁 | `just locks-test` | Redis |
| 跨网关交接 | redis-handoff-a.toml / redis-handoff-b.toml；`just handoff-smoke` | 共享所有权、共用密钥与内部 RPC |
| 即时事件 | events.toml；`just nats-test nats-smoke` | 真实 NATS，默认本机 4222 |

配置文件位于 [demo config](examples/demo-game/config)，字段与完整操作见用户指南。Redis 和 NATS 专项分别可用 TOK_TEST_REDIS_URL、TOK_TEST_NATS_URL 指定地址，测试使用独立命名空间。常规 verify 不要求这些外部服务，不把未执行的专项算作通过。

Kubernetes 配置当前仅可校验，运行该发现后端会明确报未实现；[部署目录](deploy/k8s/README.md)尚无可运行清单。

## 工程约定

库位于 `crates/`，游戏在 `examples/demo-game` 组装二进制。框架协议属于 tok-proto，游戏协议属于 demo；protobuf 及消息 ID 关联在构建期生成到 Cargo OUT_DIR，不提交生成的 Rust 文件。

公开 API 使用中文文档，missing_docs 与 rustdoc 检查由 workspace 统一执行；非测试代码的 unwrap/expect 需要局部说明。完整 lint 说明见用户指南，协作与中文 Conventional Commits 规则见 [AGENTS.md](AGENTS.md)。

全部工程文本使用 LF；Git、EditorConfig 和 rustfmt 已配置，`just line-endings-check` 在常规测试中拒绝 CRLF/CR。Windows/Linux 分别验证，本机结果不能替代另一平台 CI 或生产压测。
