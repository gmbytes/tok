# tok
实验性游戏服务器框架

目标平台：Windows amd64 和 Linux amd64（x86_64）。

设计约定与后续设计范围见 [设计文档](docs/design.md)。

现状分析、分阶段任务及验收条件见 [实施大纲](docs/implementation-plan.md)。

用户指南与 API 文档走 Rust 原生 rustdoc 流程：执行 `just docs`，打开 `target/doc/tok/index.html`。指南源文件位于 [guide.md](crates/tok/docs/guide.md)，Rust 示例随常规测试验证；CI 保存生成的文档产物。

workspace 启用 `missing_docs = "deny"`，对外公开的模块、类型、字段、枚举变体和方法缺少文档注释时，编译与常规检查会失败。仅 protobuf 自动生成模块豁免；手写代码必须维护文档注释，rustdoc 同时检查文档链接和标记语法。

项目命令使用 [just](https://just.systems/)，在仓库根目录执行 `just` 查看可用任务。

## 工程结构

框架采用 Cargo workspace，库位于 `crates/`，具体游戏组装最终二进制。`examples/demo-game` 展示游戏协议、服务配置和内存服务目录。

当前已提供配置校验、内存目录与变更订阅、bid 路由、单进程内存所有权、protobuf 生成、服务生命周期、可观测组件及共享内部 gRPC 端点。PlayerGateway 提供 TCP 登录、认证扩展点、有界串行 session 和通知入口。示例支持玩家状态读写及 player 调用 guild；按 Ctrl+C 优雅退出。timer、断线恢复、Redis、Kubernetes 发现和 NATS 消费按后续批次推进。

网关统一命名为 `PlayerGateway` 和 `SceneGateway`，分别对接 player 与场景服务。`tok-gateway` 中的 `player/` 放置玩家逻辑 session、timer、下行补发及重连交接，`scene/` 放置场景接入和场景连接 session；`transport/`、`security/`、`codec/` 提供按需组合的接入组件。示例玩家网关服务名为 `player-gateway`。

## 开发命令

需要安装 rustup 和 just 1.58.0。仓库固定 Rust 1.99.0；protoc 由构建依赖提供，无需额外安装。若 `RUSTUP_TOOLCHAIN` 覆盖为旧版本，启动前应清除覆盖或设为 1.99.0。

```text
just check
just verify
just proto
just docs
just run
just smoke
just rpc-smoke
just client-smoke
just config-check examples/demo-game/config/gateway.toml
just config-check examples/demo-game/config/game.toml
just build
```

`just fmt` 格式化代码，`just verify` 检查格式、clippy 和测试。`just proto` 在协议输入变更时触发构建期生成，生成代码位于 Cargo 的 OUT_DIR，不提交生成的 Rust 文件。

`just run` 持续运行客户端 TCP 与内部 RPC，`just smoke` 运行一秒后通过同一退出流程结束。`just rpc-smoke` 验证内部调用；`just client-smoke` 使用真实 TCP 完成登录、状态读取、状态变更和公会查询，然后优雅退出。也可传入 `--run-for <秒数>`。Linux 额外支持 SIGTERM，适配容器停止。服务默认初始化期限为 30 秒，整体退出期限为 10 秒，游戏可通过 `Application::with_timeouts` 调整。

示例凭证为 `demo:<正整数玩家 ID>`，只用于本地开发。TCP 使用 4 字节大端长度和 protobuf Frame，协议及状态机见 [客户端与 session 约定](docs/client-session.md)。身份以认证后的 session 为准，同玩家请求串行，不同玩家可以并发。队列满返回明确错误；重复登录先关闭旧入口并收束旧调用，断线立即释放会话。完整帧的读写期限也包含空闲等待，本批尚无心跳和断线宽限期。

`[player_gateway]` 配置连接、session、执行队列、下行缓冲、帧长度与期限；默认值见 local.toml。此批为明文 TCP 和单进程内存协调，示例状态是有界内存数据，生产认证、安全、持久化和跨网关恢复按后续阶段实施。

游戏通过 `ServiceFactory` 注册各类服务，通过 `Application::register_rpc_endpoint` 显式注册共享 gRPC 端点。应用先绑定监听，业务初始化完成后才加入目录并开启对应的就绪入口；未就绪调用返回 Unavailable。监听或服务启动失败、超时、崩溃或提前结束会触发全进程清理，撤销目录并通知任务退出。服务在取消后负责清理自己的资源，超出退出期限会中止任务。

进程配置 `node_id` 和 `endpoints`，每个端点配置 `name`、`protocol` 和 `address`；各服务通过 `endpoint` 引用端点。多个 protobuf 服务可通过 `RpcEndpoint` 绑定到同一内部 gRPC 端点，每个端点只监听一次。同一 protobuf 服务不能在一个端点重复绑定。示例监听 internal-rpc 和 client，配置描述本身不自动创建监听，TCP 由网关工厂准备。

服务通过 `(node_id, service_name)` 定位，服务名在节点内唯一，不同节点可使用相同服务名。游戏通过 `Application::register("guild", factory)` 按服务名注册实现，目录通过 `snapshot("guild")` 查询该服务的节点及绑定端点的协议、地址。业务实体仍通过 bid 路由和互斥。

```toml
[[endpoints]]
name = "internal-rpc"
protocol = "grpc"
address = "127.0.0.1:7001"

[[services]]
service_name = "player"
endpoint = "internal-rpc"

[[services]]
service_name = "guild"
endpoint = "internal-rpc"
```

RPC 使用 `CallContext`、`HandlerRegistry` 和 `RpcService` 分离业务上下文、强类型分发与 gRPC 接入。`RpcClients` 有界复用 Channel，设置调用超时且不重试；通过 metadata 传播 W3C TraceContext。用户可用 `TowerHandler` 接入代码组装的中间件链，决定选择、顺序和作用范围。请求级锁契约已定义，Redis 后端、锁 Layer 和中间件配置表达后续实现，普通业务入口不自动加锁。

`just build` 输出当前平台的 `target/release/demo-game.exe` 或 `target/release/demo-game`。同一二进制通过 `--config` 选择服务。当前 Kubernetes 配置仅支持 `--check-config` 校验，实际运行会明确报错。

CI 在 Windows 和 Linux 的 amd64 runner 上分别执行 just 检查、测试及 release 构建。

## 日志、指标与追踪

`tok-observability` 统一初始化 tracing 日志、Prometheus 指标和 OpenTelemetry 追踪，`tok::observability` 提供访问入口。每个进程初始化一次，库和业务通过 tracing/metrics 埋点，服务结束后调用 `Observability::shutdown` 关闭指标端口并刷新追踪。

配置可选，默认 INFO 文本日志、关闭指标端口和追踪导出。生产示例使用 JSON 日志输出到 stdout；部署平台负责日志采集。可以在 TOML 中添加：

```toml
[observability]
log_filter = "info,tok_gateway=debug"
log_format = "json"
metrics_address = "127.0.0.1:9090"
otlp_endpoint = "http://127.0.0.1:4317"
trace_filter = "info"
trace_sample_ratio = 0.1
```

`RUST_LOG` 覆盖初始日志规则；`telemetry.logs.filter()` 读取当前规则，`telemetry.logs.set_filter("info,tok_gateway=debug")` 动态更新。规则无效时返回错误并保留原规则。框架提供控制句柄，管理 HTTP/RPC 接口或配置监听器后续接入。日志过滤与追踪过滤、采样互相独立。

指标端口提供 `/metrics`，支持计数、Gauge 和耗时 Histogram；现有埋点包括服务启动次数、启动耗时、运行服务数及运行失败次数。默认延迟桶从 1 毫秒到 30 秒。player_id、bid、request_id 等不作为指标标签。

追踪通过 OTLP gRPC 导出到 Collector，使用 W3C TraceContext 传播器和父级采样决策，采样率作用于新建根 trace。JSON 日志包含 service/node_id，进入有效追踪 span 时同时包含 trace_id/span_id。已接入服务初始化和 RPC 调用 span，RPC metadata 传播上下文；NATS headers 传播随事件组件实现。token 和协议正文默认不记录。

强追踪规则命中时直接采样，优先于父级决定、普通追踪过滤和 ratio。支持 span 名称、target 和字段的精确匹配；一条规则的全部条件必须满足，多条规则任意命中即可。字段值支持字符串、数字和布尔值。下面的规则强制采集 player_id 为 123 的 client_request：

```toml
[[observability.trace_force_rules]]
span = "client_request"
fields = { player_id = 123 }
```

`telemetry.traces.config()` 读取采样配置，`set_config(SamplingConfig)` 原子更新过滤、ratio 和规则；`set_force_rules(Vec<ForceTraceRule>)` 单独更新规则，空列表清除规则。无效配置保留原值，更新影响后续采样决定，已开始的 span 保持原决定。强追踪子 span 继承已采样父级，即使过滤规则为 off；规则移除也不截断已开始的链路。

`trace_filter` 使用级别和模块规则，如 `info,demo_game::player=debug`，决定普通根 span 的采样资格；span 字段条件通过强追踪规则配置。为避免字段规则被提前排除，span 进入 SDK 后统一决定采样，普通根 span 仍按过滤和 ratio 丢弃。默认比例采样只用于无有效父级且未命中强规则的 span。

匹配字段应在创建 span 时提供。采样后补录字段不会重新采样；尽量在入口请求 span 上命中规则，随后传播采样决定。下游命中规则可开启从该处开始的追踪，但无法补回此前丢弃的上游 span。强制采样需要启用 OTLP 导出，不意味着追踪系统故障时保证数据送达。
