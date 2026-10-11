# tok
实验性游戏服务器框架

目标平台：Windows amd64 和 Linux amd64（x86_64）。

设计约定与后续设计范围见 [设计文档](docs/design.md)。

项目命令使用 [just](https://just.systems/)，在仓库根目录执行 `just` 查看可用任务。

## 工程结构

框架采用 Cargo workspace，库位于 `crates/`，具体游戏组装最终二进制。`examples/demo-game` 展示游戏协议、服务配置和内存服务目录。

当前已提供工程骨架、配置校验、内存目录、protobuf 构建流程、配置驱动的服务生命周期及可观测组件。运行示例会启动多个服务的生命周期任务并持续运行，按 Ctrl+C 优雅退出。游戏网络监听、session 执行器、Redis 所有权、Kubernetes 发现和 NATS 消费尚未实现。

## 开发命令

需要安装 rustup 和 just。仓库固定 Rust 1.99.0；protoc 由构建依赖提供，无需额外安装。

```text
just check
just verify
just proto
just run
just smoke
just config-check examples/demo-game/config/gateway.toml
just config-check examples/demo-game/config/game.toml
just build
```

`just fmt` 格式化代码，`just verify` 检查格式、clippy 和测试。`just proto` 在协议输入变更时触发构建期生成，生成代码位于 Cargo 的 OUT_DIR，不提交生成的 Rust 文件。

`just run` 持续运行本地服务，`just smoke` 运行一秒后通过同一退出流程结束。也可传入 `--run-for <秒数>`。Linux 额外支持 SIGTERM，适配容器停止。服务默认初始化期限为 30 秒，整体退出期限为 10 秒，游戏可通过 `Application::with_timeouts` 调整。

游戏通过 `ServiceFactory` 注册各类服务。初始化完成后才加入服务目录；服务启动失败、超时、崩溃或提前结束会触发全进程清理，撤销目录并通知已启动服务退出。服务在取消后负责清理自己的资源，超出退出期限会中止任务。示例服务目前仅演示生命周期，配置地址尚未用于网络监听。

进程配置 `node_id` 和 `endpoints`，每个端点配置 `name`、`protocol` 和 `address`；各服务通过 `endpoint` 引用端点。多个服务可共享一个内部 gRPC 端点，客户端接入使用另外的端点。协议名由传输实现解释，可扩展；传输参数与加密组件在接入实现时补齐。当前端点配置用于校验和发现描述，尚未启动实际监听。

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

服务配置还将包含业务服务及其 RPC 中间件链，选择、顺序、参数和作用范围由用户控制。框架通过 Tower 提供常用 Layer 和便捷组装能力；例如公会实例每次 RPC 按 guild id 获取分布式锁，由游戏为该服务配置，锁后端可注入和替换。目前中间件配置与 RPC 调度尚未实现，后续遵循这一组装边界，详见设计文档。

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

追踪通过 OTLP gRPC 导出到 Collector，使用 W3C TraceContext 传播器和父级采样决策，采样率作用于新建根 trace。JSON 日志包含 service/node_id，进入有效追踪 span 时同时包含 trace_id/span_id。已接入服务初始化 span；RPC metadata、NATS headers 的上下文注入提取会随对应链路实现。token 和协议正文默认不记录。

强追踪规则命中时直接采样，优先于父级决定、普通追踪过滤和 ratio。支持 span 名称、target 和字段的精确匹配；一条规则的全部条件必须满足，多条规则任意命中即可。字段值支持字符串、数字和布尔值。下面的规则强制采集 player_id 为 123 的 client_request：

```toml
[[observability.trace_force_rules]]
span = "client_request"
fields = { player_id = 123 }
```

`telemetry.traces.config()` 读取采样配置，`set_config(SamplingConfig)` 原子更新过滤、ratio 和规则；`set_force_rules(Vec<ForceTraceRule>)` 单独更新规则，空列表清除规则。无效配置保留原值，更新影响后续采样决定，已开始的 span 保持原决定。强追踪子 span 继承已采样父级，即使过滤规则为 off；规则移除也不截断已开始的链路。

`trace_filter` 使用级别和模块规则，如 `info,demo_game::player=debug`，决定普通根 span 的采样资格；span 字段条件通过强追踪规则配置。为避免字段规则被提前排除，span 进入 SDK 后统一决定采样，普通根 span 仍按过滤和 ratio 丢弃。默认比例采样只用于无有效父级且未命中强规则的 span。

匹配字段应在创建 span 时提供。采样后补录字段不会重新采样；尽量在入口请求 span 上命中规则，随后传播采样决定。下游命中规则可开启从该处开始的追踪，但无法补回此前丢弃的上游 span。强制采样需要启用 OTLP 导出，不意味着追踪系统故障时保证数据送达。
