# tok 用户指南

tok 是实验性的 Rust 游戏服务器框架。游戏通过本 crate 组装组件和服务，输出自己的二进制，并通过 TOML 选择运行的服务。

## 生成和阅读文档

在仓库根目录执行 `just docs`，或 `cargo doc --workspace --no-deps --locked`。
生成入口是 `target/doc/tok/index.html`，包含本指南和各组件的 API 文档。
需要在浏览器中打开时，可自行执行 `cargo doc -p tok --no-deps --locked --open`。
Rust 示例随 `cargo test --workspace --locked` 执行文档测试，API 链接由 rustdoc 校验。
公开 API 通过 `missing_docs = "deny"` 强制维护文档注释，缺失时编译失败。
仅 protobuf 自动生成模块豁免；手写模块、类型、字段、枚举变体及方法都需要文档。
框架自己的 API 使用中文说明；第三方重导出补充中文介绍，完整 API 说明保留上游原文。

## 配置一个服务

[`AppConfig`] 描述节点、端点、服务和可观测配置。
服务通过 `(node_id, service_name)` 定位，业务实体 ID 与服务身份分别管理。
多个服务可以引用同一个端点；端点中的协议名由对应的传输组件解释。

```rust
use tok::{AppConfig, core::ServiceDirectory, runtime::{Application, ServiceContext,
    ServiceFactory, ServiceTask, StartTask}};

struct GameService;

impl ServiceFactory for GameService {
    fn start(&self, context: ServiceContext) -> StartTask {
        Box::pin(async move {
            // 在返回前完成初始化，持有资源使用 RAII，初始化必须可取消。
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                // 收到退出通知后清理服务资源。
                Ok(())
            }) as ServiceTask)
        })
    }
}

# fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let config = AppConfig::from_toml(r#"
node_id = "local"
discovery = "memory"

[[endpoints]]
name = "internal-rpc"
protocol = "grpc"
address = "127.0.0.1:7001"

[[services]]
service_name = "guild"
endpoint = "internal-rpc"
"#)?;

let mut app = Application::new(config)?;
app.register("guild", GameService)?;
let directory = app.directory();

let runtime = tokio::runtime::Runtime::new()?;
runtime.block_on(app.run_until(async {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    Ok(())
}))?;

// 退出后服务已从目录移除。
assert!(directory.snapshot("guild")?.is_empty());
# Ok(())
# }
```

[`runtime::ServiceFactory::start`] 返回初始化 future；初始化完成后返回持续运行的服务任务。
[`runtime::Application::run_until`] 监督任务，初始化失败、超时、提前结束或崩溃会触发整个进程清理。
服务通过 [`runtime::ServiceContext::shutdown`] 接收取消通知。
默认初始化期限为 30 秒，整体退出期限为 10 秒，可通过
[`runtime::Application::with_timeouts`] 调整。
生产进程可向 `run_until` 传入 [`runtime::shutdown_signal`]，支持 Windows Ctrl+C 和 Unix SIGTERM。

## 日志 指标与追踪

在 Tokio 运行时内调用 [`observability::Observability::init`]，每个进程初始化一次。
服务结束后调用 [`observability::Observability::shutdown`]，关闭指标端口并刷新追踪。
默认启用 INFO 文本日志；Prometheus 端口和 OTLP 导出按配置开启。

`RUST_LOG` 覆盖初始日志过滤规则。运行时可通过 [`observability::LogControl`]
调整日志过滤，通过 [`observability::TraceControl`] 调整采样规则。
无效规则会保留原配置；日志过滤与追踪采样分别管理。

```toml
[observability]
log_format = "json"
log_filter = "info"
metrics_address = "127.0.0.1:9090"
otlp_endpoint = "http://127.0.0.1:4317"
trace_filter = "info"
trace_sample_ratio = 0.1
```

指标不使用 player_id、bid 或 request_id 作为标签。token 和协议正文默认不记录。
内部 RPC 已通过 metadata 传播 W3C TraceContext；NATS 传播随事件组件实现接入。

## 内部 RPC 与共享端点

先用 [`rpc::HandlerRegistry`] 注册游戏消息，再用 [`rpc::RpcService`] 显式绑定节点和业务服务名。
player 使用 `into_player`，普通业务服务使用 `into_business`，两者使用不同的 protobuf 服务。
其他业务也可以使用自己生成的 gRPC 服务，通过 [`rpc::RpcEndpoint::add_service`] 绑定。
同一端点不能重复注册同一个 protobuf 服务；多个普通业务服务共享端点时，应分别定义不同的 protobuf 服务。

下面用一个公会请求演示完整组装。游戏需要依赖 tok、prost 和 tokio。
实际运行会监听 127.0.0.1:7001，并在收到进程退出信号后清理资源。

```rust,no_run
use std::sync::Arc;
use tok::{AppConfig, rpc::{HandlerRegistry, RpcEndpoint, RpcService},
    runtime::{Application, ServiceContext, ServiceFactory, ServiceTask, StartTask,
        shutdown_signal}};

#[derive(Clone, PartialEq, prost::Message)]
struct ReadGuild {
    #[prost(uint64, tag = "1")]
    guild_id: u64,
}

struct Lifecycle;
impl ServiceFactory for Lifecycle {
    fn start(&self, context: ServiceContext) -> StartTask {
        Box::pin(async move {
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                Ok(())
            }) as ServiceTask)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = AppConfig::from_toml(r#"
node_id = "local"
discovery = "memory"
[[endpoints]]
name = "internal-rpc"
protocol = "grpc"
address = "127.0.0.1:7001"
[[services]]
service_name = "guild"
endpoint = "internal-rpc"
"#)?;

    let mut handlers = HandlerRegistry::new();
    handlers.register(1, 2, |context, request: ReadGuild| async move {
        if context.bid.as_deref() != Some(request.guild_id.to_string().as_str()) {
            return Err(tok::rpc::tonic::Status::invalid_argument("业务 ID 不匹配"));
        }
        Ok(request)
    })?;
    let service = RpcService::new("local", "guild", Arc::new(handlers))?;
    let endpoint = RpcEndpoint::new().add_service("guild", service.into_business())?;
    let mut app = Application::new(config)?;
    app.register("guild", Lifecycle)?;
    app.register_rpc_endpoint("internal-rpc", endpoint)?;
    app.run_until(shutdown_signal()).await?;
    Ok(())
}
```

应用先绑定共享监听，业务初始化完成后才开启对应服务的就绪入口。
初始化失败、监听失败和运行异常都进入同一清理流程。
工厂初始化不能等待尚未启动的其他服务；依赖调用应在必要服务就绪后发起。

调用方通过 [`runtime::Application::service_directory`] 或 [`runtime::ServiceContext::directory`]
查询服务，再用 [`infra::routing::route`] 按 bid 选点，使用 [`rpc::RpcClients`] 调用。
Channel 缓存有界，节点和地址相同的服务复用连接；节点变更时调用 `invalidate` 撤销该节点缓存。
请求携带 [`core::CallContext`]，player 调用还需匹配的 [`core::SessionIdentity`]。
上下文校验检查目标和字段一致性，不能代替内部调用身份认证或全局 session 所有权校验。

每次调用配置期限且不自动重试。超时不表示远端业务已回滚；未知结果由游戏处理。
用户可通过 [`rpc::TowerHandler`] 注入自行组装的 Tower Service，选择并发限制、过载和锁策略。
普通业务入口不会自动加锁。
[`rpc::notification`] 编码强类型通知，真正下行序号和补发由后续网关 session 分配。

运行 `just rpc-smoke` 可以验证示例 player 与 guild 的内部调用并退出。
此检查使用构造的内部会话上下文，不代表登录认证或客户端 session 已实现。

## 服务发现与内存所有权

[`core::ServiceDirectory`] 提供快照和变更订阅。先订阅再读取初始快照，每次通知后重新读取需要的服务。
通知可以合并；不能根据通知次数推断服务变更次数。
[`runtime::Application::with_service_directory`] 可注入查询后端；本进程注册和退出撤销仍由应用管理。
[`runtime::Application::directory`] 返回本进程的内存注册目录。

[`infra::ownership::MemoryOwnership`] 是有界的单进程所有权后端，实现
[`core::coordination::OwnershipStore`] 的条件获取、续租与释放。
接管必须匹配旧 epoch，旧持有者不能续租或释放新所有权。
其 epoch 仅在后端存活期间有效，不能用于跨进程或进程重启后的协调；生产 Redis 实现仍待后续实现。

## 组件入口与当前边界

| 入口 | 用途 |
| --- | --- |
| [`config`] | TOML 加载、校验与端点引用 |
| [`runtime`] | 服务工厂、启动监督与退出 |
| [`core`] | 共享类型、错误和组件契约 |
| [`infra`] | 服务目录、路由及协调后端 |
| [`rpc`] | 内部 RPC 与 Tower 组装 |
| [`gateway`] | PlayerGateway、SceneGateway 与接入组件 |
| [`events`] | 本地及跨节点事件 |
| [`observability`] | 日志、指标与追踪 |

目前示例监听共享内部 gRPC 端点，客户端网络监听、session、timer 调度、Redis 和 NATS 尚未接通。
配置端点地址本身不启动监听，需显式注册 RpcEndpoint。Kubernetes 配置可校验，运行该发现后端仍会明确报错。
游戏业务协议和 handler 由游戏定义，框架组件不反向依赖具体游戏。
