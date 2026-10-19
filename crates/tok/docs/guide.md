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
此检查使用构造的内部会话上下文；登录认证和客户端 session 由 `just client-smoke` 单独验证。

## 服务发现与内存所有权

[`core::ServiceDirectory`] 提供快照和变更订阅。先订阅再读取初始快照，每次通知后重新读取需要的服务。
通知可以合并；不能根据通知次数推断服务变更次数。
[`runtime::Application::with_service_directory`] 可注入查询后端；本进程注册和退出撤销仍由应用管理。
[`runtime::Application::directory`] 返回本进程的内存注册目录。

[`infra::ownership::MemoryOwnership`] 是有界的单进程所有权后端，实现
[`core::coordination::OwnershipStore`] 的条件获取、续租与释放。
接管必须匹配旧 epoch，旧持有者不能续租或释放新所有权。
其 epoch 仅在后端存活期间有效，不能用于跨进程或进程重启后的协调；生产 Redis 实现仍待后续实现。

## Redis 多进程开发发现

单进程示例和单元测试继续使用 Memory。多进程联调可以选择 `discovery = "redis"`，
由 [`infra::discovery::redis::RedisDirectory`] 提供注册与后台同步，由 [`core::ServiceDirectory`]
读取本地快照。注册生命周期通过 [`core::ServiceRegistry`] 与只读查询分开。
Application 在运行时连接 Redis，业务初始化成功后发布服务；构造应用与配置检查不访问 Redis。

先启动 Redis 7.4，例如：

```text
docker run --detach --name tok-dev-redis --publish 127.0.0.1:6379:6379 redis:7.4-alpine
```

然后在两个终端分别执行：

```text
just run examples/demo-game/config/redis-game.toml
just run examples/demo-game/config/redis-gateway.toml
```

若要验证业务后退出网关，可用下面命令代替第二条：

```text
cargo run -p demo-game --locked -- --config examples/demo-game/config/redis-gateway.toml --client-smoke
```

两份配置必须使用相同 Redis URL 和 namespace，node_id 各不相同。增加游戏进程时复制配置并修改 node_id 和监听端口。
跨开发机使用时，`advertise_address` 应填其他机器能够连接的 IP 与端口；`address` 保留本机监听地址。
例如监听 `0.0.0.0:7001`，发布 `192.0.2.10:7001`。
工厂通过 [`runtime::ServiceContext::listen_address`] 获取监听地址，`context.service.address` 是发布地址。
Redis 发现拒绝发布通配地址；Memory 仍允许原有单进程配置。

```toml
node_id = "dev-game-1"
discovery = "redis"

[redis_discovery]
url = "redis://127.0.0.1:6379/"
namespace = "my-dev"
lease_ttl_ms = 15000
refresh_interval_ms = 1000
operation_timeout_ms = 2000
max_nodes = 256
```

以上是配置头部，端点和服务仍按已有格式声明。完整示例见仓库的 redis-game.toml 与 redis-gateway.toml。
[`infra::discovery::redis::RedisDiscoveryConfig`] 对 namespace、容量与期限进行校验：
namespace 最长 64 字节，只允许 ASCII 字母、数字、短横线和下划线；最多 4096 节点，单节点最多 64 个服务、64 KiB 描述。
租期必须大于刷新间隔与两倍操作期限之和。连接、扫描、发布和注销都有有限期限，连接 URL 的 Debug 输出隐藏凭据。

每个节点使用带 TTL 的 Redis Hash 保存实例 token 和服务列表，key 为 `tok:discovery:{namespace}:node:<node_id 的十六进制编码>`。
相同 node_id 的活动实例不能互相覆盖，续租、更新和删除均校验实例 token。退出时条件撤销；崩溃或运行 future 被丢弃后停止续租，由 TTL 清理遗留记录。
后台使用 SCAN 周期读取目录，更新本地缓存并合并发送变更通知，初期不依赖 Pub/Sub。
变更通常在下一次刷新后可见；后端断开、租约丢失、扫描超时或目录超过配置容量时，缓存失效，监督任务失败，Application 退出。
该开发后端采用失败后退出策略，恢复 Redis 后需要重新启动应用，不后台无限重连。没有后台刷新时，缓存最多保留一个租期，过期查询返回错误。

`Application::directory()` 始终返回本进程的内存注册镜像；跨进程查询必须使用 `Application::service_directory()` 或 `context.directory`。
`with_service_directory` 只替换查询来源，注册仍使用配置选定的后端。
首次 Redis 同步完成前快照返回不可用，业务应等待就绪后发起调用；示例 smoke 会有界等待。

`just redis-test` 验证实际注册、通知、namespace 隔离、崩溃过期、租约丢失与旧实例条件删除。
`just redis-smoke` 自动启动独立游戏和网关进程，经 TCP 验证登录、状态读写及 player 调用 guild，再验证正常退出、换地址重启及崩溃摘除。
两者默认使用本机 Redis 6379，可用 `TOK_TEST_REDIS_URL` 覆盖；每次使用唯一 namespace，不清空数据库。
这些测试需要显式运行，常规 `just verify` 不依赖 Redis；CI 增加独立 Linux Redis 集成任务。

Redis 发现只解决服务定位。示例玩家状态和 session 所有权仍保存在进程内存；多网关竞争与交接需要后续 Redis 所有权实现。
生产发现继续按 Kubernetes 设计推进。

## 客户端 TCP 与玩家 session

运行 `just client-smoke` 会启动示例，完成真实 TCP 登录、状态读取、写入、再次读取及 player 调用 guild，然后退出并清理监听。
运行 `just run` 可持续监听本地客户端端口 `127.0.0.1:7000`。
示例凭据为 `demo:<正整数玩家 ID>`，如 `demo:42`；该认证器仅供本地开发，状态保存在有界内存中。

客户端使用四字节大端长度前缀与 protobuf [`gateway::protocol::Frame`]，版本为 1。
登录和业务请求的 `request_id` 必须非零，并在同一连接上严格递增。
下面示例登录后读取玩家状态；示例游戏的读取消息 ID 为 1，空正文是合法的默认 protobuf 请求。

```rust,no_run
use std::time::Duration;
use tok::gateway::{codec::{FrameCodec, VERSION},
    protocol::{Frame, LoginRequest, BusinessRequest, frame::Content}};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = TcpStream::connect("127.0.0.1:7000").await?;
    let codec = FrameCodec::new(1_048_576, Duration::from_secs(15))?;
    codec.write(&mut connection, &Frame {
        version: VERSION,
        content: Some(Content::Login(LoginRequest {
            request_id: 1,
            credential: "demo:42".into(),
        })),
    }).await?;
    let login = codec.read(&mut connection).await?.ok_or("连接已关闭")?;
    match login.content {
        Some(Content::LoggedIn(response)) if response.error_code == 0 => {},
        _ => return Err("登录失败".into()),
    }
    codec.write(&mut connection, &Frame {
        version: VERSION,
        content: Some(Content::Request(BusinessRequest {
            request_id: 2,
            message_id: 1,
            body: Vec::new(),
        })),
    }).await?;
    let reply = codec.read(&mut connection).await?.ok_or("连接已关闭")?;
    match reply.content {
        Some(Content::Response(response)) => {
            assert_eq!(response.request_id, 2);
            assert_eq!(response.error_code, 0);
            // 游戏使用自己的 protobuf 响应类型解码 response.body。
        },
        _ => return Err("响应类型错误".into()),
    }
    Ok(())
}
```

游戏通过 [`gateway::player::Authenticator`] 返回规范化的可信玩家 ID，同一玩家的凭据别名必须归一到相同 ID。
[`gateway::player::PlayerGateway::new`] 注入认证器、[`core::coordination::OwnershipStore`] 和
[`gateway::player::PlayerExecutor`]；[`gateway::player::RpcPlayerExecutor`] 可组合
[`infra::routing::RendezvousRouter`] 与 [`rpc::RpcClients`] 调用 player 服务。
网关工厂先绑定 TCP 监听，再把监听及服务退出 token 交给 `serve` 的受监督任务。
客户端正文不提供 session 身份；示例 player 从可信的调用上下文读取玩家 ID。

[`gateway::player::GatewayConfig`] 对连接数、会话数、请求队列、下行队列、包长及期限提供有界默认值。
TOML 的可选 `[player_gateway]` 节可覆盖参数，未知字段和非法容量会使配置校验失败：

```toml
[player_gateway]
max_connections = 128
max_sessions = 1024
queue_capacity = 64
outbound_capacity = 64
max_frame_size = 1048576
io_timeout_ms = 15000
auth_timeout_ms = 5000
rpc_timeout_ms = 5000
ownership_ttl_ms = 30000
renew_interval_ms = 10000
```

同一玩家的业务请求顺序执行，不同玩家可以并发。队列满立即返回错误，RPC 超时不重试。
通过 `gateway.sessions()` 获取 [`gateway::player::SessionManager`]，可查询当前
[`gateway::player::SessionHandle`]；`notify` 与响应共用有界下行队列，不调用 player。
下行队列满会关闭连接，避免慢客户端无限积压。框架错误见 [`gateway::ErrorCode`]，业务响应的非零错误码保留原值，
游戏可通过 [`gateway::player::RpcErrorMapper`] 自定义 tonic 状态映射。

重复登录使用新连接：先关闭旧入口，等待当前执行结束或达到执行期限，再条件释放所有权并建立新的 session_id 和 epoch。
旧队列不再执行，外部网关持有的所有权直接拒绝。连接断开即关闭 session；退出时等待在途执行并条件释放所有权。
当前没有心跳，完整帧读取期限包含等待首字节的空闲时间，默认空闲 15 秒会断开。
本批尚不调度 RPC 返回的 timer 信息，也不提供断线宽限期、ack、补发或恢复，这些按下一批实现。

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

目前示例监听共享内部 gRPC 和客户端 TCP 端点，已接通注入认证、有界串行 session、状态读写及服务组合。
Redis 开发发现已接通；timer 调度、恢复、Redis 所有权和 NATS 尚未接通。
配置端点地址本身不启动监听，内部端点需显式注册 RpcEndpoint，TCP 由网关工厂绑定。
Kubernetes 配置可校验，运行该发现后端仍会明确报错。
游戏业务协议和 handler 由游戏定义，框架组件不反向依赖具体游戏。
