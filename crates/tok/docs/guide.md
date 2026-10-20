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
游戏通过 `Application::register_factory` 注册初始化工厂。服务默认按 `service_name` 查找工厂，需要另选实现时填写可选的 `factory`。
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
app.register_factory("guild", GameService)?;
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
    app.register_factory("guild", Lifecycle)?;
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
其 epoch 仅在后端存活期间有效，不能用于跨进程或进程重启后的协调；跨网关共享所有权使用下面的 Redis 后端。

## Redis session 所有权与超时边界

[`infra::ownership::redis::RedisOwnership`] 实现 [`core::coordination::OwnershipStore`]，
提供原子获取、显式 expected_epoch 接管、条件续租和释放。`ownership` 与 `discovery` 独立选择，默认均可使用 Memory：

```toml
node_id = "dev-gateway-1"
discovery = "redis"
ownership = "redis"

[redis_discovery]
url = "redis://127.0.0.1:6379/"
namespace = "demo-dev"

[redis_ownership]
url = "redis://127.0.0.1:6379/"
namespace = "demo-dev"
operation_timeout_ms = 2000
```

以上是配置头部，继续追加端点和服务声明。示例 redis-gateway.toml 已显式启用共享所有权，redis-game.toml 无需持有玩家租约。
增加网关进程时复制网关配置，修改 node_id、监听及发布端口；所有网关必须共享同一所有权 URL、数据库和 namespace。
[`infra::ownership::redis::RedisOwnershipConfig`] 验证 namespace 和操作期限，Debug 隐藏 URL。
单次操作期限为正且不超过 60 秒，也不能超过网关 rpc_timeout_ms；租期与续租间隔仍由 player_gateway 配置。
同一配置的持有者必须使用相同身份规范化规则，Redis 后端不自行转换玩家 ID。

每个玩家有两个键：`tok:ownership:{namespace:<玩家 ID 十六进制>}:lease` 和 `:epoch`。
租约 Hash 保存 session_id、node_id、service_name、epoch 并设置 TTL，续租和释放比较全部字段。
epoch 计数器不自动过期，释放、过期或进程重启后继续递增；按账号规模规划计数器容量，不单独淘汰仍会使用的计数器。
Lua 使用 INCR 后 GET 读取十进制字符串，比较和返回均不经过 Lua number，避免超过 2^53 失真。
计数器使用 Redis 有符号 64 位整数，耗尽时操作失败，不回绕。
两个键使用同一 hash tag，为 Redis Cluster 共槽预留；当前客户端连接单 Redis 主端点，Cluster 拓扑和 MOVED/ASK 适配尚未实现。
Redis 持久化或故障切换导致状态回退仍属于系统异常边界。

当前网关对其他网关仍持有的租约返回 Unavailable，不自动抢占。只有正常释放或租约过期后，另一个网关才能建立新 session。
后端的 expected_epoch 接管是交接协议的基础能力，调用方必须先收束旧入口，不能把直接接管当成已完成交接。
续租失败、后端不可达或调用期限耗尽时停止当前 session 的新业务；旧入口的条件释放不会删除新所有权。
连接断开与应用正常退出会条件释放，进程崩溃后由 TTL 过期。超时的 Redis 操作可能已提交，结果未知，不自动重试，遗留租约等待过期。

系统采用有限的时间保护：Redis 获取和续租从操作开始计时，明显超过租期的回应不视为有效；
session 从续租开始时刻计算本地保守下界，剩余租期不足一次 RPC 预算时不发起新调用。进程内使用单调时钟，不比较不同机器的墙上时间。
RPC 超时不表示远端 handler 已停止或写入已回滚。时间检查通过后仍可能暂停、延迟或过期，系统明确接受检查与实际副作用之间的竞态窗口。
不默认增加逐次远程校验或强制每次存储写入携带 fencing token。
存储层尚未实现；后续存储封装在写入前检查调用期限和本地已知租约期限，已超时或有效性不确定时拒绝写入，作为尽力保护。
资产扣减等业务按实际需要选择幂等键、条件更新或事务，不由框架强制统一增加策略。

`just ownership-test` 显式验证真实 Redis 竞争、完整身份校验、过期、重启代次、2^53 相邻整数和计数器耗尽。
`just ownership-smoke` 启动独立游戏及两个网关进程，验证竞争拒绝、释放后登录、租约丢失关闭、崩溃后过期和旧代次失效。
测试使用 TOK_TEST_REDIS_URL，默认本机 6379；常规测试不依赖 Redis。
timer 与同网关恢复已接通；跨网关交接和请求级 LockBackend 实现仍按后续阶段推进。

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

Redis 发现只解决服务定位。玩家状态仍在进程内存，session 所有权默认 Memory，可独立选择上述 Redis 实现；跨网关交接尚未实现。
生产发现继续按 Kubernetes 设计推进。

## 客户端 TCP 与玩家 session

示例游戏的消息 ID 由 `demo-game/proto/demo/v1/message_id.proto` 的 `MessageId` 枚举统一维护。
例如 `MESSAGE_ID_GET_PLAYER_REQUEST = 1` 按命名规则对应 `GetPlayerRequest`，
构建脚本校验 `demo.v1` 的顶层消息并生成 `GameMessage::MESSAGE_ID` 关联。
枚举项遵循 `MESSAGE_ID_<消息类型的 UPPER_SNAKE_CASE>`，Rust 类型使用 prost 的命名规则。
未匹配的消息或枚举项、重复名称或数字、非正数 ID 会使构建失败；零仅用于 `MESSAGE_ID_UNSPECIFIED`。
在 demo 中导入 `protocol::GameMessage` 后使用 `GetPlayerRequest::MESSAGE_ID`，原来的 `player::GET_PLAYER` 等手写常量已移除。

请求与回应的配对仍通过 HandlerRegistry 显式注册；客户端调用辅助函数从请求、回应类型读取 ID。
名称关联只在构建期生成静态映射，运行时通过数字分发，不按字符串猜测类型或回应。
数值一经使用保持稳定，不因重排枚举或 Rust 模块而改变；删除消息时将其编号与枚举名称标记为 `reserved`，不能复用。
这套生成约定属于示例游戏，框架信封和 HandlerRegistry 仍使用 `u32`，不依赖游戏枚举或命名约定。

运行 `just client-smoke` 会启动示例，完成真实 TCP 登录、状态读取、写入、再次读取及 player 调用 guild，然后退出并清理监听。
运行 `just run` 可持续监听本地客户端端口 `127.0.0.1:7000`。
示例凭据为 `demo:<正整数玩家 ID>`，如 `demo:42`；该认证器仅供本地开发，状态保存在有界内存中。

客户端使用四字节大端长度前缀与 protobuf [`gateway::protocol::Frame`]，版本为 3。
protobuf 命名空间仍为 `tok.client.v1`，帧版本由 `VERSION` 决定；旧版本 1、2 客户端须同步升级，服务端拒绝旧帧版本。
登录和业务请求的 `request_id` 必须非零，并在同一连接上严格递增。
下面示例登录后读取玩家状态；示例游戏的读取消息 ID 为 1，空正文是合法的默认 protobuf 请求。

```rust,no_run
use std::time::Duration;
use tok::gateway::{codec::{FrameCodec, VERSION},
    protocol::{Frame, LoginRequest, BusinessRequest, Ack, frame::Content}};
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
            resume_token: String::new(),
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
            assert_eq!(response.server_seq, 1);
            // 游戏在这里解码 response.body 并应用业务状态；完成后才累计确认。
            codec.write(&mut connection, &Frame {
                version: VERSION,
                content: Some(Content::Ack(Ack { ack_seq: response.server_seq })),
            }).await?;
        },
        _ => return Err("响应类型错误".into()),
    }
    Ok(())
}
```

游戏通过 [`gateway::player::Authenticator`] 返回规范化的可信玩家 ID，同一玩家的凭据别名必须归一到相同 ID。
`gateway::Gateway::player` 注入认证器、[`core::coordination::OwnershipStore`] 和
[`gateway::player::PlayerExecutor`]；[`gateway::player::RpcPlayerExecutor`] 可组合
[`infra::routing::RendezvousRouter`] 与 [`rpc::RpcClients`] 调用 player 服务。
网关工厂先绑定 TCP 监听，再把监听及服务退出 token 交给 `serve` 的受监督任务。
客户端正文不提供 session 身份；示例 player 从可信的调用上下文读取玩家 ID。

player 内部按职责组织会话通信与执行：

| 名称 | 职责 |
| --- | --- |
| `SessionHandle` | 向逻辑会话提交业务、发布通知及请求关闭 |
| `BoundSession` | 携带连接代次的操作入口，校验连接是否仍然有效 |
| `SessionChannel` | 管理连接绑定、请求准入、下行发布、确认与断线恢复 |
| `ReplayCache` | 保存已编码下行消息，查询可连续补发的范围 |
| `ReplayPlan` | 由连接写循环执行恢复响应、补发及恢复完成标记 |
| `ResumeTokens` | 签发并验证恢复凭证，关联可信玩家和逻辑会话 |
| `TimerScheduler` | 调度 timer 并将到期任务送入会话队列 |

通信状态位于 `player::channel`，恢复凭证位于 `player::resume_token`。
原模块路径 `player::outbound`、`player::resume` 已相应调整。

[`gateway::player::PlayerConfig`] 对连接数、会话数、请求队列、下行队列、包长及期限提供有界默认值。
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
max_timers = 128
max_timer_bytes = 1048576
replay_ttl_ms = 30000
replay_capacity = 256
replay_max_bytes = 4194304
disconnect_grace_ms = 30000
resume_token_ttl_ms = 3600000
max_resume_tokens = 16384
```

同一玩家的业务请求顺序执行，不同玩家可以并发。队列满立即返回错误，RPC 超时不重试。
通过 `gateway.ingress().sessions()` 获取 [`gateway::player::SessionManager`]，可查询当前
[`gateway::player::SessionHandle`]；`notify` 与响应共用有界下行队列，不调用 player。
下行队列满会断开慢连接并进入宽限期，后续消息继续进入有界缓存。框架错误见 [`gateway::ErrorCode`]，业务响应的非零错误码保留原值，
游戏可通过 [`gateway::player::RpcErrorMapper`] 自定义 tonic 状态映射。

重复登录使用新连接：先关闭旧入口，等待当前执行结束或达到执行期限，再条件释放所有权并建立新的 session_id 和 epoch。
旧队列不再执行，外部网关持有的所有权直接拒绝。连接断开后默认保留 session 30 秒，期间继续 timer 与续租；到期收束在途执行并条件释放所有权。显式关闭、非法协议、所有权失效和网关退出仍终止 session。
当前没有心跳，完整帧读取期限包含等待首字节的空闲时间，默认空闲 15 秒会断开。
已支持 timer 更新、下行序号、ACK、有界缓存与同网关恢复；跨网关交接仍待后续实施。

### 下行序号、确认与缓存

已绑定 session 的 `BusinessResponse`（含业务错误、队列拒绝和超时）与 `Notify` 共用连续 `server_seq`，
从 1 开始，按发布顺序编号。`request_id` 关联请求和回应，`message_id` 标识游戏消息类型；
较晚请求被队列拒绝时，其回应可能先于较早请求的在途 RPC 回应发布。
登录回应和控制错误不占业务序号；未认证业务请求改为返回 `ControlError`，不再返回无会话序号的业务回应。

客户端按序应用回应和通知后，使用独立 `Ack { ack_seq }` 确认连续处理完成的位置。
缺少 3 而已收到 4 时不能确认 4；重复下行不重复应用业务状态。
ACK 不使用 `request_id`，不进入 player 队列，不触发 RPC。零值、重复或倒退的 ACK 忽略；
超过最大发布序号的 ACK 返回 `InvalidRequest` 控制错误并关闭连接，未认证 ACK 返回 `Unauthenticated` 并关闭连接。
确认上界使用缓存已提交的序号，本地写入完成通知不等于客户端确认。

下行先编码并缓存再提交发送队列，缓存和 writer 共享最终正文，包含版本和序号，不含 TCP 长度前缀或加密封装。
超大回应在缓存前转换为空正文的 `FrameTooLarge` 回应，使用原请求消息 ID；
超大通知直接返回 `FrameTooLarge`，不消耗序号，也不因此关闭 session。
发布成功表示缓存接收；连接正常时同时提交发送队列，断线或补发期间只进入缓存，不表示客户端已收到或处理。

缓存按单会话的时间、条数和编码字节上限淘汰最旧记录，允许淘汰未确认消息；时间使用单调时钟。
`replay_capacity` 上限 65536，`replay_ttl_ms` 上限 86400000，`replay_max_bytes` 上限 64 MiB 且不得小于 `max_frame_size`。
这些值均须为正；默认 30 秒、256 条、4 MiB 是开发参数，单会话上限并非整个网关内存预算。
合法 ACK 释放已确认记录；过期记录在发布、ACK 或缓存查询时清理，重读快照不延长原始保留时间。

[`gateway::player::SessionHandle::replay_after`] 查询给定确认位置之后的完整缓存，返回
[`gateway::player::replay::ReplayBatch`]，其 `through_seq` 是查询时的最大序号，`frames` 是按序编码正文。
范围有缺口或位置越界返回 `None`；已确认释放的字节不能再从更早位置查询，关闭后返回 `SessionClosed`。
查询不发送、不确认消息；调用方持有快照会延长对应字节的生命期，应及时释放。
`try_request` 的独立回应只交给调用方，仍无下行序号，也不自动写入客户端缓存。

### 同网关断线恢复

成功登录的 `LoginResponse` 包含 `resume_token` 和 `resume_token_ttl_ms`。
当前 token 是两个随机 UUID v4 组成的 64 字符不透明凭证，仅在签发网关的有界内存中验证，关联可信玩家及原 session；
不写 Redis，不记录日志，不支持跨网关验证。网关重启后凭证失效，客户端重新鉴权。
凭证默认有效 1 小时，宽限期默认 30 秒，两者独立；恢复不延长凭证期限。
有效凭证表满时新玩家登录返回 `CapacityExceeded`，不淘汰其他玩家的有效凭证；过期记录在签发及验证时回收。
同一玩家新登录轮换凭证并撤销旧凭证；仍在线的 session 不因凭证到期主动退出。

EOF、帧 I/O 超时、传输失败或下行队列满时，session 在服务器观察到断线后进入宽限期。
已排队和在途调用继续收束，timer 与租约续期继续；回应、通知进入原下行缓存。
`disconnect_grace_ms=0` 保留立即关闭行为，最大 86400000；
`resume_token_ttl_ms` 为正且不超过 86400000，`max_resume_tokens` 为正且不超过 65536。
宽限期由 session 的受监督 future 计时，不为每次断线创建任务；到期停止新业务、收束调用并条件释放所有权。

客户端暂停新业务，在新连接发送 `ResumeRequest { request_id, resume_token, ack_seq }`。
原会话、租约及缓存范围仍有效时，先收到成功 `ResumeResponse`，随后按原字节和序号补发，最后收到
`RecoveryComplete { through_seq }`。保持原 `session_id`、epoch 和序列，不执行普通登录或重试 player RPC。
新绑定失效旧连接；请求、ACK、读侧回应及晚到清理都比较连接代次，旧连接退出不能断开新连接。

补发由连接写循环执行，读循环仍处理 ACK；新下行先缓存再按序追上，不越过恢复完成标记。
客户端收到标记后恢复业务，提前提交得到 `InvalidState`。
整个补发受 `io_timeout_ms` 限制；中途缓存出现缺口时返回 `ControlError(ResumeUnavailable)` 并断开新连接，不能宣告成功。
`ResumeResponse.last_request_id` 是网关已见过的最大业务请求 ID（包含队列拒绝）。
客户端保留自己的计数，新请求必须大于本地已发送最大值和服务端值，不重发未知结果请求。
在途调用可能在恢复完成标记之后才产生回应，仍通过原 `request_id` 与下行序列接续。

token 有效而会话过期或缓存不完整时，`ResumeResponse.error_code` 返回 `ResumeUnavailable`。
客户端可在同一未绑定连接发送 `LoginRequest { resume_token, credential: 空 }` 快速重登，绕过游戏认证器；
两种凭证必须二选一。快速重登建立新 session、epoch 和 token，下行从 1 开始，并重新请求完整业务状态。
新登录后旧 token 撤销；伪造、过期或其他网关签发的凭证返回 `AuthenticationFailed`，须普通鉴权。

框架不固定游戏初始化消息。快速重登后的状态请求应返回完整状态与 `TimerUpdate::ReplaceAll`，重建 timer 集合；
demo 的 `GetPlayer` 从服务端保存的生产截止时间重建 timer，已完成任务不会重复结算。
同网关恢复复用原 timer 集合，不重新安装。`just client-smoke` 包含断线期间生产、恢复原会话及状态校验。


## 玩家 timer

timerSpec 由服务端业务逻辑生成，不作为客户端调度接口。客户端只发起“开始生产”等业务动作，
耗时、执行消息、参数与产出均由服务端规则决定。

业务请求通过 [`rpc::HandlerRegistry::register_with_timers`] 返回 [`rpc::HandlerReply`]，
其 `timers` 使用 [`core::TimerUpdate`]。`None` 保持原集合，`ReplaceAll` 全量替换，空集合清空；
`Patch` 可增改或取消指定 ID。timer ID 必须非空且不超过 256 字节，消息 ID 非零，周期必须大于零；
同一次更新不能重复操作同一 ID。数量和 ID、正文总字节数受 `max_timers`、`max_timer_bytes` 限制，
默认分别为 128 和 1048576。更新先完整校验再替换；非法或超容量的更新会终止 session，避免继续驱动旧集合。

```rust
use prost::Message;
use tok::{core::{TimerSpec, TimerUpdate}, rpc::{HandlerRegistry, HandlerReply}};

#[derive(Clone, PartialEq, Message)]
struct StartProduction {}
#[derive(Clone, PartialEq, Message)]
struct ProductionFinished {}

# fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut handlers = HandlerRegistry::new();
handlers.register_with_timers(1, 2, |_context, _request: StartProduction| async move {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| tok::rpc::tonic::Status::internal("系统时钟无效"))?.as_millis() as i64;
    Ok(HandlerReply {
        message: StartProduction {},
        timers: Some(TimerUpdate::ReplaceAll(vec![TimerSpec {
            timer_id: "production".into(), message_id: 3, body: ProductionFinished {}.encode_to_vec(),
            due_at_ms: now + 1000, interval_ms: None,
        }])),
    })
})?;
handlers.register_timer(3, |context, _event: ProductionFinished| async move {
    let trigger = context.timer.expect("分发表已确认内部计时来源");
    // 在这里根据可信 session 和服务端生产规则结算产出。
    let _ = (context.session, trigger.scheduled_at_ms, trigger.fired_at_ms);
    Ok(None) // 不改变当前 timer 集合。
})?;
# Ok(())
# }
```

[`rpc::HandlerRegistry::register_timer`] 独立于客户端请求分发表；`CallContext.timer` 由内部 RPC 信封传递，
客户端不能通过填写 timer 的消息 ID 进入该处理函数。它包含 ID、预定与实际触发时刻，仍需遵循部署侧的内部调用身份约定。
业务正文只包含游戏参数，timer 更新和触发来源不直接发送客户端。
直接构造 [`core::CallContext`] 的普通调用需填写 `timer: None`；自定义 CallHandler 和 Tower 中间件可以读取该字段识别内部计时来源。

session 建立时对齐 UTC 与单调时钟，此后按同一基准等待，墙钟调整不会重放已调度的周期。
每个 session 使用一个调度 future，与执行循环共同受网关监督，不为每个 timer 创建任务。
到期只进入已有的有界执行队列，排队与执行期间同一 timer 合并为一个触发，满队列等待容量；客户端可能收到 QueueFull。
执行前检查调度版本、取消状态和所有权预算；旧更新对应的排队任务失效。
周期错过的时刻不逐次追赶，完成后按原基准计算下一个未来时刻；一次性 timer 触发后移除。
timer 失败或超时只记录本次失败，不重试；周期的后续触发独立进行。

自定义 [`gateway::player::PlayerExecutor`] 的 `execute_request` 返回 [`gateway::player::PlayerReply`]，
普通 `LogicalMessage` 可通过 `.into()` 转换；需要计时执行时实现 `execute_timer` 方法。
原 `execute`、`timer` 方法已分别改为上述名称，自定义实现需同步调整。
`execute_timer` 的关联序号由 session 独立生成，不消耗客户端 request_id；计时结果可继续返回更新，不产生客户端回应。
示例 `START_PRODUCTION` 发起生产，由服务端安装一次性 timer；内部 `FINISH_PRODUCTION` 按固定规则更新玩家状态。
请求中没有 timer 配置或产出参数，`just client-smoke` 验证完整 TCP 与 RPC 链路。
当前由业务响应安装 spec，未增加隐式的 player 登录调用。断线宽限期内 timer 继续运行；到期、显式关闭或租约失效时清理。快速重登通过游戏初始化请求重新安装完整集合。

## 统一 Gateway 与接入组件

[`gateway::Gateway`] 共用连接接收、容量控制、后台任务监督与退出流程。
[`gateway::GatewayIngress`] 定义连接处理和清理钩子，当前
[`gateway::player::PlayerIngress`] 实现玩家认证、所有权和串行 session。
运行框架通过 [`gateway::transport::GatewayTransport`] 接收连接，当前具体适配为 TCP；
其他传输和 Scene 组件仍待实现，不需要复制网关运行循环。

服务可通过 `factory` 显式选择初始化工厂，省略时按服务名查找。例如自定义入口名称：

```toml
[[endpoints]]
name = "client"
protocol = "tcp"
address = "127.0.0.1:7000"

[[services]]
service_name = "my-entry"
endpoint = "client"
factory = "player-ingress"
```

[`runtime::Application::register_factory`] 按名称注册工厂；运行前检查配置实际使用的工厂是否已注册，
每个服务调用一次工厂并传入自身的服务身份、端点和退出 token。
[`config::ServiceConfig::factory_name`] 优先返回显式的 `factory`，省略时返回 `service_name`；端点协议不参与工厂选择。
demo-game 提供 `player-ingress`、`player` 与 `guild`；选择 `scene-ingress` 明确返回尚未实现。
Player 接入组件要求独占 TCP 端点，业务组件要求 gRPC 端点；具体约束由游戏组装层检查。

普通业务配置只需 `service_name` 和 `endpoint`，例如 player、guild 默认找到同名工厂；示例网关显式使用 `factory = "player-ingress"`。
直接构造 `ServiceConfig` 时填写 `factory: None` 或 `Some(名称)`；原 `gateway` 和 `component` 配置字段已移除。
初始化工厂使用 `register_factory` 按名称注册，同一工厂注册一次即可供多个服务使用。
RPC 端点仍显式绑定服务名；业务调用方、路由与发现查询使用实际的 `service_name`。

自定义组件通过 `Gateway::new` 获取 [`gateway::GatewayContext`]，调用 `launch` 提交受监督的后台任务。
[`gateway::GatewayConfig`] 单独限制连接数和后台任务总数，任务总数包含等待启动的任务。
Player 保留 `[player_gateway]` 配置布局，`PlayerConfig::runtime_limits` 从连接数和最大 session 数
生成运行时容量；会话、队列、所有权与协议参数仍由 Player 组件使用。

普通连接错误只关闭该连接；连接 panic、后台任务错误或 panic 会使网关退出。
退出时运行框架取消任务、调用组件 `stop`、收束后台任务，再调用异步 `finish`。
`stop` 必须可重复调用；组件收到取消后应停止新业务并完成清理，整体退出期限由外层 Application 或调用方控制。
运行 future 被强制丢弃时会取消并中止受监督任务、调用 `stop`，无法等待异步 `finish`。

已有 [`gateway::player::PlayerGateway`] 保留原构造、`sessions()` 和 TCP `serve` 接口，内部委托统一运行框架；
玩家接入参数统一使用 `PlayerConfig`，通用运行时容量使用 `gateway::GatewayConfig`。新代码使用 `Gateway::player` 与 `PlayerIngress`。
Scene 目前仅预留模块，不套用 Player 的串行与所有权语义。

## 组件入口与当前边界

| 入口 | 用途 |
| --- | --- |
| [`config`] | TOML 加载、校验与端点引用 |
| [`runtime`] | 服务工厂、启动监督与退出 |
| [`core`] | 共享类型、错误和组件契约 |
| [`infra`] | 服务目录、路由及协调后端 |
| [`rpc`] | 内部 RPC 与 Tower 组装 |
| [`gateway`] | 统一 Gateway 运行框架与 Player、Scene 接入组件 |
| [`events`] | 本地及跨节点事件 |
| [`observability`] | 日志、指标与追踪 |

目前示例监听共享内部 gRPC 和客户端 TCP 端点，已接通注入认证、有界串行 session、状态读写及服务组合。
Redis 开发发现、session 所有权、timer 调度、下行序号、ACK 缓存与同网关恢复已接通；跨网关交接和 NATS 尚未接通。
配置端点地址本身不启动监听，内部端点需显式注册 RpcEndpoint，TCP 由网关工厂绑定。
Kubernetes 配置可校验，运行该发现后端仍会明确报错。
游戏业务协议和 handler 由游戏定义，框架组件不反向依赖具体游戏。
