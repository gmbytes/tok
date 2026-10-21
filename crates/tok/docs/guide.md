# tok 用户指南

tok 是实验性的 Rust 游戏服务器框架。游戏通过本 crate 组装组件和服务，输出自己的二进制，并通过 TOML 选择运行的服务。

本指南只说明当前已实现 API。架构与校正目标分别维护在仓库的 `docs/design.md`、`docs/service-runtime.md`，当前缺口见 `docs/status.md`，方向校正的实施与验收记录见 `docs/implementation-plan.md`。旧批次和协议版本已移入历史文档，不作为使用说明。

## 当前运行模型

| 范围 | 接口与边界 |
| --- | --- |
| 服务构建与运行 | Factory / Init / Builder / Running；Application 按配置组装，Gateway 作为扩展 |
| 统一处理协议 | Request / execution::Context / Tower Layer；保留各入口的输入、结果及调度约束 |
| RPC 与事件 | 配置驱动的 RPC 工厂、统一事件 Registry，本地借用与远程解码共用处理链 |
| 主题兴趣 | Event::topic 决定路由，Pool 聚合固定与会话兴趣，group 属于订阅组件 |
| 生命周期扩展 | 网关、管理器、会话快照与结束等待，会话状态工厂及显式交接适配 |

## 生成和阅读文档

在仓库根目录执行 `just docs`，或 `cargo doc --workspace --no-deps --locked`。
生成入口是 `target/doc/tok/index.html`，包含本指南和各组件的 API 文档。
需要在浏览器中打开时，可自行执行 `cargo doc -p tok --no-deps --locked --open`。
Rust 示例随 `cargo test --workspace --locked` 执行文档测试，API 链接由 rustdoc 校验。
公开 API 通过 `missing_docs = "deny"` 强制维护文档注释，缺失时编译失败。

仅 protobuf 自动生成模块豁免；手写模块、类型、字段、枚举变体及方法都需要文档。
框架自己的 API 使用中文说明；第三方重导出补充中文介绍，完整 API 说明保留上游原文。

workspace 同时禁止持有同步锁或 RefCell 借用跨越 await、丢弃未处理的 Future、过大 Future、`unimplemented!`、重复模块名、引用计数指针的隐式克隆、无等待的 async 函数，以及匿名忽略 must_use 返回值。对应检查为 `await_holding_lock`、`await_holding_refcell_ref`、`let_underscore_future`、`large_futures`、`unimplemented`、`module_name_repetitions`、`clone_on_ref_ptr`、`unused_async`、`let_underscore_must_use`。引用计数克隆使用 `Arc::clone` 等显式写法；取消和退出中的尽力操作按具体语句使用带原因的 `#[expect]`，手写模块不得整体关闭这些检查。公共角色名称确需保留时也逐项注明原因；tonic 生成模块仅豁免 `clone_on_ref_ptr`。

## 服务模型与框架分层

已确认的目标是统一服务构建与运行：服务由状态、入口路由和处理链组成，处理链由中间件和业务 handler 组成，运行设施负责生命周期与任务监督。RPC、事件和其他业务入口接到同一执行模型；普通服务不要求具备 session。

tok-gateway 是这套基础设施的内置扩展，增加连接、认证、session、串行调度、补发与交接能力。游戏自定义服务和 gateway 应使用相同的构建与扩展机制。

本 crate 是统一导出与应用组装门面。共同契约和运行能力应由下层提供，gateway 不反向依赖本 crate；使用端可以继续从 tok 取得便捷接口。Application 与 Gateway 复用公共工厂和任务生命周期；RPC、事件、timer 及网关消息链使用共同执行协议。

## crate 职责与导入路径

| 使用入口 | 职责 |
| --- | --- |
| [`core`] / tok-core | 服务身份、Extensions、发现、路由和通用请求锁契约 |
| [`execution`] / tok-execution | Context / StopReason、Request、Tower 调用执行及按自定义键分发处理链 |
| [`lifecycle`] / tok-lifecycle | 服务工厂、生命周期编排和有界任务监督 |
| [`player`] / tok-player | SessionIdentity、LogicalMessage、TimerSpec / TimerTrigger / TimerUpdate、所有权与恢复凭证契约 |
| [`grpc`] / tok-grpc | 通用 Channel 缓存、共享端点、追踪、期限及类型化执行，不理解游戏正文 |
| [`protobuf`] / tok-protobuf | 泛型键 protobuf 正文编解码，不依赖生成协议、玩家或 Tonic |
| [`message`] / tok-message | 非零编号消息注册与本地处理；message::rpc 提供远程协议适配 |
| [`gateway`] / tok-gateway | 连接、session FIFO、计时调度、下行和恢复的运行实现 |

调用执行层和生命周期层均不依赖 tok-player，彼此也不依赖。tok-player 只依赖 tok-core，供 RPC、网关和基础设施后端共同使用，不能反向依赖 gateway 或门面。[`player::TimerTrigger`] 表达玩家会话的实际计时触发，与同库 TimerSpec / TimerUpdate 组成计时契约；通用执行设施不依赖这些类型。

所有权使用 [`player::ownership::Store`]、[`player::ownership::Record`]，持有者复用 [`core::Identity`]；恢复凭证使用 [`player::credentials::ResumeTokenCodec`]。两者分别返回 [`player::ownership::Result`] 和 [`player::credentials::Result`]，不提供玩家根 Error / Result。所有权错误区分 Conflict、CapacityExceeded、Unavailable、参数、配置及可选操作；凭证验证的非法或过期结果为 Ok(None)，本地组件故障才返回 Err。请求锁使用 core::lock，不要求玩家会话。
锁、所有权与发现接口统一使用 [`core::BoxFuture`]，其中输出参数是完整类型，不隐含 Result。锁返回 `BoxFuture<'a, core::lock::Result<T>>`，发现返回 `BoxFuture<'a, core::discovery::Result<T>>`，所有权返回 `BoxFuture<'a, player::ownership::Result<T>>`。别名不会自行排队、启动或监督任务；lifecycle::task::Task 继续表达交给监督范围的工作。原 Operation 与 RegistrationTask 别名已移除。

迁移时将旧 core 下的玩家类型改从 player 导入，将 CallContext 改从 message::rpc 导入；Context / StopReason 改从 execution 导入，Extensions 改从 core 根导入；不保留旧路径兼容别名。message::rpc::player 是 PlayerRpc 客户端适配，gateway::player 是会话运行实现，顶层 player 是共享契约，职责不同。

### 玩家凭证与所有权

凭证只证明本地认证和有效期校验通过，不能代替获取所有权。普通登录的 acquire 必须原子更新最新登录标记；快速重登比较该标记与原 session_id，成功后更新为新会话。转移、续租和释放保留标记；后端回收标记后拒绝相应快速重登，客户端需要普通鉴权。顶号不会改变旧凭证的本地验签结果，但会阻止它再次建立会话。

```rust
use std::time::Duration;
use tok::{
    core::Identity,
    infra::ownership::MemoryOwnership,
    gateway::player::resume_token::SignedResumeTokens,
    player::{credentials::ResumeTokenCodec, ownership::Store},
};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let store = MemoryOwnership::new(16)?;
let record = store.acquire(
    "42".into(), "login-1".into(),
    Identity { node_id: "local".into(), service_name: "gateway".into() },
    None, Duration::from_secs(30),
).await?;
// 临时密钥仅用于单进程开发；多网关应配置相同的固定共享密钥。
let tokens = SignedResumeTokens::ephemeral()?;
let token = tokens.issue(&record.identity, Duration::from_secs(60))?;
assert_eq!(tokens.verify(&token)?.map(|claims| claims.session_id), Some("login-1".into()));
assert!(store.release(record).await?);
# Ok(())
# }
```

LogicalMessage 的 error_code 同时可承载框架与游戏错误，编号分配遵循接入协议；server_seq 由会话发布时分配，未发布的回应和通知也为 None。TimerSpec 由可信玩家业务处理者声明，客户端不能直接提交计时需求。

## 公开路径与内部模块

公开 API 按使用者的功能概念组织，内部文件组织通过 mod 隐藏，主要入口可以由 pub use 重导出。此规则来自 Rust 官方对公开 API 与内部结构解耦的指导，不是固定目录模板。公开模块路径与根路径可以同时存在，指向同一定义，不要求只保留一个路径。

core::discovery / lock / routing / service 是功能命名空间；常用的 Identity、ServiceDescriptor、ServiceDirectory、ServiceRegistry、DirectoryWatch 和 ServiceRouter 同时可从 core 根导入。Extensions 和 BoxFuture 直接从根访问；各领域 Error / Result 位于其功能模块，core 不提供根错误。通常可导入主要类型；使用锁接口等一组相关项时保留模块限定名，避免 Key / Lease 等通用名字失去上下文。

例如服务身份既可使用 `use tok::core::Identity`，也可使用 `use tok::core::service::Identity`；错误分类说明位于公开的 Error 类型文档，不需要访问内部 error 模块。根导出的角色名前缀按辨识价值保留，不因路径形式机械删改。

## 错误边界

tok-core 不提供根 Error / Result；各能力只定义和返回自己能够解释的错误，组装层显式聚合。

| 接口范围 | 错误类型与主要类别 |
| --- | --- |
| 服务发现和目录 | [`core::discovery::Error`]：InvalidInput、InvalidConfig、RegistrationConflict、CapacityExceeded、StateUnavailable、Unavailable、Unsupported |
| 请求锁 | [`core::lock::Error`]：InvalidInput、InvalidConfig、WaitTimeout、CapacityExceeded、Unavailable |
| 业务选点 | [`core::routing::Error`]：InvalidInput、NoAvailableService、Discovery；Discovery 保留发现错误及 source |
| 会话所有权 | [`player::ownership::Error`]：InvalidInput、InvalidConfig、Unsupported、Conflict、CapacityExceeded、Unavailable |
| 恢复凭证 | [`player::credentials::Error`]：InvalidInput、InvalidConfig、Unavailable；非法或过期凭证为 Ok(None) |
| 执行上下文创建 | [`execution::ContextError`]：InvalidService、InvalidOperation、DeadlineOverflow |
| 任务范围创建 | [`lifecycle::task::CreateError`]：InvalidCapacity、ParentStopping |
| 服务构建器创建 | [`lifecycle::CreateError`]：InvalidIdentity、Tasks；Tasks 保留任务范围构造错误 |
| 应用配置 | [`config::Error`]：Invalid；配置校验按组件参数给出原因 |
| 玩家接入配置 | [`gateway::player::ConfigError`] |
| 注册处理链 | [`execution::registry::Error`]：DuplicateKey |
| 按键分发执行 | [`execution::registry::DispatchError`]：Unknown、Stopped、Handler，保留键与原始业务错误 |

各领域接口返回自己的 Result；组合服务由上层聚合，例如 application::Error::Discovery 单独承载发现失败，不再经 Core 透传。发现与锁后端配置校验采用相应领域错误，Application 的整体配置校验将组件校验失败转换为 config::Error::Invalid。application::Error::Configuration 与 Create 分别保留配置和服务构建错误的来源，Unsupported 表达应用尚未实现的可选能力。

调用方根据变体判断，不解析 Display 文本。新增错误类别应对应新的处理语义；增加一种底层网络错误通常仍映射到既有不可用类别。其他领域继续使用自己的错误类型，例如 player::ownership::Error、player::credentials::Error、message::rpc::Error 或 tonic::Status；跨领域转换由适配层负责。内置后端的错误信息隐藏凭据，不将底层 URL 或 token 作为诊断正文。

## 通用执行与处理链

使用 [`execution::Context`]、类型化 [`execution::Request`] 与共同的 [`execution::Layer`]。
上下文的 `service()` 表示当前执行所属服务，`operation()` 是入口适配器填写的稳定操作标识，不拼接实体或请求 ID。业务实体身份、路由输入等按需使用类型化输入或扩展携带；当前未提供通用路由器或熔断器。
执行上下文和类型化处理链位于 tok-execution，服务构建、生命周期和任务监督位于 tok-lifecycle，两者只共同依赖 tok-core；类型化能力容器使用 tok_core::Extensions；普通服务与 gateway 共用这两组设施，网关另行增加会话能力及专用协议状态机。

以下普通 Rust 输入不要求 protobuf，本地状态直接借用，经过 await 后仍在当前任务中修改：

```rust
use std::{io, time::Duration};
use tok::{execution::{Context, Layer as ExecutionLayer, Request}, core::Identity, application::CancellationToken};
use tower::{Layer, ServiceExt, service_fn};

async fn update(request: Request<&mut u64>) -> io::Result<u64> {
    tokio::task::yield_now().await;
    *request.input = request.input.checked_add(1).ok_or_else(|| io::Error::other("计数溢出"))?;
    Ok(*request.input)
}

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let context = Context::new(
    Identity { node_id: "local".into(), service_name: "counter".into() },
    "counter.changed",
    Duration::from_secs(2), CancellationToken::new(),
)?;
let mut value = 0;
let result = ExecutionLayer.layer(service_fn(update))
    .oneshot(Request::new(context, &mut value)).await?;
assert_eq!((result, value), (1, 1));
# Ok(())
# }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> { example().await }
```

Context::child 继承当前服务身份、原截止点、追踪和扩展，父取消向子传播，子取消不关闭父调用。
Extensions 共享类型化能力；插入 Arc 状态等能力由游戏明确选择，不强制把本地 &mut 状态转换为共享锁。
Context 不固定调用来源。入口适配器通过 Extensions 提供自己的类型化元信息，各维度可同时存在：

| 入口 | 已提供的扩展 |
| --- | --- |
| 游戏 RPC | [`message::rpc::CallContext`]；timer 调用另有 [`player::TimerTrigger`] |
| 事件分发 | [`events::EventId`]、[`events::Topic`]；编码接收另保留 `Arc<events::Message>` |
| 玩家请求准入与执行 | [`gateway::player::ClientRequest`]，只含 request_id / message_id |
| session 事件与 timer 执行 | 事件头及原始 `Arc<Message>`，或既有 TimerTrigger |

事件 Registry 派生子上下文，使用当前事件的 ID / topic 覆盖继承值，不改变父上下文；本地派生事件清除继承的原始编码消息。其余扩展按 Context::child 规则保留，因此 RPC 关联信息与事件头可以共存。这些值用于中间件观察，不构成认证或分发权限；请求、事件和 timer 仍由各自协议入口及分发表隔离。

ExecutionLayer 将内层就绪与执行放入同一预算，执行前后检查取消和截止点，丢弃 future 会停止本地等待，不回滚副作用。
外层 poll_ready 立即就绪，内层 readiness 在调用 future 内等待；它不承诺内层立即可执行。
调用会克隆内层 Service，共享限流状态应由对应 Layer 共享。需要预算保护的 readiness 等待应放在 ExecutionLayer 内侧。

借用输入没有静态生命周期要求，可用于直接执行的 Layer；要求后台任务为静态生命周期的 Buffer、spawn 等不能直接接受该借用。
owned 输入满足相应 Send 条件时仍可交给运行时。P1 不增加隐式队列，也不改变现有 session FIFO 或协议。

运行 `just execution-smoke` 验证 RPC 形态、事件、timer 和 gateway/session manager 能力的组合；示例不绑定端口，也不代表生产 RPC/事件适配已迁移。

## 配置一个服务

[`AppConfig`] 描述节点、端点、服务和可观测配置。
服务通过 `(node_id, service_name)` 定位，业务实体 ID 与服务身份分别管理。
游戏通过 `Application::register_factory` 注册初始化工厂。服务默认按 `service_name` 查找工厂，需要另选实现时填写可选的 `factory`。
多个服务可以引用同一个端点；端点中的协议名由对应的传输组件解释。

```rust
use tok::{AppConfig, core::ServiceDirectory, application::Application, lifecycle::{Init, Factory, InitFuture, task::Task}};

struct GameService;

impl Factory for GameService {
    fn initialize(&self, init: Init) -> InitFuture {
        Box::pin(async move {
            // 在返回前完成初始化，持有资源使用 RAII，初始化必须可取消。
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                // 收到退出通知后清理服务资源。
                Ok(())
            }) as Task)
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

[`lifecycle::Factory::initialize`] 返回初始化 future；初始化完成后返回持续运行的服务任务。
[`application::Application::run_until`] 监督任务，初始化失败、超时、提前结束或崩溃会触发整个进程清理。
服务通过 [`lifecycle::Handle::cancellation`] 接收取消通知。

初始化返回 [`lifecycle::InitFuture`]，成功后提供受监督的 [`lifecycle::task::Task`]。
任务容量预留与提交返回 [`lifecycle::task::SubmitError`]，可按 CapacityExceeded、Stopping、Closed、InvalidName 区分失败原因；名称须非空白且不超过 256 字节。提交成功仅表示入队。
调用执行错误为 [`execution::Error`]，内层错误通过标准 source 链保留；任务执行与退出错误为 [`lifecycle::task::Error`]，服务初始化与退出错误为 [`lifecycle::Error`]。后两者使用 Join 表示 Tokio 任务异常结束，正常返回错误使用 Run；任务完成报告在 panic 时仍保留提交名称。

默认初始化期限为 30 秒，整体退出期限为 10 秒，可通过
[`application::Application::with_timeouts`] 调整。
生产进程可向 `run_until` 传入 [`application::shutdown_signal`]，支持 Windows Ctrl+C 和 Unix SIGTERM。

## 通用工厂与生命周期

[`lifecycle::Factory`] 是公共构建契约，可实现 trait 或使用返回 InitFuture 的闭包。
工厂创建状态、入口和 Tower 链，返回主任务；普通服务不要求网络端点或 session。
Application 通过类型化 [`application::Binding`] 扩展注入端点和发现查询；RPC 入口可通过 Service::deferred 创建绑定槽，再在工厂中使用 message::rpc::Binding::install 安装实际处理链；示例 player/guild 已按此方式初始化状态。端点仍由 Application 共享监听，就绪后才接受调用。

任务范围与服务生命周期分别管理：

| 设施 | 职责及状态 |
| --- | --- |
| [`lifecycle::task::Supervisor`] | 通用任务所有者，驱动任务及有界退出；状态为 Open / Stopping / Stopped |
| [`lifecycle::task::Handle`] | 提交任务、预留容量、查询使用量、请求停止和等待任务范围结束 |
| [`lifecycle::Builder`] | 尚未初始化的服务；消费自身初始化，返回 Prepared |
| [`lifecycle::Prepared`] | 初始化已完成；消费自身 start，将主任务交给监督并推进就绪 |
| [`lifecycle::Running`] | 主任务已启动；run 监督服务策略并收束完整退出 |
| [`lifecycle::Handle`] | 服务阶段、停止和结束等待；通过 tasks() 操作任务范围 |

下面的普通服务没有端点，初始化提交后台任务，再按唯一顺序启动、停止并等待退出：

```rust
use std::{sync::Arc, time::Duration};
use tok::{core::{Extensions, service::Identity}, application::CancellationToken, lifecycle::{Init, Factory, Phase, InitFuture, Builder, task::Task}};

fn initialize(init: Init) -> InitFuture {
    Box::pin(async move {
        let stop = init.service.cancellation();
        init.service.tasks().launch("worker", Box::pin(async move {
            stop.cancelled().await;
            Ok(())
        })).await?;
        Ok(Box::pin(async move {
            init.service.cancellation().cancelled().await;
            Ok(())
        }) as Task)
    })
}
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let builder = Builder::new(
    Identity { node_id: "local".into(), service_name: "worker".into() },
    8, CancellationToken::new(),
)?;
let handle = builder.handle();
let factory: Arc<dyn Factory> = Arc::new(initialize);
let prepared = builder.build(factory, Extensions::default(), Duration::from_secs(2)).await?;
assert_eq!(handle.phase(), Phase::Starting);
let service = prepared.start();
assert_eq!(handle.phase(), Phase::Ready);
let running = tokio::spawn(service.run(Duration::from_secs(2)));
handle.request_stop();
running.await??;
assert_eq!(handle.phase(), Phase::Stopped);
handle.wait_finished().await; // run 成功后立即完成，无需额外 drop 所有者。
# Ok(())
# }
```

Builder 不能直接运行；build 消费 Builder，start 消费 Prepared，类型阻止重复初始化/启动。
服务就绪只在 start 推进一次；若已收到停止通知，仍启动主任务完成清理，阶段保持 Stopping。
Application 在 start 之后开启 RPC 准入并发布发现；run 不再隐式标记就绪。

通用 Supervisor 不认识工厂、主任务或 Ready。所有者用 next 取得后台完成结果，再决定策略；
服务运行层允许有限后台任务正常完成，将后台错误/panic 和主任务提前结束视为服务失败。
后台容量同时覆盖预留、排队与运行，used_capacity 表示使用的名额，包含尚未提交的预留。
生命周期句柄不重复提供 launch、reserve 或 used_capacity；统一通过 `init.service.tasks()` 或服务句柄的 `tasks()` 访问任务范围。
任务范围内部由 ScopeState 记录状态与活动数，ActivityGuard 在创建/释放时维护计数；容量守卫负责归还名额，活动守卫负责阻止范围提前结束。
初始化与主任务单独跟踪，不占后台名额。

任务范围只有一个消费式退出入口 `shutdown(timeout)`：Some 指定期限，None 由外层控制期限。
成功返回表示被跟踪任务和子范围均已释放，wait_finished 已完成；
超时中止并回收自身拥有的任务，返回 ShutdownTimeout。外部跟踪工作仍须由其所有者回收，未释放前状态保持 Stopping。
Drop 只发起取消和中止，不能承诺异步清理完成，应通过 wait_finished 观察实际释放。
任务不得等待自身范围结束。

服务 Running::run 先停止准入，收束主任务清理，再关闭任务范围；主任务持有的子组件与清理均属于完整服务退出。
Supervisor::child 创建独立容量的子范围，父取消向子传播；Supervisor::track 跟踪外部调度工作，容量和回收仍由调用方控制。
工厂使用 [`gateway::Gateway::new_in`] 并传入 `&Init`，继承所属服务身份、能力和取消关系；Gateway 保留连接循环和 ingress finish。
Gateway::tasks 暴露后台/连接任务范围，其结束不表示 ingress finish 已完成；完整网关退出使用 `Gateway::handle().wait_finished()`，也可等待所属服务 Handle 或 serve future。

Application 默认每服务允许 64 个后台名额，可用 [`application::Application::with_task_capacity`] 调整。
共享端点和发现任务由应用统一管理，子任务范围有独立容量。
`just lifecycle-smoke` 演示普通服务的状态、请求/事件链，以及 TCP gateway 的构建和完整退出。

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
内部 RPC 已通过 metadata 传播 W3C TraceContext；Core NATS 已通过标准 headers 传播，转发进入 session 后继续关联至内部事件 RPC。

## 按自定义键分发处理链

[`execution::registry::Registry`] 只根据键选择和执行 Tower 处理链，不规定输入、结果或编码方式。
`Registry<K, T, R, E>` 中，K 是实现 Ord + Clone 的分发键，T 是每次调用的自有输入，R 是结果，E 是原始业务错误。键可以是枚举、字符串或自定义类型，不要求 Display；零值和空字符串是否有效由协议或业务决定。
核心执行库不依赖 prost、Tonic 或框架生成协议。分发保留输入 Context 的操作名、期限、取消和扩展，不创建队列、增加预算或重试。共享业务状态可放在输入的 Arc 中；借用输入仍可直接通过 execution 的 Tower 调用执行，当前注册器只保存静态处理链。

```rust
use std::time::Duration;
use tok::{core::Identity, execution::{Context, Request, Registry}};
use tok::application::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Operation { Increment }

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut registry = Registry::<Operation, u64, u64, std::io::Error>::new();
registry.register(Operation::Increment, |request| async move {
    Ok(request.input + 1)
})?;
let context = Context::new(
    Identity { node_id: "local".into(), service_name: "custom".into() },
    "local.increment", Duration::from_secs(1), CancellationToken::new(),
)?;
assert_eq!(registry.dispatch(&Operation::Increment, Request::new(context, 41)).await?, 42);
# Ok(())
# }
```

注册错误 [`execution::registry::Error`] 保留重复键；运行错误 [`execution::registry::DispatchError`] 区分 Unknown、Stopped 和 Handler，并保留原始键及业务错误。

### protobuf、编号消息与 RPC 适配

[`protobuf`] 提供正文编解码适配，封装上述通用分发表；消息键 K 仍为泛型，不把 protobuf 放入执行核心。
`protobuf::Registry<K, C, O, E>` 的 C 是业务状态，O 是附加结果；Reply 包含回应消息和附加结果，`Message<K>` 包含键和编码正文。
[`message::Registry<C, O, E>`] 在其上限定 u32 非零请求与回应编号，Message 是编号消息，Reply 仍保留泛型附加结果。以下示例使用该编号消息表；本地调用不依赖 Tonic 类型。

编解码和内层 Service 的就绪等待也计入原执行范围。解码错误通过 `registry::DispatchError::Handler(protobuf::Error::Decode(...))` 返回，业务错误保留在 `protobuf::Error::Handler` 中；RPC 边界再映射为 Status。

```rust
use std::{sync::Arc, time::Duration};
use prost::Message as _;
use tok::{core::Identity, execution::{Context, Request}, message::{self, Registry, Reply, Message}};
use tok::application::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint64, tag = "1")]
    value: u64,
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut registry = Registry::<u64, (), std::io::Error>::new();
registry.register(1, 2, |target, input: Value| async move {
    if input.value != target {
        return Err(std::io::Error::other("目标不匹配"));
    }
    Ok(Reply { message: input, output: () })
})?;
let registry = Arc::new(registry);
let context = Context::new(
    Identity { node_id: "local".into(), service_name: "custom".into() },
    "local.get", Duration::from_secs(1), CancellationToken::new(),
)?;
let reply = registry.dispatch(Request::new(context, (42, Message {
    key: 1,
    body: Value { value: 42 }.encode_to_vec(),
}))).await?;
assert_eq!(Value::decode(reply.message.body.as_slice())?.value, 42);

// 游戏线协议明确使用 u32 消息 ID；同一分发表由本地和 RPC 共用。
let adapter = message::rpc::business::Handler::new(
    registry,
    |call| call.bid.as_deref().and_then(|id| id.parse::<u64>().ok())
        .ok_or_else(|| message::rpc::tonic::Status::invalid_argument("目标 ID 无效")),
    |error: std::io::Error| message::rpc::tonic::Status::invalid_argument(error.to_string()),
);
let rpc = message::rpc::Service::new("local", "custom", Arc::new(adapter))?;
# drop(rpc);
# Ok(())
# }
```

业务中间件通过 register_service 安装，本地与 RPC 调用共用该链。普通业务 RPC 适配要求 u32 消息 ID，拒绝零请求消息 ID 和零回应消息 ID；message::Registry 在注册时校验请求/回应 ID 非零，RPC HandlerRegistry 复用该表，通用分发表和 protobuf 适配本身不施加该规则。
示例公会的 guild::handlers 返回 message::Registry 及消息注册错误，业务错误保留其自身类型；RPC 组装只负责提取目标、映射错误并安装请求锁。运行 `just dispatch-smoke` 验证直接本地调用，`just rpc-smoke client-smoke` 验证同一业务链在真实 RPC/TCP 中的使用。

当前 message::rpc::HandlerRegistry 的普通请求表已委托执行库分发；timer、事件及其玩家附加结果仍由游戏协议层组合。通知归属仍待校正；通用请求锁已迁入 execution::lock，RPC 仅保留协议策略与映射。

## 内部 RPC 与共享端点

通用 RPC 设施从 [`grpc`] 导入；游戏消息、玩家事件、timer 和 CallContext 从 [`message::rpc`] 导入。通用 Client / Listener 直接从 grpc 导入，消息协议适配器不重导出它们。

自定义 protobuf 不需要依赖游戏协议：用 [`grpc::Client::execute`] 包装生成客户端；服务端由 [`grpc::Server::context`] 建立预算、追踪和取消范围，再用 [`grpc::Server::execute`] 调用任意类型化 Tower Service。协议适配器负责认证、输入校验、操作名和类型化元信息，Server 不负责解码或决定业务入口。

先用 [`message::rpc::HandlerRegistry`] 注册游戏消息，再用 [`message::rpc::Service`] 显式绑定节点和业务服务名。
player 使用 `into_player`，普通业务服务使用 `into_business`，两者使用不同的 protobuf 服务。
其他业务也可以使用自己生成的 gRPC 服务，通过 [`grpc::Listener::add_service`] 绑定。
同一端点不能重复注册同一个 protobuf 服务；多个普通业务服务共享端点时，应分别定义不同的 protobuf 服务。

下面用一个公会请求演示完整组装。游戏需要依赖 tok、prost 和 tokio。
实际运行会监听 127.0.0.1:7001，并在收到进程退出信号后清理资源。

```rust,no_run
use std::sync::Arc;
use tok::{AppConfig, message::rpc::{HandlerRegistry, Service}, application::{Application, shutdown_signal}, lifecycle::{Init, InitFuture, task::Task}};

#[derive(Clone, PartialEq, prost::Message)]
struct ReadGuild {
    #[prost(uint64, tag = "1")]
    guild_id: u64,
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
    let mut app = Application::new(config)?;
    app.register_rpc_factory("guild", |init: Init| -> InitFuture {
        Box::pin(async move {
            // 状态、入口和 Layer 在同一个工厂中初始化。
            let mut handlers = HandlerRegistry::new();
            handlers.register(1, 2, |context, request: ReadGuild| async move {
                if context.bid.as_deref() != Some(request.guild_id.to_string().as_str()) {
                    return Err(tok::message::rpc::tonic::Status::invalid_argument("业务 ID 不匹配"));
                }
                Ok(request)
            })?;
            init.extensions.get::<tok::message::rpc::Binding>()
                .ok_or_else(|| std::io::Error::other("缺少 RPC 初始化绑定"))?
                .install(&init, Arc::new(handlers))?;
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                Ok(())
            }) as Task)
        })
    }, Service::into_business)?;
    app.run_until(shutdown_signal()).await?;
    Ok(())
}
```

Application::register_rpc_factory 按配置的 factory（默认服务名）建立入口，将所属 `message::rpc::Binding` 注入 Init.extensions；工厂安装实际处理链，未安装会在初始化结束时失败，不发布服务。多个服务可共用工厂，但同一端点仍不能重复绑定同一个 protobuf 服务。协议转换函数不决定锁或中间件。

自定义 protobuf 服务通过 `Application::bind_rpc(service_name, server)` 声明；生命周期沿用 register_factory。已有完整 Listener 可交给 register_rpc_endpoint，端点必须与配置完全匹配，同一端点不与逐服务声明混用。高级自定义组装仍可手动使用 Service::deferred；不需要普通使用者维护一份按服务名查询的绑定表。

应用先合并并校验共享端点，再绑定监听，业务初始化完成后才开启对应服务的就绪入口。
初始化失败、监听失败和运行异常都进入同一清理流程。
工厂初始化不能等待尚未启动的其他服务；依赖调用应在必要服务就绪后发起。

调用方通过 [`application::Application::service_directory`] 或 [`application::Binding::directory`]
查询服务，再用 [`infra::routing::route`] 按 bid 选点。
[`grpc::Client`] 管理共享连接、缓存撤销、默认期限及通用调用执行，不包含 Player 或 Business 方法。
[`message::rpc::player::Client`] 提供 call / handle_event，[`message::rpc::business::Client`] 提供普通业务 call；
构造时分别传入同一个 `Arc<Client>`，协议适配器共享连接但保持各自的调用约束。
自定义 protobuf 客户端推荐通过 Client::execute 接入统一期限、追踪和指标；也可以使用 Client::channel 取得 Channel，自行管理调用预算和协议适配。
Client 的默认期限须大于零且不超过一天。execute 保留更短的请求 grpc-timeout，准备与发送共用本地截止点，超期结果不会成功返回；这不会中断阻塞代码或回滚远端副作用。
Channel 为延迟连接，实际发送时才建立网络连接。

客户端通过 [`grpc::Client::with_endpoint_factory`] 定制 Tonic Endpoint，服务端通过 [`grpc::Listener::from_server`] 接收配置好的 Tonic Server：

```rust
use std::time::Duration;
use tok::grpc::{self, tonic::transport};

# fn example() -> Result<(), grpc::Error> {
let client = grpc::Client::new(Duration::from_secs(3), 128)?
    .with_endpoint_factory(|target| {
        Ok(transport::Endpoint::from_shared(format!("http://{}", target.address))?
            .connect_timeout(Duration::from_secs(1))
            .http2_adaptive_window(true))
    });
let server = transport::Server::builder()
    .http2_adaptive_window(Some(true));
let endpoint = grpc::Listener::from_server(server);
// 接着使用 endpoint.add_service(...) 绑定服务，并驱动监听 Future。
# drop((client, endpoint));
# Ok(())
# }
```

默认构造仍采用明文连接；启用 TLS 时，应用依赖可为 `tonic` 开启 `tls-ring` 或 `tls-aws-lc` feature，客户端工厂构造 HTTPS URI 并配置 ClientTlsConfig，服务端配置 ServerTlsConfig 后传给 from_server。框架不复制 Tonic 的证书或 HTTP/2 配置，也不将密钥放进服务发现。
工厂在缓存锁外按需执行，并发缓存未命中时可能多次调用，应当快速返回且无副作用。同一节点和地址共享 Channel，必须具有一致的 URI/TLS 等连接策略，不能随业务服务名变化；替换工厂会清空本实例缓存，在途克隆继续使用原连接。自定义连接参数由工厂负责，execute 的整体预算始终生效。
from_server 接受未添加 Tower Layer 的 Tonic Server，业务中间件仍可在 add_service 时包装各个服务；就绪控制和业务绑定校验继续由 grpc::Listener 管理。

[`grpc::Server`] 是逻辑服务的调用执行入口；[`grpc::Listener`] 负责网络监听和 protobuf 服务绑定。
[`grpc::Readiness::set`] 返回是否命中已绑定的服务名，直接指定服务时应检查结果；Application 遍历多个端点时允许未命中。
Channel 缓存有界，节点和地址相同的服务复用连接；节点变更时调用 `invalidate` 撤销该节点缓存。
请求携带 [`message::rpc::CallContext`]，player 调用还需匹配的 [`player::SessionIdentity`]。
上下文校验检查目标和字段一致性，不能代替内部调用身份认证或全局 session 所有权校验。

每次调用配置期限且不自动重试。超时不表示远端业务已回滚；未知结果由游戏处理。
用户可通过 [`message::rpc::TowerHandler`] 注入自行组装的 Tower Service，选择并发限制、过载和锁策略。
普通业务入口不会自动加锁。
[`message::notification`] 编码强类型通知，真正下行序号和补发由后续网关 session 分配。

运行 `just rpc-smoke` 可以验证示例 player 与 guild 的内部调用并退出。
此检查使用构造的内部会话上下文；登录认证和客户端 session 由 `just client-smoke` 单独验证。

### 无端点的事件服务

ServiceConfig.endpoint 是可选引用。纯事件或后台服务省略 endpoint，仍经过相同 Factory 初始化、主任务及后台监督、停止和结束等待；不生成 ServiceDescriptor，不发布发现地址，也不注入网络用 application::Binding。需要查询其他服务时，可以在组装时取得 Application::service_directory 并注入自己的工厂。

```toml
node_id = "local"
discovery = "memory"
[[services]]
service_name = "audit"
```

服务工厂仍负责构造事件 Registry、状态和 Layer，并将接收循环作为主任务或受监督任务；可以选择本地来源或 Core NATS，不要求构造 gateway。可运行的组合见示例 `examples/demo-game/examples/application.rs`，执行 `just application-smoke`：RPC 发布本地事件，无端点审计服务借用状态处理，两者安装同一个泛型 Layer。网关及区服主题的完整组合继续由 demo 主程序与 nats-smoke 验证。

## 请求级锁

[`infra::locks::Memory`] 提供有界单进程请求锁，克隆共享状态；
[`infra::locks::Redis`] 提供跨进程请求锁，配置为 [`infra::locks::RedisConfig`]。
两者实现 [`core::lock::Backend`]，返回显式释放的 [`core::lock::Lease`]。
Redis 后端使用有限 TTL 和唯一持有者标识，Lua 比较身份后释放；旧租约不能删除新锁。
后端不续租，取消后不在 Drop 中异步释放，依靠 TTL 兜底。

[`execution::lock::Layer`] 是通用请求锁处理链，显式接收从 Request 提取 Key 的函数，以及判断 Ok 回应是否表示业务成功的函数。输入、回应和错误均由调用方定义，不读取 CallContext、不构造 Status，也不判断 protobuf 字段。当前输入、回应和内层错误要求自有 Send + static 类型。

```rust
use std::{sync::Arc, time::Duration};
use tok::{
    core::{Identity, lock::Key},
    execution::{Context, Request, lock},
    infra::locks::Memory,
    application::CancellationToken,
};
use tower::{Layer as _, ServiceExt};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let layer = lock::Layer::new(
    Arc::new(Memory::new(32)?), lock::Config::default(),
    |request: &Request<u64>| Ok::<_, std::io::Error>(Key {
        namespace: "inventory".into(), bid: request.input.to_string(),
    }),
    |_: &u64| true, // 此处理链的 Ok 回应都表示成功。
)?;
let service = layer.layer(tower::service_fn(|request: Request<u64>| async move {
    Ok::<_, std::io::Error>(request.input + 1)
}));
let context = Context::new(
    Identity { node_id: "local".into(), service_name: "inventory".into() },
    "inventory/update", Duration::from_secs(2), CancellationToken::new(),
)?;
assert_eq!(service.oneshot(Request::new(context, 41)).await?, 42);
# Ok(())
# }
```

通用 Config 只定义时间预算，命名空间来自取键函数。`lock::Error<E>` 区分取键、后端获取、整体获取超时、剩余租期不足、业务执行超时、原始业务错误、释放失败与原上下文停止。获取、执行和释放共用原 Context 的期限、取消和追踪；内层 Service 的 readiness 也计入持锁执行预算。释放失败时保留原始 Err，以及结果判断函数判为失败的 Ok 回应，只覆盖业务成功。

[`message::rpc::lock::Layer`] 通过 [`message::rpc::TowerHandler`] 在普通业务 RPC 的上下文校验之后包住 handler。
按逻辑服务名与上下文 bid 加锁，空 bid 拒绝执行，不同服务隔离；节点名不进入锁键。
由 Rust 组装代码按服务显式安装，不需要 `ServiceConfig` 开关：

```rust
use std::sync::Arc;
use tok::{core::lock::Backend, infra::locks, message::rpc::{Handler, HandlerRegistry, Service, TowerHandler, lock, tower::{self, Layer}}};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let backend: Arc<dyn Backend> = Arc::new(locks::Memory::new(1024)?);
let registry = Arc::new(HandlerRegistry::new()); // 在此注册业务 handler。
let service = tower::service_fn(move |request| {
    let registry = Arc::clone(&registry);
    async move { registry.call(request).await }
});
let layer = lock::Layer::new(backend, "guild", tok::execution::lock::Config::default())?;
let rpc = Service::new("node-a", "guild", Arc::new(TowerHandler(layer.layer(service))))?
    .into_business();
# drop(rpc);
# Ok(())
# }
```

多进程时将后端替换为 `Arc::new(locks::Redis::new(locks::RedisConfig { namespace: "dev".into(), ..Default::default() })?)`，
所有节点须使用相同 Redis 与环境命名空间。请求级锁与 session 所有权独立注入，不从 ownership 配置推导。
示例 `demo_game::application` 默认给 guild 安装 Memory 锁；多进程共享业务使用
`demo_game::application_with_locks` 显式注入共享后端。player 仍由 session 串行执行，网关交接接口不安装此 Layer。

[`execution::lock::Config`] 默认竞争等待 100ms、整体获取期限 500ms、租期 3s、业务执行期限 1s、释放期限 100ms。
从获取开始按单调时钟扣除耗时，剩余租期必须容纳完整执行预算和释放余量，否则拒绝执行。
内部 Service 的 readiness 等待也计入执行预算；需要整体并发限制时在锁 Layer 外侧组装共享限流器。
`Memory::new(capacity)` 分别限制活动键和并发获取数量，Redis 的 `max_pending` 限制本地并发获取，超限快速拒绝。
获取竞争可在等待期限内尝试，业务调用不重试；后端操作结果不确定时不重试写操作。

正常结束或 Layer 内部超时后显式等待有界条件释放，外层丢弃整个调用 future 时由 TTL 兜底。
获取/执行超时返回 `DeadlineExceeded`，容量不足返回 `ResourceExhausted`，后端故障返回 `Unavailable`。
释放失败会记录系统错误：原 handler Status 或非零 RpcError 保留，业务成功则返回 `Unavailable`，结果不能视作回滚。
这遵循既定超时与租约竞态边界，不保证异常路径下的绝对互斥，也不强制存储使用 fencing。
`just locks-test` 显式验证真实 Redis 的跨进程竞争、过期、旧租约条件释放及释放断线；常规测试无需 Redis。

## 服务发现与内存所有权

[`core::ServiceDirectory`] 提供快照和变更订阅。先订阅再读取初始快照，每次通知后重新读取需要的服务。
通知可以合并；不能根据通知次数推断服务变更次数。
[`application::Application::with_service_directory`] 可注入查询后端；本进程注册和退出撤销仍由应用管理。
[`application::Application::directory`] 返回本进程的内存注册目录。

[`infra::ownership::MemoryOwnership`] 是有界的单进程所有权后端，实现
[`player::ownership::Store`] 的条件获取、续租与释放。
接管必须匹配旧 epoch，旧持有者不能续租或释放新所有权。
其 epoch 仅在后端存活期间有效，不能用于跨进程或进程重启后的协调；跨网关共享所有权使用下面的 Redis 后端。

## Redis session 所有权与超时边界

[`infra::ownership::redis::RedisOwnership`] 实现 [`player::ownership::Store`]，
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
timer、同网关恢复、可选的跨网关交接和请求级锁均已接通；Redis Cluster 客户端仍待实现。

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
工厂通过 `init.extensions.get::<application::Binding>()` 获取 Application 注入的绑定，再由 [`application::Binding::listen_address`] 取得监听地址，`binding.service.address` 是发布地址。
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

`Application::directory()` 始终返回本进程的内存注册镜像；跨进程查询必须使用 `Application::service_directory()` 或工厂扩展中的 `Binding.directory`。
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
协议正式发布后，数值保持稳定，不因重排枚举或 Rust 模块而改变；删除已发布消息时将其编号与枚举名称标记为 `reserved`，不能复用。发布前可直接调整结构与编号。
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
use tok::gateway::{codec::{FrameCodec, VERSION}, protocol::{Frame, LoginRequest, BusinessRequest, Ack, frame::Content}};
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
`gateway::Gateway::player` 注入认证器、[`player::ownership::Store`] 和
[`gateway::player::Executor`]；[`gateway::player::RpcExecutor`] 可组合
[`infra::routing::RendezvousRouter`] 与 [`message::rpc::player::Client`] 调用 player 服务，后者复用共享 Client。
网关工厂先绑定 TCP 监听，再把监听及服务退出 token 交给 `serve` 的受监督任务。
客户端正文不提供 session 身份；示例 player 从可信的调用上下文读取玩家 ID。

player 内部按职责组织会话通信与执行：

| 名称 | 职责 |
| --- | --- |
| `SessionHandle` | 向逻辑会话提交业务、发布通知及请求关闭 |
| `BoundSession` | 携带连接代次的操作入口，校验连接是否仍然有效 |
| `channel::Channel`（内部） | 管理连接绑定、请求准入、下行发布、确认与断线恢复 |
| `replay::ReplayCache`（内部） | 保存已编码下行消息，查询可连续补发的范围 |
| `ReplayPlan` | 由连接写循环执行恢复响应、补发及恢复完成标记 |
| `SignedResumeTokens` | 使用本地共享密钥签发和验证，不存储逐 token 记录 |
| `timer::Scheduler`（内部） | 调度 timer 并将到期任务送入会话队列 |

通信、缓存和计时机制封装在私有 `session` 模块内，恢复凭证位于 `player::resume_token`。
对外从 `player` 导入 `SessionManager`、`SessionHandle`、`PendingResponse`、`ReplayBatch`；无需依赖内部文件路径。

内部实现按职责组织：`session/manager.rs` 管理目录、登录、恢复及凭证刷新协调；
`handle.rs` 提供会话与连接操作视图；`state.rs` 存放共享会话数据；`worker.rs` 串行执行业务并负责续租与退出；
`handoff.rs` 实现跨网关交接；`channel.rs`、`replay.rs`、`timer.rs` 分别管理通信绑定、确认缓存和计时调度。
这些模块保持私有或仅在 `player` 内可见，公共 API 经 `player` 统一导出。

原模块路径 `player::outbound`、`player::resume` 已相应调整。

[`gateway::player::Config`] 对连接数、会话数、请求队列、下行队列、包长及期限提供有界默认值。
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
[`gateway::player::ReplayBatch`]，其 `through_seq` 是查询时的最大序号，`frames` 是按序编码正文。
范围有缺口或位置越界返回 `None`；已确认释放的字节不能再从更早位置查询，关闭后返回 `SessionClosed`。
查询不发送、不确认消息；调用方持有快照会延长对应字节的生命期，应及时释放。
`try_request` 的独立回应只交给调用方，仍无下行序号，也不自动写入客户端缓存。

### 同网关断线恢复

成功登录的 `LoginResponse` 包含 `resume_token` 和 `resume_token_ttl_ms`。
凭证由 HMAC-SHA256 认证，载荷包含玩家、原 session、签发和过期时刻；客户端原样保存，不假定固定长度。
[`player::credentials::ResumeTokenCodec`] 是同步的本地签发/验签契约，不访问 Redis 或其他网络来源。
[`gateway::player::resume_token::SignedResumeTokens`] 使用启动时加载的共享密钥签发和验证。
默认生成进程临时密钥，重启后旧 token 无法验证；配置相同共享密钥后各网关可独立验证，不共享凭证表。
凭证默认有效 1 小时，宽限期默认 30 秒，两者独立；恢复、交接及验签不刷新原过期时刻。

在线客户端可发送 `RefreshResumeTokenRequest { request_id }` 主动刷新，成功收到
`RefreshResumeTokenResponse { request_id, error_code: 0, resume_token, resume_token_ttl_ms }` 后替换保存的凭证。
建议按成功签发/刷新返回的 TTL 提前刷新，例如在有效期经过约 80% 时发起，并为网络延迟留出余量；
这与连接保活独立，客户端仍须遵守完整帧读取的空闲期限。
请求不携带旧 token，授权依据是当前已认证的在线连接及有效会话所有权；原 token 已过期仍可在线刷新。
未登录、旧连接、被顶号或所有权失效的会话不能刷新；补发完成前和交接冻结期间也不允许刷新。

刷新只重新签发同一 session_id 的凭证，从签发时重新计算 `resume_token_ttl_ms`，
不更改 epoch、请求进度、timer 或下行序号，不建立新的所有权，也不执行游戏认证器或 player RPC。
刷新前会条件续租确认所有权；校验、签发与回应入队同会话替换、交接及连接重绑定协调。
两个新增帧为控制消息，不占 `server_seq`、不进入补发缓存；`request_id` 必须为当前连接内递增的非零值，
与登录、恢复及业务请求共用计数。成功回应丢失时客户端可在仍在线的连接用新请求 ID 重试。
旧 token 保留原到期时间，不因刷新立即撤销；同一会话的多个未过期 token 都可验证，
新登录替换 session_id 后全部失去会话接入资格。断线后不能凭已过期 token 刷新，须普通鉴权。
客户端 Frame 为版本 3，刷新凭证使用 oneof 字段 12、13；使用刷新能力须更新客户端协议。


新登录仍会失效旧入口。签名有效不代表能进入当前会话：Resume 必须匹配原 session；快速重登通过
[`player::ownership::Store::acquire_for_relogin`] 原子比较该玩家最新登录的 session_id，再获取新所有权。
新登录更新此标记，交接与租约释放保留，因此旧 token 不能顶回新会话，也不能在新会话下线后重新进入。
Redis 只保存所有权与最新登录标记，不保存 token；这属于会话协调，不是验签的一部分。

EOF、帧 I/O 超时、传输失败或下行队列满时，session 在服务器观察到断线后进入宽限期。
已排队和在途调用继续收束，timer 与租约续期继续；回应、通知进入原下行缓存。
`disconnect_grace_ms=0` 保留立即关闭行为，最大 86400000；
`resume_token_ttl_ms` 为正且不超过 86400000；不再配置凭证表容量。
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
伪造、过期不匹配或格式不合法 返回 `AuthenticationFailed`。签名仍有效但原会话已替换时，恢复返回 `ResumeUnavailable`，快速重登返回 `SessionReplaced`。默认临时密钥无法验证其他独立进程签发的凭证。

框架不固定游戏初始化消息。快速重登后的状态请求应返回完整状态与 `TimerUpdate::ReplaceAll`，重建 timer 集合；
demo 的 `GetPlayer` 从服务端保存的生产截止时间重建 timer，已完成任务不会重复结算。
同网关恢复复用原 timer 集合，不重新安装。`just client-smoke` 包含在线刷新、使用新凭证恢复原会话、断线期间生产及状态校验。

### 跨网关交接

示例使用 `ownership = "redis"` 和多节点发现，并在 `[player_gateway]` 设置 `handoff_enabled = true`。
参与节点配置相同的固定共享密钥；`[resume_token]` 示例为：

```toml
[resume_token]
# 公开示例密钥，仅供开发，部署时替换为随机共享密钥。
secret_key = "1111111111111111111111111111111111111111111111111111111111111111"
clock_skew_ms = 5000
```

不同游戏及开发、正式环境必须使用独立共享密钥来隔离凭证。
密钥在启动时加载；维护时统一更换共享密钥并重启所有参与网关，旧 token 失效，客户端需普通登录。
`clock_skew_ms` 只容忍签发时刻领先本机，不延长过期时间；跨节点仍依赖部署侧时间同步。

每个节点配置名为 `gateway-rpc` 的网关内部 gRPC 服务；`gateway_rpc_service_name` 可覆盖发现名称。
自定义组装先通过 [`gateway::player::Ingress::with_resume_token_codec`] 注入本地 codec，
再通过 [`gateway::player::Ingress::with_gateway_rpc`] 注入目录并向
[`gateway::rpc::GatewayRpcService`] 登记会话管理器；内部服务交给 [`grpc::Listener::add_service`]。
统一入口当前提供 PrepareHandoff、CommitHandoff、CancelHandoff、CompleteHandoff，未来网关管理 RPC 在同一服务扩展。
登记不受 handoff_enabled 限制，交接关闭时由具体交接操作拒绝；不会为每组方法重复登记管理器。
普通 credential 登录仍不能直接抢占远端活动租约；恢复交接由旧网关协作完成。

新连接仍发送同一个 `ResumeRequest`。旧网关冻结新准入，排空已接收请求和 timer，导出原编码下行缓存、请求进度及 timer 调度快照；
新网关校验并预留容量后，由旧网关串行协调提交与取消，再比较完整旧所有权原子转移。
成功保留 `session_id` 和下行序列、增加 `epoch`，先回应、补发，再发送 `RecoveryComplete`。
交接期间新的请求和主动通知明确失败，不透明转发或重试；已成功发布的通知进入快照。

`handoff_timeout_ms` 默认 15000（1..60000），`max_handoffs` 默认 16（1..4096），
`max_handoff_bytes` 默认 8388608（65536..134217728）。源准备记录和目标暂存共用节点交接容量；
快照有独立大小校验，内部消息上限同时显式设置。缓存和原恢复窗口不会因交接刷新，timer 不补跑全部历史周期。

提交前失败可取消准备；旧所有权与恢复窗口仍有效时，旧网关恢复运行。提交开始后结果不确定先查询，旧入口不会自行恢复。
接管后失败关闭新入口并条件释放所有权。旧网关宕机、快照缺失或缓存有缺口时返回 `ResumeUnavailable`，
客户端等待原租约释放或过期后，用仍有效的 token 快速重登并请求完整状态；不回滚业务或重发未知结果请求。

本地启动 Redis 和游戏进程后，分别执行：

```text
just run examples/demo-game/config/redis-game.toml
just run examples/demo-game/config/redis-handoff-a.toml
just run examples/demo-game/config/redis-handoff-b.toml
```

两个网关的客户端端口为 7100、7200，内部端口为 7101、7201；只适用于受信开发环境。
`just resume-token-test` 离线验证签名、期限和维护换密钥失效，`just handoff-smoke` 验证三个独立进程的交接、补发、timer 和宕机降级。
测试通过 `TOK_TEST_REDIS_URL` 选择 Redis，使用独立 namespace；常规 `just verify` 不依赖外部服务。

## 玩家 timer

timerSpec 由服务端业务逻辑生成，不作为客户端调度接口。客户端只发起“开始生产”等业务动作，
耗时、执行消息、参数与产出均由服务端规则决定。

业务请求通过 [`message::rpc::HandlerRegistry::register_with_timers`] 返回 [`message::rpc::HandlerReply`]，
其 `timers` 使用 [`player::TimerUpdate`]。`None` 保持原集合，`ReplaceAll` 全量替换，空集合清空；
`Patch` 可增改或取消指定 ID。timer ID 必须非空且不超过 256 字节，消息 ID 非零，周期必须大于零；
同一次更新不能重复操作同一 ID。数量和 ID、正文总字节数受 `max_timers`、`max_timer_bytes` 限制，
默认分别为 128 和 1048576。更新先完整校验再替换；非法或超容量的更新会终止 session，避免继续驱动旧集合。

```rust
use prost::Message;
use tok::{player::{TimerSpec, TimerUpdate}, message::rpc::{HandlerRegistry, HandlerReply}};

#[derive(Clone, PartialEq, Message)]
struct StartProduction {}
#[derive(Clone, PartialEq, Message)]
struct ProductionFinished {}

# fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut handlers = HandlerRegistry::new();
handlers.register_with_timers(1, 2, |_context, _request: StartProduction| async move {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| tok::message::rpc::tonic::Status::internal("系统时钟无效"))?.as_millis() as i64;
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

[`message::rpc::HandlerRegistry::register_timer`] 独立于客户端请求分发表；`CallContext.timer` 由内部 RPC 信封传递，
客户端不能通过填写 timer 的消息 ID 进入该处理函数。它包含 ID、预定与实际触发时刻，仍需遵循部署侧的内部调用身份约定。
业务正文只包含游戏参数，timer 更新和触发来源不直接发送客户端。
直接构造 [`message::rpc::CallContext`] 的普通调用需填写 `timer: None`；自定义 Handler 和 Tower 中间件可以读取该字段识别内部计时来源。

session 建立时对齐 UTC 与单调时钟，此后按同一基准等待，墙钟调整不会重放已调度的周期。
每个 session 使用一个调度 future，与执行循环共同受网关监督，不为每个 timer 创建任务。
到期只进入已有的有界执行队列，排队与执行期间同一 timer 合并为一个触发，满队列等待容量；客户端可能收到 QueueFull。
执行前检查调度版本、取消状态和所有权预算；旧更新对应的排队任务失效。
周期错过的时刻不逐次追赶，完成后按原基准计算下一个未来时刻；一次性 timer 触发后移除。
timer 失败或超时只记录本次失败，不重试；周期的后续触发独立进行。

自定义 [`gateway::player::Executor`] 的 `execute_request` 返回 [`gateway::player::Reply`]，
普通 `LogicalMessage` 可通过 `.into()` 转换；需要计时执行时实现 `execute_timer` 方法。
原 `execute`、`timer` 方法已分别改为上述名称，自定义实现需同步调整。
`execute_timer` 的关联序号由 session 独立生成，不消耗客户端 request_id；计时结果可继续返回更新，不产生客户端回应。
示例 `START_PRODUCTION` 发起生产，由服务端安装一次性 timer；内部 `FINISH_PRODUCTION` 按固定规则更新玩家状态。
请求中没有 timer 配置或产出参数，`just client-smoke` 验证完整 TCP 与 RPC 链路。
当前由业务响应安装 spec，未增加隐式的 player 登录调用。断线宽限期内 timer 继续运行；到期、显式关闭或租约失效时清理。快速重登通过游戏初始化请求重新安装完整集合。

## 在工厂中安装 RPC 处理链

普通应用使用 `Application::register_rpc_factory` 自动注入一次性 [`message::rpc::Binding`]。下面是高级手动组装方式：[`message::rpc::Service::deferred`] 返回端点服务和绑定，再在所属 Factory 中安装状态与处理链。
绑定校验服务身份、拒绝重复安装；安装前调用返回 Unavailable，服务停止后拒绝继续执行业务。共享端点的就绪门控仍由 Application 管理。

```rust
use std::sync::Arc;
use tok::{message::rpc::{Service, HandlerRegistry}, lifecycle::{Factory, Init, InitFuture, task::Task}};
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let (endpoint, binding) = Service::deferred("node-a", "guild")?;
let factory = move |init: Init| -> InitFuture {
    let binding = binding.clone();
    Box::pin(async move {
        let registry = HandlerRegistry::new(); // 在此创建状态、注册路由和安装 Layer。
        binding.install(&init, Arc::new(registry))?;
        Ok(Box::pin(async move {
            init.service.cancellation().cancelled().await;
            Ok(())
        }) as Task)
    })
};
# fn assert_factory(_: impl Factory) {}
# assert_factory(factory);
# drop(endpoint);
# Ok(())
# }
```

RPC、timer 的 `register_service` / `register_timer_service` 接收 `Request<(CallContext, Req)>`；事件路由使用 `Request<events::Input<...>>`。
同一个泛型 Layer 可以读取共同的 `request.context`，正文和结果保持各自类型。`HandlerRegistry::events_mut()` 配置事件链，`events()` 读取同一表供本地执行。
RPC 默认服务执行上限为 30 秒，可用 `with_execution` 显式设置；有效 grpc-timeout 只会缩短上限。停止和期限错误只在 RPC 边界映射为 Status。
事件多个处理者的 timer 更新按注册顺序合成，后续 ReplaceAll 覆盖之前更新；最终更新仍由 session 校验和应用，不产生客户端回应。

## 统一事件注册与本地执行

[`events::Event`] 声明稳定的 `NAME`，使用 1 到 128 字节的 ASCII 字母、数字、点、短横线或下划线，点分段不能为空。
`DELIVERY` 默认 Immediate；本地执行不应用传输投递策略。普通 Rust 事件无需 prost。

[`events::Registry`] 同时保存多种事件，每种事件可按顺序注册多个处理者。容量按处理者总数计算。
`register` 接收可跨 await 借用状态和事件的回调；`register_service` 安装相同输入的 Tower 链；同步回调通过 [`execution::sync_handler`] 适配。
`dispatch(...).await` 直接在当前任务执行，不 spawn、不进入 session 队列，不内部获取实体锁。

```rust
use std::time::Duration;
use tok::{execution::{Context, Request}, core::service::Identity, events::{Event, Envelope, EventId, Registry}};
use tokio_util::sync::CancellationToken;
struct Changed { delta: i64 }
impl Event for Changed { const NAME: &'static str = "game.value.changed.v1"; }
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut events = Registry::<i64, (), std::io::Error>::new(4)?;
events.register::<Changed, _>(|request| Box::pin(async move {
    tokio::task::yield_now().await;
    *request.input.state = request.input.state.checked_add(request.input.event.payload.delta)
        .ok_or_else(|| std::io::Error::other("数值溢出"))?;
    Ok(())
}))?;
let parent = Context::new(Identity { node_id: "node-a".into(), service_name: "player".into() },
    "update", Duration::from_secs(1), CancellationToken::new())?;
let event = Envelope::new(EventId::generate(), Changed { delta: 3 })?;
let mut value = 0;
let results = events.dispatch(&mut value, Request::new(parent, &event)).await?;
assert_eq!(results.len(), 1);
assert_eq!(value, 3);
# Ok(())
# }
```

事件上下文由注册表派生为 `event/<NAME>`，继承原期限、取消、追踪和本地能力；同一次分发中的处理者共享期限，不逐个重置预算。
首错停止，`DispatchError::Handler` 保留原业务错误；协议/解码失败、未知类型和取消/超时分别表达。
零订阅者返回空结果。已执行的状态修改不回滚，不自动重试或去重，也不截获 panic。
异步回调可以借用 `&mut` 状态，调用方必须保障状态的串行访问；不应持有标准互斥锁跨 await。

远程接收通过 `enable_protobuf::<E>()` 给同一类型安装解码器，再调用 `receive(...).await` 执行同一组处理者。
正文只解码一次，原始 `Arc<Message>` 保留在执行扩展中；本地 `dispatch` 清除继承的原始编码，避免把父事件误当成当前事件转发。
`just events-smoke` 演示同一回调在本地和解码接收时执行。示例 player 的生产完成处理也已使用此注册表，timer 入口派生原执行上下文后 await 分发。
事件结果只用于当前服务端处理，不单独生成客户端 Response。

[`events::Envelope`] 保存稳定 [`events::EventId`]、强类型正文及 [`events::TraceContext`]，不携带玩家目标或客户端消息 ID。
业务侧使用强类型 `Envelope<E>`，运行时使用正文已编码的 `Message`，网络协议使用 `tok_proto::internal::v1::EventEnvelope`；追踪信息通过 NATS headers 传输。
新事件用 `EventId::generate()` 生成 UUID v4；同一次事件重复发布保持原 ID，框架不自动去重。
ID 限制为 1 到 128 个可打印 ASCII 非空白字节，追踪头每字段最多 512 字节。
标准 OpenTelemetry 传播器负责注入提取，未指定追踪头时 Message 构造默认采集当前 span。

## Core NATS 即时事件

本节介绍当前按 topic 和类型建立的固定兴趣订阅。P5 已接通传输路由，本地与远程注册表及 gateway/session 处理链沿用共同协议；P6 已提供按组件绑定的会话兴趣聚合，不按 player_id 自动订阅。

任何服务都可以使用 [`events::nats::Bus::publish_event`] 发布，用 [`events::Registry`] 订阅自己关心的类型。
目标由业务正文表达，订阅者自行解释；总线不要求 player、session 或 RPC。
每个 Registry 可对一种类型有序注册多个处理函数；多个服务可以分别订阅同一种事件。
处理函数可捕获状态与发布连接，也可以实现 [`events::Handler`] 自定义分发。

```rust,no_run
use std::sync::Arc;
use tok::events::{self, Envelope, Event, EventId};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct GuildChanged { #[prost(uint64, tag = "1")] guild_id: u64 }
impl Event for GuildChanged {
    const NAME: &'static str = "game.guild.changed.v1";
    // 这类事件固定属于 realm:42，发布者不再额外填写。
    fn topic(&self) -> Result<events::Topic, events::Error> {
        events::Topic::named("realm:42")
    }
}

# async fn example(stop: CancellationToken) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let bus = events::nats::Bus::connect(events::nats::Config::default()).await?;
let topic = events::Topic::named("realm:42")?;
let mut handlers = events::Registry::new(16)?;
handlers.register::<GuildChanged, _>(|request| Box::pin(async move {
    let event = &request.input.event.payload;
    // 这里可以更新公会状态、处理邮件或编排其他服务器操作。
    println!("{}: {}", request.input.event.event_id, event.guild_id);
    Ok(events::Outcome::Handled)
}))?;
handlers.enable_protobuf::<GuildChanged>()?;
let subscription = bus.subscribe(events::Subscriber::new(
    tok::core::service::Identity { node_id: "node-a".into(), service_name: "guild".into() },
    Arc::new(handlers), events::TopicFilter::Exact(topic.clone()), None,
)?).await?;
bus.publish_event(Envelope::new(EventId::generate(), GuildChanged { guild_id: 7 })?).await?;
// 将此 future 交给服务运行时监督；生产代码不能遗留脱管任务。
subscription.run(stop).await?;
# Ok(())
# }
```

事件通过 `Event::topic(&self)` 决定路由，默认返回 `Topic::default()`：普通全局事件只需声明 NAME，不必填写额外 topic。可覆盖为固定主题，或根据事件的 channel_id / realm_id 字段计算具名主题。该方法必须是无 I/O、无副作用的确定性计算。

`Envelope::new` 从事件取得 topic；没有发布时覆盖 topic 的接口。修改公开正文或 topic 后，validate / 编码会再次核对；Registry 远程解码也在进入处理链前核对正文的路由契约。Message 是未解码的表示，仅能检查主题格式；自定义原始 Handler 若绕过 Registry，需要自己验证正文契约。本地 dispatch 读取相同的 Envelope.topic，是显式调用，不模拟 broker 过滤或消费组。

`Subscriber::new(identity, handler, TopicFilter::Exact(topic), None)` 只订阅该 topic 与 Handler::event_types 声明的类型；`Some("mail.reward")` 让同名 group 的匹配实例分担事件。`TopicFilter::All` 是显式的所有 topic 兴趣，仍只接收声明的事件类型及当前 namespace。默认兴趣与 All 不同，具名 `"default"` 也与默认作用域不同；通配兴趣不能作为发布主题。

Topic::named 接受最多 256 个 UTF-8 字节，允许点、冒号、斜线及 Unicode，拒绝空名、空白、控制字符和 `*` / `>`。名称不作大小写或 Unicode 归一化。NATS 适配器集中生成 `tok.events.<namespace>.<topic-token>.<event_type>`：默认 token 为 d，具名 token 为 n 加原始 UTF-8 的小写十六进制；All 在该位置使用单 token 通配符。业务不拼接 subject，接收时核对正文 topic、事件类型与规范 subject，拒绝伪造或非规范编码。
不同 group 分别得到一份。group 属于订阅配置，不属于 Event，不从服务名推导；全局路由也不意味着每个实例都要执行业务。消费组不提供 bid 归属路由、持久化或去重。
按 bid 持有状态的服务仍需自行路由到对应实例。group 属于每个订阅组件的组装选择，网关不固定消费策略。仅查询本地会话的组件在多网关部署中需要各节点独立接收；若采用同组消费，组件必须保证事件能够路由到目标会话所在节点。

克隆 Bus 共享连接、发布容量与订阅预算；同一连接可建立多个逻辑订阅。每个订阅串行等待 handler，不新增并发工作池。
仅保证具体 topic 兴趣/类型订阅内的接收顺序；跨类型、跨发布者及 group 不提供全局执行顺序。session FIFO 按实际准入顺序串行，不承担跨订阅排序。
handler 有有限等待期限，退出或超时会取消在途 future，已产生的副作用不回滚、不重试。
取消、退出或丢弃一个订阅只退订自身，不关闭共享连接；Bus::close 由连接持有者显式调用。
断线、发布入队失败或取消会保守停用连接；SDK 报告慢消费者时停止本连接上的所有消费任务并上报，发布仍可使用。需要消费故障隔离时使用独立 Bus。

回调收到 `Request<Input<...>>`，包含当前状态借用、强类型 Envelope 及执行上下文；路由主题可从 `request.input.event.topic` 读取，原始编码可用 `context.extensions().get::<Arc<Message>>()` 取得。转发直接共享原始 Message，不重新编码，也不重新采集追踪头。
`Outcome` 区分 Ignored、Handled、Rejected、Partial，表示当前 handler 阶段的处理结果；日志/指标另列错误与超时。
网关可把 deliver 的 Report 转为 Outcome，Handled 只表示完成本地准入，不表示后续 player 执行完成。
内部 Executor::execute_event 只返回可选 TimerUpdate，类型上不包含客户端回应；业务执行结果由会话处理，不返回 NATS 发布者。

发布成功仅表示客户端已接受发布操作，不保证 broker 接收或订阅者执行完成，断线仍可能丢失事件。无订阅者、队列满、离线及冻结窗口可能丢失事件，不自动重试或去重。
Core NATS 拒绝 Persistent，不静默降级；JetStream、离线加载和去重辅助仍待实现。
仅订阅初始化通过同连接临时 inbox 自回环确认订阅命令已被 broker 处理，需要相应发布与订阅权限；只确认当前连接，不保证其他节点已就绪。发布不创建探测订阅、不等待回环。async-nats 0.50 的 flush 只确认本地写出，不承担订阅就绪确认。
通用 protobuf 事件协议使用初始版本 1；尚未发布，不保留开发中间字段编号或维护其兼容。

### 游戏自定义网关处理

共同事件上下文和 gateway/session 处理链已接通；固定订阅可直接组装，动态主题通过 interest::Pool 与所选组件绑定，处理者使用相同 Registry。

[`events::Subscriber`] 将 Handler 与可选 group 组合为独立订阅组件；同一网关可安装多个组件，各自选择类型、消费组和处理行为。
[`gateway::player::events::forward`] 提供基本的玩家转发处理者，游戏从完整 Envelope 的 topic 或正文选择 Target，原始事件进入 session FIFO。
[`gateway::player::events::notify`] 提供广播通知处理者，游戏负责选择 Target 和通知正文，并指定客户端消息 ID。
这两个内置处理者返回 Registry，可交给 Subscriber 组装；自定义 Registry 或 Handler 使用同一抽象，没有固定的组件种类枚举。
示例将玩家转发与通知分别组装为两个组件；None 只是该示例本地筛选方式的选择，不是网关限制。

事件订阅由游戏服务工厂组装。取得 Gateway::tasks()，显式预留后台任务容量，再把 subscription.run(stop) 交给该运行时监督。
Ingress 不依赖 NATS，也不限制只能安装一个事件消费任务；可在 `serve` 前确认订阅并提交任务。发布/订阅连接的关闭由持有者负责。
用 ingress.sessions() 捕获会话目录并注册游戏处理者；普通服务则通过 `Init.service.tasks().launch` 提交同一个消费 future。
处理函数可执行自定义网关操作，也可创建 [`gateway::player::events::Delivery`] 交给 SessionManager::deliver：

- `Action::Notify { message_id, body }`：显式转换为客户端协议通知，不执行 player；成功准入后沿 server_seq 进入补发缓存。
- `Action::Forward(payload)`：服务器事件排入 session 原有 FIFO，与请求和 timer 串行；不产生客户端 Response。

此处 Target::Topic 是用于筛选本地会话的主题，例如 `guild:7`；NATS 按 Event::topic 和事件类型路由。通过 Pool 绑定后，成员主题驱动该组件的具体传输兴趣；业务仍显式决定如何映射投递目标。内部 `channel::Channel` 则管理会话通信与连接，三者职责独立。

`Target` 支持 Players、Topic、All，仅筛选本地在线且未冻结、未恢复的会话，未知或离线玩家不拉起。
这些目标和动作是网关本地 API，不是通用事件传输字段。通知正文可以和事件正文完全不同。
`deliver(...).await` 返回 admitted / skipped / rejected，本地准入不等于玩家执行完成。
主题使用本地反向索引，只遍历其成员；Players 按 ID 查找，All 遍历本地会话。每处理 64 个目标让出调度，不在跨 await 时持有目录、索引或会话锁。
通知正文在在线及成员检查通过后才复制。skipped 只计入已选目标中不满足准入条件的会话，不把非主题成员计入。
索引保存弱引用及完整会话身份，加入/退出与会话状态在同一锁下更新；关闭或析构清理，恢复保留，交接重建，旧代次清理不影响新代次。
取消分发 future 不回滚已准入目标；让出调度期间发生退出、关闭或冻结，后续目标会重新检查。

需要会话内定制处理时，注入自己的 [`gateway::player::Executor`]，实现 execute_event 并委托其他来源给 RpcExecutor。
事件仍先进入会话 FIFO，续租并检查所有权后执行；默认 RPC 适配调用 PlayerRpc.HandleEvent，返回独立 EventResponse（序号、错误、timer 更新），类型上没有客户端 payload。
[`message::rpc::HandlerRegistry::register_event`] 与普通请求、timer 表隔离；自定义 Handler 可实现 event，TowerHandler 适配携带共同 Request 的 Call 输入，事件路由通过同一事件 Registry 安装 Layer。
GatewayIngress、Handler、SessionManager 服务器 API、Executor 分别覆盖接入、网关事件编排、会话筛选和串行执行扩展。
游戏附加内存状态不会自动迁移，若需跨网关恢复，应另外设计快照与恢复规则。

事件完全由服务器驱动。客户端 Frame 没有事件入口；将事件编码塞进业务 Request 也不会进入事件表，伪造上行 Notify 被拒绝。
客户端请求不能选择内部事件名称、广播动作、目标或 timer 来源。游戏可以让明确注册且完成业务校验的请求主动发布事件。
内部 gRPC 和 NATS 的可信性依赖部署及认证，不能仅靠字段校验阻止持有内部凭据的调用者。

可信服务器通过异步 SessionHandle::join_topic / leave_topic 管理成员，默认每会话 64 个，最多 1024 个，主题名最多 256 字节；等待中的加入也占用有界操作容量。
同网关恢复保留，新登录重建，交接随版本 1 快照迁移；冻结期间拒绝修改，旧句柄不能影响新会话。
没有客户端主题管理协议。客户端 Frame 仍为版本 3。

示例显式配置连接，不改变 ServiceConfig：

```toml
[events]
url = "nats://127.0.0.1:4222"
namespace = "dev"
operation_timeout_ms = 2000
max_message_bytes = 1048576
subscription_capacity = 64
client_capacity = 128
max_subscriptions = 32
max_pending_subscriptions = 8
max_buffer_bytes = 134217728
handler_timeout_ms = 5000
```

`max_message_bytes` 包含事件正文与追踪头，总上限 1 MiB。每个活动的 topic/类型订阅按 `subscription_capacity × max_message_bytes` 预留缓冲预算，
总额不得超过 max_buffer_bytes；同名类型的多个 group 分别占用。数量或预算不足在初始化时拒绝，退订/丢弃后释放。
这是对配置范围内排队正文的保守预留，不是进程堆内存硬限额，不包含 SDK 元数据、在途解码和无效超大输入。
max_subscriptions 限制实际 topic/类型订阅数，max_pending_subscriptions 限制建立操作和探测 SID；client_capacity 控制命令缓冲与并发发布，handler_timeout_ms 约束当前 handler。
连接/发布的 operation_timeout_ms 与 handler 等待期限分别管理。
配置校验不连接 NATS，URL 凭据不进入 Debug 或框架错误。坏包拒绝并记录，队列容量不足快速拒绝，不积压重试。
W3C traceparent / tracestate 使用标准 NATS headers；发布、网关接收、session 排队和内部 RPC 保持关联，指标不使用玩家 ID 标签。

`just nats-test` 使用 TOK_TEST_NATS_URL（默认本机 4222）验证真实 NATS、独立网关、坏包、断线、过载、无回环发布和订阅就绪确认。
`just nats-smoke` 使用 events.toml 真实登录客户端，发布 AddValue 后由游戏网关订阅者转发给 player；发布 ValueChanged 后由订阅者转换为 ValueNotice。
示例通知 ID 在 proto 枚举中定义，内部 AddValue 没有客户端消息 ID；普通配置不启用 NATS，不依赖消息服务器。

### 动态兴趣与可信初始主题

`events::interest::Pool` 接收传输适配、组件模板和通用任务句柄，以组件名与具体 Topic 合并引用；每个组件保留自己的类型、group、身份和上下文。模板的主题在 acquire 时替换为 Exact，不先建立 All 订阅。同名组件中多个会话共享同一次建立与消费，不同组件不合并 handler。

Pool 不创建自己的运行循环；调用方必须持续驱动所传任务范围。初始化期获取固定兴趣用 `Init.service.tasks()`，该范围由服务 Builder 在初始化期间驱动；不要等待尚未 serve 的 Gateway::tasks。后台消费异常向监督者上报，不隐藏重试。

```rust,no_run
use std::sync::Arc;
use tok::{events::{self, Event, Topic, TopicFilter}, gateway::player::{Ingress, events::{Target, notify}}};

#[derive(Clone, PartialEq, prost::Message)]
struct ChatMessage {
    #[prost(uint64, tag="1")]
    channel_id: u64,
    #[prost(bytes, tag="2")]
    body: Vec<u8>,
}
impl Event for ChatMessage {
    const NAME: &'static str = "chat.message.v1";
    fn topic(&self) -> Result<Topic, events::Error> {
        Topic::named(format!("chat:{}", self.channel_id))
    }
}
# async fn assemble(init: &tok::lifecycle::Init, ingress: Ingress, bus: events::nats::Bus, chat_message_id: u32)
# -> Result<(Ingress, events::interest::Lease), Box<dyn std::error::Error + Send + Sync>> {
// 由可信游戏策略选择初始频道；生产可从已验证身份和状态计算。
let ingress = ingress.with_topic_policy(|_request| Ok(vec!["chat:7".into()]));
let sessions = ingress.sessions();
let capabilities = sessions.extensions();
let handlers = notify::<ChatMessage, _>(sessions, chat_message_id, |event| {
    let topic = event.topic.as_str().ok_or(events::Error::InvalidInput("聊天缺少 topic"))?;
    Ok((Target::Topic(topic.into()), event.payload.body.clone()))
})?;
let subscriber = events::Subscriber::new(init.identity.clone(), Arc::new(handlers),
    TopicFilter::Exact(Topic::default()), None)?.with_extensions(capabilities);
let pool = events::interest::Pool::new(Default::default(), Arc::new(bus),
    init.service.tasks(), [("chat.notify".into(), subscriber)])?;
// 固定兴趣是独立引用；即使没有会话成员也保留，服务退出时释放此 Lease。
let fixed = pool.acquire("chat.notify", Topic::named("chat:0")?).await?;
let ingress = ingress.with_topic_interests(pool, vec!["chat.notify".into()])?;
# Ok((ingress, fixed))
# }
```

固定兴趣和会话兴趣分别持有 Lease；最后一个引用释放后取消消费并有界清理，同一主题的后续获取等待旧订阅清理，避免重叠消费。Drop 撤销本引用，`release().await` 在自己是最后一个引用时等待退出。Pool 的 snapshot 可观察兴趣、引用和等待数；组件绑定和 Lease 都弱持有 Pool，避免经处理者、目录和会话成员形成引用环；Ingress 或服务组装层必须持有 Pool。

`join_topic(...).await` 等待所有绑定组件就绪，然后重验 session、连接代次、冻结及成员状态再激活。中途退出、关闭、重连或部分组件失败，只撤销本次引用，不删除其他成员或固定兴趣。`leave_topic(...).await` 先让本地成员失效，再有界释放引用；超时不恢复成员。没有绑定组件时，成员操作仍然有效，但不建立传输订阅。

新登录的 `with_topic_policy` 在已认证并取得会话身份后执行；返回有界主题列表，在开放登录前通过同一异步成员入口完成。策略必须短小且不阻塞；失败会关闭新会话并按原流程释放所有权，不返回成功登录。

当前选择保留整个逻辑会话生命期内的成员和兴趣：临时断线的宽限期不退群，冻结也不释放源端兴趣；离线/冻结时仍由会话准入拒绝即时事件。恢复前核对保留兴趣就绪，业务数据不补收。交接目标在提交前准备快照中的兴趣，提交前不登记目标会话；取消释放目标预留，提交后源端关闭释放引用。只迁移主题名，不迁移 SID、引用计数或 future，不承诺冻结窗口无丢失。按在线成员释放兴趣的可选优化尚未提供。

Pool 配置限制组件、活动/建立中/退出中的兴趣、等待操作及期限；公共任务范围限制排队与运行，不另设无界命令队列。NATS 另限制实际 topic/类型 SID、缓冲和同时建立操作；临时探测 SID 由 max_pending_subscriptions 限制，实际 SDK SID 上界为 max_subscriptions + max_pending_subscriptions。


## 统一 Gateway 与接入组件

Player/Scene 共用 Gateway 外壳，P2 已统一后台任务监督；P4 已接通作用域能力、扩展状态及业务处理链。

[`gateway::Gateway`] 共用连接接收、容量控制、后台任务监督与退出流程。
[`gateway::GatewayIngress`] 定义连接处理和清理钩子，当前
[`gateway::player::Ingress`] 实现玩家认证、所有权和串行 session。
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

[`application::Application::register_factory`] 按名称注册工厂；运行前检查配置实际使用的工厂是否已注册，
每个服务调用一次工厂，Init 提供服务身份、生命周期句柄及扩展；端点与发现信息位于 Binding 扩展。
[`config::ServiceConfig::factory_name`] 优先返回显式的 `factory`，省略时返回 `service_name`；端点协议不参与工厂选择。
demo-game 提供 `player-ingress`、`player` 与 `guild`；选择 `scene-ingress` 明确返回尚未实现。
Player 接入组件要求独占 TCP 端点，业务组件要求 gRPC 端点；具体约束由游戏组装层检查。

普通业务配置只需 `service_name` 和 `endpoint`，例如 player、guild 默认找到同名工厂；示例网关显式使用 `factory = "player-ingress"`。
直接构造 `ServiceConfig` 时填写 `factory: None` 或 `Some(名称)`；原 `gateway` 和 `component` 配置字段已移除。
初始化工厂使用 `register_factory` 按名称注册，同一工厂注册一次即可供多个服务使用。
RPC 端点仍显式绑定服务名；业务调用方、路由与发现查询使用实际的 `service_name`。

独立构造使用 `Gateway::new(identity, config, build)`；服务工厂中使用 `Gateway::new_in(config, &init, build)`。构造回调取得 [`gateway::Handle`]，可查询身份与能力；其 `tasks()` 取得共同任务句柄，提交受监督任务。`Gateway::tasks()` 仍直接返回任务句柄。
[`gateway::GatewayConfig`] 单独限制连接数和后台任务总数，任务总数包含等待启动的任务。
Player 保留 `[player_gateway]` 配置布局，`Config::gateway_limits` 从连接数和最大 session 数
生成网关容量；会话、队列、所有权与协议参数仍由 Player 组件使用。

普通连接错误只关闭该连接；连接 panic、后台任务错误或 panic 会使网关退出。
完整组装后调用一次组件 `initialize`，再由外层驱动 `serve`。退出时取消任务、调用组件 `stop`、收束后台任务，再调用异步 `finish`。
`stop` 必须可重复调用；组件收到取消后应停止新业务并完成清理，整体退出期限由外层 Application 或调用方控制。
运行 future 被强制丢弃时会取消并中止受监督任务、调用 `stop`，无法等待异步 `finish`。

玩家接入统一使用 `Gateway::player` 或 `Gateway::new` 与 `Ingress` 组装，
通过 `gateway.ingress().sessions()` 获取会话管理器。
玩家接入参数使用 `Config`，网关容量使用 `gateway::GatewayConfig`。
Scene 目前仅预留模块，不套用 Player 的串行与所有权语义。

## 网关与会话能力

[`gateway::Handle`] 提供网关身份、阶段、连接/后台容量、取消、停止请求与完整结束等待。其阶段为 Prepared、Serving、Stopping、Stopped，Stopped 包含组件 finish；`tasks().wait_finished()` 只观察任务范围。
[`gateway::player::SessionManager`] 提供 owner、gateway、snapshot、cancellation、request_stop 和 wait_finished；其 Open / Stopping / Stopped 记录玩家接入范围的准入与活动工作。停止管理器不强制关闭其他接入组件，网关 finish 的额外清理通过 gateway 句柄观察。
[`gateway::player::SessionHandle`] 提供身份、snapshot、extensions、关闭、取消及 wait_finished。会话阶段区分 Opening、Online、Detached、Resuming、Handoff、Stopping、Stopped；快照包含连接代次、主题、请求/下行位置及 FIFO 容量。快照不是后续操作的许可，框架在实际准入或发布时重新检查。

`SessionManager::extensions()` 返回当前管理器作用域的能力副本，包含 gateway 和 manager 句柄；工厂创建的服务生命周期句柄也随网关能力继承。
将它交给 `Subscriber::with_extensions` 后，网关事件回调与消息中间件可查询相同作用域能力。会话执行再增加当前 SessionHandle 和会话工厂的能力；远程 RPC 不传播这些本地对象。

## 玩家中间件与扩展状态

[`gateway::player::Pipeline`] 使用同一 Tower 协议，分别安装 `with_admission_layer`、`with_request_layer`、`with_event_layer`、`with_timer_layer`、`with_downlink_layer`。
准入与新下行使用 `Request<LogicalMessage>`；执行器使用 `Request<RequestInput>`、`Request<EventInput>`、`Request<TimerInput>`，不再使用无共同上下文的多参数调用。
每个入口保留自己的结果类型。事件只返回 timer 效果，不生成客户端 Response；timer 也不自动下发客户端回应。

```rust
use std::sync::Arc;
use tok::{player::LogicalMessage, gateway::player::{Executor, Pipeline, SessionHandle}, execution::Request, message::rpc::tower::util::MapRequestLayer};
fn pipeline(executor: Arc<dyn Executor>) -> Pipeline {
    let downlink = MapRequestLayer::new(|mut request: Request<LogicalMessage>| {
        // 此时尚未编号和编码；可查询当前会话并修改新消息正文。
        if let Some(session) = request.context.extensions().get::<SessionHandle>() {
            let _identity = session.identity();
            request.input.body.push(1);
        }
        request
    });
    Pipeline::new(executor).with_downlink_layer(downlink)
}
```

在 Gateway 构造回调内，通过 `Ingress::with_pipeline` 安装完整链；配置完成后再导出 manager/session 句柄。`with_gateway_rpc` 只声明绑定，实际注册在组件 initialize 时执行，避免链式配置顺序使 RPC 看见旧工厂；stop 按实例撤销注册。
准入 Layer 不能绕过认证、当前绑定、来源、序号及容量校验；拒绝也会消耗已收到的请求 ID。执行 Layer 在原 session FIFO 内、所有权检查之后运行，不改变单玩家串行约束。
准入预算从连接处理开始，执行预算从出队并完成所有权检查后开始；两者是独立阶段，FIFO 等待由容量约束。一个阶段内的 readiness、中间件与处理者共享同一期限，本地派生事件继续继承该期限。
顶号、关闭和停机停止新业务，在途执行按原预算收束；会话取消信号可通过 SessionHandle 观察，本次执行上下文的取消令牌用于本次调用，不因优雅停止立即中断已开始的业务。
准入及绑定回应使用连接代次的取消范围，同会话重连也会使旧绑定的异步准入失效。

新下行中间件在异步串行门锁内执行；通道状态锁不跨 await，完成后重新校验连接/主题等条件，再分配序号、编码和缓存。`SessionHandle::notify` 改为显式 await；异步处理期间退出主题不会产生新缓存或序号。
缓存补发直接重用原编码，不再运行 Layer。普通通知被中间件拒绝时返回错误，内部/会话关闭错误才自动关闭会话；回应发布失败则关闭会话，不重执原业务。

[`gateway::player::session::extension::Factory`] 创建每个逻辑会话的线程安全能力，并提供 initialize、snapshot、validate、restore 和 on_change。网关与管理器作用域的数据由服务工厂初始化，分别通过 Init.extensions 和 Ingress::with_extensions 安装。
扩展能力可包含游戏自定义句柄或状态；它们不授予任意改写内建状态的权限。业务内部借用 `&mut` 的本地执行仍使用统一事件注册表，不要求为了本地分发把状态放入共享锁。
生命周期通知覆盖初始化、绑定/解除绑定、恢复开始/完成、交接准备/取消/提交、目标恢复、关闭和执行结束，携带相应连接代次。回调同步、短小且在通道锁外执行；查询到的是当前状态，可能晚于通知描述的代次。回调 panic 会记录并关闭会话，不回滚内建迁移。

通过 `Ingress::with_session_extension` 显式安装状态及迁移适配。源端冻结并排空前序执行后采集扩展快照；目标端在提交前检查格式名称和 validate，提交后用新身份恢复。格式不匹配、校验失败或快照超限都不转移所有权；提交后恢复失败会关闭接管并尽力释放目标所有权，不重开旧端。
未安装自定义工厂时只接受显式空状态格式，不静默丢弃其他格式或缺失的扩展快照。交接快照仍为未发布的版本 1，客户端 Frame 仍为版本 3，不承诺开发中间版本混跑。
完整可运行实现见 demo 的 `gateway/middleware.rs`：同一个 Observe Layer 安装到五个入口，会话统计状态通过显式适配跨网关恢复。`just client-smoke nats-smoke handoff-smoke` 覆盖真实使用链路。

## 组件入口与当前边界

| 入口 | 用途 |
| --- | --- |
| [`config`] | TOML 加载、校验与端点引用 |
| [`application`] | 服务工厂、启动监督与退出 |
| [`core`] | 服务身份、Extensions 和发现/锁/路由领域契约及各自错误 |
| [`execution`] | 类型化请求与预算/取消执行 Layer |
| [`lifecycle`] | 公共工厂、任务监督和服务生命周期 |
| [`infra`] | 服务目录、路由及协调后端 |
| [`grpc`] | 内部 RPC 与 Tower 组装 |
| [`gateway`] | 统一 Gateway 运行框架与已实现的 Player 接入；Scene 尚未实现 |
| [`events`] | 统一异步事件、本地状态借用、Core NATS 即时投递及网关广播/转发 |
| [`observability`] | 日志、指标与追踪 |

目前示例监听共享内部 gRPC 和客户端 TCP 端点，已接通注入认证、有界串行 session、状态读写及服务组合。
Redis 开发发现、session 所有权、timer、ACK 缓存、同网关恢复、可选跨网关交接和 Core NATS 已接通；JetStream、离线加载及生产安全仍待实现。
配置端点地址本身不启动监听；内部 RPC 使用 register_rpc_factory / bind_rpc 或显式完整端点注册，TCP 由网关工厂绑定。
Kubernetes 配置可校验，运行该发现后端仍会明确报错。
游戏业务协议和 handler 由游戏定义，框架组件不反向依赖具体游戏。
