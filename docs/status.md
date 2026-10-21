# 当前实现状态

核对日期：2026-10-21。P6 已提交为 `06d9dd2`，P7 应用组装与接口清理已提交为 `017a9ea`。框架的“服务 + 处理链”方向校正已完成[七批计划](implementation-plan.md)本轮验收，历史结果见[实施历史](implementation-history.md)；本页只维护当前能力和边界。

## crate 与基础契约

玩家运行契约已独立为 [tok-player](../crates/tok-player/src/lib.rs)，分库提交为 `9f32c91`；游戏协议 CallContext 位于 [tok-message](../crates/tok-message/src/rpc/context.rs)。Context / StopReason 归 tok-execution，Extensions 独立为 tok_core::Extensions。core 删除 tokio-util 与 tracing 依赖，当前只依赖 serde / thiserror。

| core 范围 | 当前职责与接口 |
| --- | --- |
| service | Identity 表示无地址的服务身份，ServiceDescriptor 表示可发现的网络服务 |
| discovery | ServiceDirectory 读本地快照，ServiceRegistry 发布/撤销，DirectoryWatch 合并变更通知 |
| routing | ServiceRouter 根据服务名与业务 ID 选点，不负责发现写入或自动重试 |
| lock | Key / Lease / Backend 定义请求级租约，后端在 infra、中间件在 RPC |
| 根导出 | Extensions 是类型化能力集合，BoxFuture 是不隐含 Result 的 Send Future 别名 |

锁、所有权和发现统一使用 BoxFuture，并显式保留 core::Result 或 player::ownership::Result。原 Operation 与 RegistrationTask 别名移除；根导出的 ServiceDirectory 等角色名前缀有辨识用途，继续保留。DirectoryStateUnavailable 表达目录本地状态不可用，Unsupported 表达当前组件不支持能力；错误类别按 core 声明的契约定义，不收集 Redis、RPC 或玩家业务专有错误。

此轮上下文、锁模块及 Future 别名校正随本次提交交付。客户端与内部 protobuf、Redis 键、期限、取消、追踪及租约行为均保持原语义。此前验收记录保留在实施历史和计划；本次 BoxFuture 统一已通过 40 项 infra/RPC 常规测试、12 项指南测试、全目标 Clippy 和 rustdoc；未重复其他外部专项或完整工作区测试，按用户要求未做 release 构建。

## core 错误契约

Error 保留为通用基础契约的错误类型，不承担整个工程的错误全集。锁竞争等待、容量耗尽、能力不支持、注册冲突和本地目录状态分别使用 LockWaitTimeout / LockCapacityExceeded / Unsupported / RegistrationConflict / DirectoryStateUnavailable。InvalidInput / InvalidConfig / NoAvailableService / CoordinationUnavailable / DiscoveryUnavailable 保留明确用途，后端将具体故障映射到契约类别。

发现首次注册冲突、资源容量耗尽与租约失效分别报告 RegistrationConflict、RegistrationCapacityExceeded 和 DiscoveryUnavailable，容量或续租失败不再误报重复注册。各接口的主要错误类别见 core::Error 的 rustdoc 表；类型分类用于处理，错误文本用于诊断，不向客户端直接泄漏底层凭据。本次已完成验收，随本次提交交付：40 项 infra/RPC 常规测试、12 项指南测试、3 项真实 Redis 发现及 5 项真实 Redis 锁专项通过；最终 Clippy、rustdoc、格式检查通过。

## 公开入口规则落地

AGENTS.md 已记录有官方依据的 API 组织规则：功能命名空间公开、实现模块默认私有、主要入口可根重导出、允许双路径。core 保留四个功能模块，error 模块改为私有，错误分类表移到公开 Error 上；Identity 增加根导出，与 service::Identity 指向同一类型。已有角色名前缀及有使用价值的双路径保留。本次已完成验收，随本次提交交付：全目标 Clippy、rustdoc、12 项指南测试及生成文档的公开入口检查通过。未做 release 构建。

## 当前能力

| 范围 | 已实现与代码依据 | 边界 |
| --- | --- | --- |
| 应用组装 | [Application](../crates/tok/src/application.rs) 复用共同 Factory / Builder，配置驱动共享监听、发现、就绪与退出；[RPC 组装](../crates/tok/src/application/rpc.rs) 自动注入初始化绑定 | 自定义 protobuf 可逐服务绑定，或显式注册完整端点；同一端点不混用两种方式 |
| 无端点服务 | ServiceConfig.endpoint 可省略，纯事件/后台服务使用同一生命周期 | 不发布网络地址，不注入 application::Binding；需要发现查询时由组装层注入 |
| 执行与中间件 | [execution::Context](../crates/tok-execution/src/context.rs)、[Request / Tower Layer](../crates/tok-execution/src/lib.rs) 用于 RPC、事件、timer 与网关消息链 | 保留具体协议、输入/结果类型、可信入口校验和调度边界，无通用路由器/熔断器 |
| 服务生命周期 | Factory / Init 初始化，Builder → Prepared → Running 消费式编排；task::Supervisor 驱动有界工作 | 主任务、后台故障及退出期限由所有者监督，不创建隐藏的任务运行时 |
| RPC | [tok-grpc](../crates/tok-grpc/src/lib.rs) 提供连接、端点和通用执行；[tok-message](../crates/tok-message/src/lib.rs) 提供游戏消息、事件、timer 及可选请求锁 | RPC 工厂初始化必须安装处理链，未安装不发布；同一端点不能覆盖同名 protobuf 服务 |
| 事件 | [Registry](../crates/tok-events/src/registry.rs) 本地借用及远程解码共用处理表，有序多处理者、原始错误及 Arc 保留 | 本地 dispatch 显式等待；远程发布不等待业务结果，不提供事务或隐式去重 |
| topic / group | Event::topic 默认全局，可固定或由正文字段派生；[Pool](../crates/tok-events/src/interest.rs) 按组件/主题聚合固定与会话引用 | group 属于订阅组件；无组独立接收，同组分担不等于玩家归属路由 |
| NATS | [Bus](../crates/tok-events/src/nats.rs) 按 namespace、topic、类型安全路由，有限 SID、建立操作、缓冲与处理预算 | Core NATS 即时事件；发布只表示接受，订阅建立有限回环就绪；没有 JetStream |
| gateway / session | [Gateway](../crates/tok-gateway/src/server.rs) 作为共同设施的内置扩展，提供快照、状态工厂、生命周期观察及准入/执行/新下行 Layer | 通信、FIFO、timer、ACK、恢复与交接保留各自状态机；不强制普通服务具备 session |
| 动态成员 | 异步 join/leave 等待所选组件就绪，可信策略在登录前准备初始主题；目标交接提交前准备兴趣 | 宽限期与冻结期间保留兴趣；离线/恢复/冻结不投递即时事件，冻结窗口不补收 |
| 发现与协调 | Memory / Redis 发现、会话所有权、请求级锁；发现缓存和路由分离 | Memory 仅单进程；租约与超时是尽力保护，不保证严格 fencing 或恰好一次 |
| 工程与文档 | 十个框架 crate、demo、构建期 protobuf/消息 ID 生成、中文 rustdoc、workspace lint、LF、Windows/Linux CI | CI 配置不等于本地已执行 Linux 验证；真实外部服务与 release 结果分别记录 |

## 使用与失败边界

普通服务的构造仍是一套 Factory / Init，RPC、事件都通过 Request 和 Tower 执行。Application::register_rpc_factory 自动按配置创建入口，将 message::rpc::Binding 注入工厂；在工厂内初始化状态、路由和 Layer 后安装。初始化装配用的 message::rpc::Binding 不保存回自身执行扩展，避免引用环。Application::bind_rpc 用于自定义 protobuf 服务，监听继续由 Application 合并管理。

网关、manager、session 通过类型化能力句柄查询和操作，不公开内部锁。Gateway::handle 的结束等待包含 ingress.finish，tasks().wait_finished 仅代表任务范围；生命周期服务的等待还包含自身清理。已接纳的 session 任务按原预算收束，停止不等于副作用回滚。

客户端 Frame 为版本 3，没有服务器事件或内部 timer 入口；客户端不能以消息 ID、正文、topic 或中间件伪造来源。请求、timer 和转发事件经过同一 session FIFO，Notify 仅走下行。新下行 Layer 位于编号、编码和缓存之前，补发发送原字节，不重复执行 Layer。

恢复凭证使用本地 HMAC-SHA256 验签，验证不读 Redis。默认宽限期 30 秒、凭证有效期 1 小时；顶号通过会话标记和共享所有权协调。跨网关交接需要明确启用共享所有权、共用密钥和内部端点。扩展迁移必须提供有界快照、校验及恢复；提交后失败关闭目标，不重新开放旧所有者。

订阅的 subject 为 tok.events.<namespace>.<topic-token>.<event_type>，由适配器编码，业务不直接拼接。默认 topic 与具名 default 及 All 兴趣不同。固定引用与成员引用独立，弱持有 Pool，服务组装层拥有协调范围；并发取消只撤销本次引用。服务停止或消费故障不隐藏重试。

## 后续范围

本轮模型与组装收敛不等于所有游戏服务器能力都已完成。JetStream、离线玩家加载、全局去重、玩家归属扇出中继、通用路由器/熔断器、生产安全、Kubernetes、Redis Cluster、Scene 接入、热配置与存储层仍属后续设计。在线成员为零时释放兴趣是未提供的可选优化，当前按逻辑会话生命期保留；不承诺即时事件无丢失。

## P7 验证与复现

常规执行 `just verify docs`；应用组合用 `just application-smoke`；本地执行/生命周期/事件、客户端与 RPC 有独立 smoke。真实 NATS 用 `just nats-test nats-smoke`，Redis 发现、所有权、锁与多进程分别使用 redis-test、ownership-test、locks-test、redis-smoke、ownership-smoke、handoff-smoke。release 构建使用 `just build`。

工作区 249 项测试通过，包含 12 项指南示例及 2 项编译失败文档测试；全目标 Clippy、格式检查和 workspace rustdoc 通过。新增覆盖无端点服务生命周期、工厂漏装处理链、RPC 声明冲突与原子校验、同工厂多身份及初始化绑定引用环释放。

本轮真实 NATS 15 项、Redis 发现 2 项、所有权 5 项、请求锁 5 项及发现双进程/共享所有权/三进程交接 3 项，共 30 项外部专项最终通过。共享所有权多进程测试首轮在等待释放时发生 Redis 查询超时，单独复跑通过；未定位超时根因，不隐去首轮失败，也不因此增加生产重试或放宽超时。应用组装、execution、lifecycle、events、启动退出、RPC、客户端与 NATS 八个 smoke 通过。

release 示例构建通过；release 二进制对全部 8 份示例配置校验通过，RPC 与 TCP 客户端 smoke 通过。UTF-8/LF、仓库内 81 处 Markdown 链接及锚点、git diff 空白检查通过。CI 加入应用组合、Redis 请求锁及真实 NATS 专项；未在本地运行 Linux，不将 CI 配置视为已通过结果。

P1–P7 方向校正完成本轮验收，P7 随本次提交交付。后续功能仍按明确边界另行设计，当前不提供吞吐或延迟压测结论。

## 调用执行与生命周期 API 错误校正（2026-10-21）

保留调用执行、task 与 lifecycle 的职责边界及公开功能命名空间；Request 与同步 handler 移入私有实现模块，根导出不变。InitTask 更名 InitFuture，复用 core::BoxFuture；task::Task 保留监督任务语义。任务预留和提交改用 SubmitError，区分容量不足、停止、入口关闭及名称非法；任务与服务错误的 Task 变体改名 Join。执行 Error::Inner 通过标准 source 暴露原始错误，任务 panic 时完成报告保留原名称；修正通用任务范围、初始化取消及 256 字节名称限制的中文文本。调用方与 rustdoc 指南同步迁移，不保留旧名称兼容别名。本轮不做 release 构建。

本轮 Windows 验收：工作区常规测试（含 12 项指南示例、2 项编译失败文档测试）、全目标 Clippy（警告视为错误）和 workspace rustdoc（警告视为错误）通过；新增标准错误链、提交失败容量释放及多任务 panic 名称保留回归测试。真实 Redis/NATS 专项未运行；本轮随本次提交交付。

## RPC 协议适配分层校正（2026-10-21）

通用 tok-grpc 保留 Client / Listener / Server，通过泛型请求和 Tower 接入任意生成的 gRPC 服务；正常依赖移除 tok-player / tok-events / tok-proto。新增 tok-message 承载游戏 CallContext、Handler / HandlerRegistry、玩家与普通业务协议客户端、绑定槽、通知编码及 timer / 事件 / 请求锁适配。门面从 message::rpc 访问游戏协议，grpc 访问 gRPC 设施；移除通用 CallHandler 上的事件契约，不提供旧路径兼容别名。Application 的普通自定义 protobuf 绑定继续使用通用端点，游戏 register_rpc_factory 使用新协议适配。客户端 Frame、内部 protobuf、事件 topic、timer、租约和无自动重试语义保持原有约束；事件与 timer 入口仍仅供可信服务端。

本轮 Windows 验收：工作区 255 项测试通过（包含 12 项指南示例和 2 项编译失败文档测试），31 项外部专项跳过；通用 RPC 新增 3 项无游戏类型执行测试，游戏 RPC 的事件入口隔离、身份、timer、共享连接、追踪及请求锁专项通过。全目标 Clippy、workspace rustdoc、application / lifecycle smoke、格式、UTF-8/LF、内部文档链接及空白检查通过。cargo tree 核对通用 RPC 的正常依赖不含玩家、事件或游戏 protobuf。不做 release 构建；未运行真实 Redis/NATS 专项或 Linux，本轮随本次提交交付。

## 调用执行与生命周期分库（2026-10-21）

原 tok-service 拆为 tok-execution 与 tok-lifecycle。tok-execution 保留 Context / StopReason、Request、Tower Layer / Service、Call 和 SyncHandler；tok-lifecycle 根导出 Factory / Init / InitFuture、Builder / Prepared / Running、Handle / Phase / Error，task 模块保留 Supervisor / Handle / Reservation / Task 等通用监督能力。两库共同依赖 tok-core，彼此无依赖；tok-lifecycle 不依赖 Tower、RPC、事件或玩家协议。门面路径改为 tok::execution / tok::lifecycle，不提供旧 tok-service 或 tok::service 兼容路径。

应用、RPC、事件、网关、示例及指南使用新路径；服务启动就绪、任务容量、预算取消、退出等待及线协议语义不变。本轮不做 release 构建。

本轮 Windows 验收：工作区 255 项测试通过，31 项外部专项按约定跳过，包含 12 项指南示例及 2 项消费式生命周期编译失败文档测试；全目标 Clippy、workspace rustdoc、lifecycle / execution smoke、格式、200 个文本文件 UTF-8/LF 及 82 处内部文档链接检查通过。依赖树确认两库无相互依赖、tok-lifecycle 无 Tower 依赖。未运行真实 Redis/NATS 专项、Linux 或 release 构建；本轮随本次提交交付。

## 执行上下文移除固定来源（2026-10-21）

Context 移除 source 字段、Source 枚举和构造/child 的来源参数。执行 Context 只管理服务、操作、期限、取消与追踪；类型化扩展位于 Request / Response。RPC、事件与网关在入口提供类型化扩展：复用游戏 CallContext、EventId / Topic / Arc<Message>、TimerTrigger，仅网关新增无正文的 ClientRequest 关联标识。RPC 关联信息与事件元信息可以共存，不再将传输方式、触发原因和客户端入口混成互斥枚举。

事件 Registry 的本地与远端分发提供相同事件头，派生子上下文覆盖当前事件头，不修改父扩展；本地派生仍清除继承的原始编码。客户端无法发布事件、请求与 timer 分表、会话身份校验、预算和取消约束保持不变；Extensions 元信息不作为认证证明。调用方和指南已迁移，不保留 Source 兼容类型。

本轮 Windows 验收：工作区 255 项测试通过、31 项外部专项跳过；回归覆盖 RPC 与事件元信息共存、当前事件头覆盖继承值但不修改父上下文、原始消息保留和客户端请求标识。全目标 Clippy、workspace rustdoc、execution / application smoke、格式、UTF-8/LF 与内部链接检查通过。未做 release 构建，未运行真实 Redis/NATS 或 Linux；本轮随本次提交交付。

## 玩家计时触发契约归属校正（2026-10-21）

TimerTrigger 从 tok-core 移至 tok-player，与 TimerSpec / TimerUpdate 聚合，门面路径为 tok::player::TimerTrigger。core 移除 timer 模块及根导出，不保留兼容别名；网关调度、玩家执行与游戏 RPC 统一消费新路径。通用执行测试和示例使用本地扩展类型，不为计时元信息依赖玩家库。内部 protobuf 与计时调度、预算及取消语义不变。本轮不做 release 构建。

本轮 Windows 验收：受影响的 core / execution / player / game-rpc / gateway 共 132 项测试通过、2 项外部专项跳过；demo 生产测试 2 项、指南示例 12 项通过。全目标编译与 Clippy、workspace rustdoc、execution smoke、格式、199 个文本文件 UTF-8/LF 与内部文档链接检查通过。未重复无关外部专项或完整工作区测试；未做 release 构建或 Linux 验证，本轮随本次提交交付。

## 通用 RPC 客户端命名（2026-10-21）

RpcClients 更名为 tok_grpc::Client，门面为 tok::grpc::Client，游戏协议层同步重导出 Client。玩家与普通业务协议适配器仍为各自模块的 Client，通过 Arc<tok_grpc::Client> 共享通用调用设施，不保留旧名称别名。连接缓存、期限、追踪、指标和不自动重试的执行行为不变。

本轮 Client 更名验收：RPC 两库 27 项常规测试与 12 项指南示例通过，全目标编译、Clippy、workspace rustdoc、格式、UTF-8/LF 和空白检查通过。本轮随本次提交交付。

同期审查发现的问题已在后续修复中处理：Client / Listener / Readiness 统一公开名称；客户端与服务端共用 grpc-timeout 解析，请求期限只缩短默认值，准备与发送计入同一截止点，迟到结果按超时返回。客户端配置期限限制为大于零且不超过一天，防止 tonic 编码过大超时值时 panic。Readiness::set 返回是否命中服务；无效名称、业务重复绑定及 protobuf 重复绑定分别报告原因。传输与监听错误保留 source 并增加中文操作说明；channel 文档明确延迟建连。内部模块继续私有，仅新增共享超时解析模块，不扩大公共 API 或引入业务错误码。

本轮修复 Windows 验收：RPC、游戏协议、门面与网关共 171 项测试通过（含 12 项指南示例），2 项外部专项按约定跳过；全目标编译与 Clippy、workspace rustdoc、rpc-smoke / client-smoke、格式、202 个文本文件 UTF-8/LF 和空白检查通过。未做 release 构建，未运行真实 Redis/NATS 或 Linux；本轮随本次提交交付。

监听接口 bind / serve 直接返回 `Result<BoxFuture<'static, Result<(), Error>>, Error>`，复用 tok_core::BoxFuture，不提供 ServerTask 别名或重导出。返回的 Future 不创建任务，须由调用方驱动和监督；外层错误表示绑定或组装失败，内层错误表示运行失败。

## RPC 传输配置扩展（2026-10-21）

Client::with_endpoint_factory 接收从 ServiceDescriptor 创建 Tonic Endpoint 的同步工厂；默认保持明文 URI 和默认连接期限。自定义工厂可设置 HTTPS/TLS、连接期限和 HTTP/2 参数，execute 的整体预算仍生效。工厂在缓存锁外执行，并发未命中允许重复构造；重新获取锁后优先复用已发布 Channel。节点和地址是连接共享边界，同一边界必须使用一致的 URI 和传输策略；替换工厂清空本实例缓存，不影响已有克隆。

服务端 Listener::from_server 接收配置好的 Tonic Server，首次路由绑定保留其 TLS/HTTP2 等参数；new/default 仍使用默认 Server。接口接受包含 Tower Layer 的 Server，监听级中间件覆盖完整路由，业务中间件也可包装具体服务。服务绑定、就绪开关、监听 Future 与 Application::register_rpc_endpoint 的监督流程不变。TLS 由应用启用 Tonic 的 TLS feature 并提供证书，不新增框架证书模型、服务发现密钥或配置文件开关。

本轮 Windows 验收：RPC 两库与门面共 78 项测试通过（含 13 项指南示例）；全目标编译与 Clippy、workspace rustdoc、rpc-smoke / client-smoke、格式、202 个文本文件 UTF-8/LF 和空白检查通过。回归覆盖自定义 URI 实际使用、传入 Server 超时生效、缓存复用/撤销、替换工厂清理及工厂错误反馈。未做真实 TLS 握手、外部 Redis/NATS、Linux 或 release 验证；随本次提交交付。

## RPC 服务端监听命名（2026-10-21）

服务端 tok_grpc::Endpoint 更名为 tok_grpc::Listener，游戏协议层同步重导出 Listener，内部实现文件改为 listener.rs，不保留旧名称别名。Listener 负责服务绑定、就绪控制与共享监听；Tonic 的客户端 transport::Endpoint 保留原名，继续由 Client 的工厂创建。应用配置中的 endpoint 仍表示网络端点，register_rpc_endpoint 等配置组装接口保持名称。传输配置、路由和执行语义不变。

本轮命名调整验收：RPC 两库与门面共 78 项测试通过（含 13 项指南示例），全目标编译、Clippy、workspace rustdoc、格式、UTF-8/LF 和空白检查通过。未做 release 构建；随本次提交交付。

内部接口补充校正：grpc-timeout 解析函数 request_budget 收紧为 pub(super)；OTel metadata 适配器明确命名为 MetadataExtractor / MetadataInjector。相关全目标编译与格式检查通过，随本次提交交付。

## 游戏处理与 RPC 适配分离：第一批（2026-10-21）

分发能力合并入 tok-execution::registry，不新增独立 crate 或 tok::dispatch 门面路径。Registry<K, T, R, E> 只负责按键选择并执行处理链，输入/结果/业务错误均泛型，不限定消息 ID、正文或编解码。K 采用 Ord + Clone 约束，不要求 Display；零或空键的合法性由业务/协议决定。

正文 protobuf 适配位于 tok-protobuf，复用通用表，提供 Registry<K, C, O, E> / Message<K> / Reply / Error，编解码与内层就绪等待也计入原预算。tok-execution 不依赖 prost、Tonic、生成协议、玩家或生命周期。message::rpc::HandlerRegistry 的普通请求表委托该适配器；business::Handler 显式转换业务状态与错误，仅在 RPC 边界构造 CallResponse / Status，并保留线协议 u32 非零规则。

公会示例用自身错误与 u64 目标，本地及 RPC 共用相同处理链；just dispatch-smoke 演示本地直接调用。当前存储的处理链与输入要求自有 Send + static 类型，共享业务状态可以 Arc 传入；借用执行继续使用直接 Tower 或事件接口。

后续第二批：校正玩家请求、timer、事件组合及结果转换，迁移通知构造。第三批已以显式业务取键泛化请求锁，迁入 tok-execution::lock，RPC 只保留协议适配；第二批玩家组合与通知归属仍未完成。现有玩家入口隔离、事件仅服务端驱动、原预算与无自动重试约束保留。

本次 Windows 验收：工作区 271 项测试通过、31 项外部专项按约定跳过，包含 15 项指南示例及生命周期编译失败文档测试。全目标编译与 Clippy、workspace rustdoc、dispatch / rpc / client / application smoke、格式、208 个文本文件 UTF-8/LF 与 82 处文档相对路径检查通过。元数据与完整正常依赖树确认无独立 tok-dispatch，tok-execution 不依赖 prost、Tonic、生成协议、玩家或生命周期。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。

注册器模块聚合校正：移除 dispatch.rs 与 dispatch/registry.rs 包装层，直接公开 tok-execution::registry，在 registry.rs 中聚合实现、Error 和 DispatchError；Registry 同时从 tok-execution 根导出。protobuf 适配继续位于 tok-message，分发键及执行约束不变。调用方和 rustdoc 使用新路径，不保留旧 dispatch 模块。本轮模块调整验收：execution / game-rpc / tok 共 90 项测试通过（含 15 项指南示例），全目标 Clippy、workspace rustdoc、格式、207 个文本文件 UTF-8/LF 和空白检查通过；随本次提交交付。

## 按功能域拆分基础错误（2026-10-21）

服务发现、请求锁和路由契约分别使用 core::discovery::{Error, Result}、core::lock::{Error, Result}、core::routing::{Error, Result}；不提供旧领域变体兼容别名。路由的 Discovery 包装保留原发现错误与错误链，Application::Error 增加 Discovery 聚合，注册、清理和故障传播均使用该类别。后端错误按相应领域映射，不返回其他领域的变体。

core 根 Error / Result 已移除，参数、配置及可选能力校验由实际所属组件表达。所有权故障归 player::ownership::Error::Unavailable；Redis 发现与锁校验使用各自领域错误，整体应用配置映射为 config::Error::Invalid。最终归属与验收见下文。

registry 注册错误定名为 Error<K>，运行失败定名为 DispatchError<K, E>；后者覆盖未知键与业务处理错误，执行停止统一由 execution::Error::Stopped 表达，避免 HandleError 误指只有处理者内部失败。protobuf 适配与示例同步新路径，不保留 RegistrationError 旧类型。

本轮 Windows 验收：工作区 272 项测试通过，31 项外部专项按约定跳过；全目标编译与 Clippy、workspace rustdoc、dispatch / rpc / client / application smoke、格式、207 个文本文件 UTF-8/LF 和 82 处相对文档路径检查通过。回归覆盖发现错误跨路由传播并保留 source、默认订阅错误的领域归属，以及现有锁状态映射和应用退出清理。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。

## 移除 core 根校验错误（2026-10-21）

删除 tok-core/error.rs 及根 Error / Result，领域发现、锁和路由契约不变，不提供兼容类型。剩余校验能力回到所属模块：execution::ContextError 表达无效服务身份、操作名和不可表示期限；lifecycle::task::CreateError 表达无效容量和父范围退出，lifecycle::CreateError 表达服务身份及底层任务范围构造失败。构造失败与运行错误分开，组合处保留原始来源。

应用配置使用 tok::config::{Error, Result}，Application 以 Configuration / Create / Discovery 聚合配置、构造和发现错误，Unsupported 表达应用尚未实现的可选能力。所有权和凭证错误分别归 player::ownership::Error 与 player::credentials::Error，不再经 Core 包装；玩家接入运行参数单独使用 gateway::player::ConfigError。示例业务状态构造采用自己的标准 I/O 参数错误，不依赖框架根错误。

本轮 Windows 验收：工作区 273 项测试通过、31 项外部专项按约定跳过，包含 15 项指南示例。全目标编译与 Clippy、workspace rustdoc、dispatch / rpc / client / application / lifecycle / execution smoke、格式、206 个文本文件 UTF-8/LF 和 82 处文档相对路径检查通过。源码扫描确认无 core::Error、core::Result 或 Error::Core 遗留，core/error.rs 已删除；回归确认停止父范围的子范围创建失败不泄漏活动记录。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。

## 消息与传输边界校正（2026-10-21）

tok-game-rpc 已调整为 tok-message，通用正文适配独立为 tok-protobuf，tok-rpc 更名为 tok-grpc。生成协议仍在 tok-proto，不依赖执行层。门面提供 tok::protobuf、tok::message 与 tok::grpc；旧 crate 与门面路径不再提供。

tok-message 根 Registry 提供不依赖 Tonic 的非零编号注册、本地分发与原始业务错误；玩家身份、事件/timer RPC 组合、线协议结果与 Status 映射集中于 message::rpc。公会示例和本地/RPC 共用处理链采用根 Registry。通用 protobuf 表继续允许字符串及零键，RPC 不自动添加锁或重试，事件与 timer 仍只能由可信服务器触发。

本轮只校正库与命名空间边界，不宣称通用请求锁泛化、玩家结果模型及通知构造后续批次已经完成。请求锁执行已迁入 execution::lock，message::rpc::lock 只保留协议策略与映射，仍按有限 TTL 和执行预算约束运行。

本轮 Windows 验收：工作区 274 项测试通过、31 项外部专项按约定跳过，包含 15 项指南示例；零消息编号的 RPC 拒绝另补定向回归。全目标编译与 Clippy、workspace rustdoc、dispatch / rpc / client / application / lifecycle / execution 六项 smoke、格式、214 个文本文件 UTF-8/LF 和 82 处相对文档路径检查通过。正常依赖确认 tok-protobuf 不依赖 Tonic、生成协议或玩家，tok-grpc 不依赖消息、事件或玩家层。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。

## 玩家契约审查修复（2026-10-21）

已移除 tok-player 根 Error / Result 和重复的 ownership::Owner。ownership::{Error, Result} 与 credentials::{Error, Result} 分别表达领域故障；持有者直接使用 core::Identity。Memory 与 Redis 构造配置失败统一为 InvalidConfig，内存所有权容量不足独立为 CapacityExceeded，网关映射为已有容量错误码；凭证故障保留不含敏感值的静态原因。

普通登录、快速重登、转移与释放对最新登录标记的要求已写入 trait 和指南；本地验签不授予所有权。模块与主要类型根导出布局保留，LogicalMessage / TimerSpec 注释、调用方、后端、自定义 Store 示例和指南同步修正。请求锁泛化已完成，后续玩家结果模型及通知归属仍按原计划推进。

本轮 Windows 验收：工作区 279 项测试通过、31 项外部专项按约定跳过，包含 16 项指南示例；最终内存所有权定向回归通过。全目标 Clippy、workspace rustdoc、client / execution smoke、格式、213 个文本文件 UTF-8/LF、82 处相对文档路径及空白检查通过。新增回归覆盖构造配置分类、最新登录标记回收、凭证故障的安全诊断和所有权容量不足的 TCP 登录错误映射。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。

## 通用请求锁迁移（2026-10-21）

请求锁执行已从 message::rpc::lock 提取为 execution::lock::{Config, Layer, Service, Error}。通用中间件显式接收取键与结果成功判断函数，输入、回应和业务错误保持泛型；不依赖 CallContext、生成协议、Tonic 或玩家。Config 只包含时间预算，以 Default 构造，不再携带 namespace。

RPC 的 lock::Layer::new(backend, namespace, config) 只校验固定服务与 bid、判断 CallResponse 业务结果并映射 Status。公会组装及原 RPC 回归继续使用该薄适配；指南新增无网络的类型化请求锁示例。通知构造和玩家结果模型校正仍未完成，不计入本轮范围。

本轮 Windows 验收：工作区 285 项测试通过、31 项外部专项按约定跳过，包含 17 项指南示例；迁移后 10 项 RPC 请求锁回归再次通过。新增 5 项纯类型化请求锁测试覆盖动态取键、上下文与业务错误保留、获取阶段原预算/取消、释放失败策略及非协作式释放迟到结果。全目标 Clippy、workspace rustdoc、rpc / client smoke、格式、215 个文本文件 UTF-8/LF、82 处相对文档路径及空白检查通过。正常依赖核对确认 tok-execution 未引入 Tonic、protobuf、玩家或事件；未运行真实 Redis/NATS、Linux 或 release 构建，随本次提交交付。

## gRPC Tower 扩展入口（2026-10-21）

新增 Client::execute_service 与 PreparedCall，为自定义协议提供类型化出站 Tower 处理链入口。execute 保留 FnOnce 回调，两个入口共用预算、追踪、连接和指标实现。就绪等待计入原期限，过期或迟到就绪不再分发；发送器用 PreparedCall::into_parts 检查并扣减远端预算，保留更短 metadata 期限。内置协议客户端继续使用原便捷入口，不宣称自动配置了出站中间件。

Listener::from_server 已支持带 Tower Layer 的 Tonic Server。内部改为 Routes 与类型擦除的监听工厂，对外 Listener 和 Application 注册无需泛型变更；共享服务就绪控制、绑定校验和传输参数保持原约束。指南展示显式出站链和监听级 Layer；不新增自动重试、路由、熔断默认策略或并行执行模型。

本轮 Windows 验收：工作区 292 项测试通过、31 项外部专项按约定跳过，包含 18 项指南示例。新增回归覆盖出站 metadata/借用输入、就绪等待和迟到就绪、发送前预算扣减、原错误与无主动重试、共享路由监听 Layer、就绪切换及 Application 非泛型注册；追踪回归同时覆盖便捷回调和 Tower 网络路径。全目标编译与 Clippy、workspace rustdoc、rpc / client / application smoke、格式、216 个文本文件 UTF-8/LF、82 处相对文档路径及空白检查通过。未运行真实 Redis/NATS、Linux 或 release 构建；随本次提交交付。


## gRPC 执行标准 Layer 化（2026-10-21）

已提供 client::{Call, Layer, Service, ResponseFuture} 与 server::{Layer, Service, ResponseFuture} 功能命名空间，Client / Server 保留主要根入口。客户端 Layer 将准备前的 Call 转换为 PreparedCall，直接组合时保留完整回应；execute_service 与 FnOnce execute 复用相同执行 Future，仍允许非 Clone 单次 Service 与借用。服务端 Layer 复用 execution 约束，Server::execute 仅作便捷调用，metadata 转换保留在 Server::context。

客户端预算从 Layer 的 call 开始，覆盖连接准备、内层就绪和发送；外层策略可提前拒绝或选点，其等待须独立约束。可重复 Service 克隆内层，共享策略状态须显式共享；指南使用标准 Layer / ServiceBuilder，说明预算放置与共享规则。Client 克隆共享工厂与缓存，invalidate 影响共享克隆，替换工厂建立独立配置。准备失败也计入 Layer 的调用指标，不主动增加重试或后台任务。

本轮 Windows 验收：工作区 298 项测试通过、31 项外部专项按约定跳过，包含 19 项指南示例。新增回归覆盖直接 Layer 的借用输入与完整回应、外层提前拒绝、共享客户端缓存与工厂隔离、内层并发就绪预算、服务身份与取消及非 Clone FnOnce 回调；真实网络追踪回归切换到直接 Layer 路径并通过。全目标 Clippy、workspace rustdoc、rpc / client / application smoke、格式、220 个文本文件 UTF-8/LF、82 处相对文档路径及空白检查通过。未运行真实 Redis/NATS、Linux 或 release 构建；本轮代码和文档尚未提交。


## 执行上下文连续性审查（2026-10-21）

以下记录修复前的审查依据，保留问题定位；六类边界现已校正，结果见本文件末尾“执行上下文连续性修复”。审查基于当时工作树中的 client/server Tower Layer，不能将表中修复前状态当作当前能力。契约见[服务运行模型](service-runtime.md#执行上下文连续性规范)，待实施顺序见[实施计划](implementation-plan.md#执行上下文连续性校正)。

### 审查时需要调整的边界（修复前）

| 优先顺序 | 修复前代码与事实 | 契约差异及调整方向 |
| --- | --- | --- |
| 1 | [gRPC 客户端输入](../crates/tok-grpc/src/client/call.rs)、[执行 Layer](../crates/tok-grpc/src/client/middleware.rs)、[PreparedCall](../crates/tok-grpc/src/client/call.rs) 和 [便捷入口](../crates/tok-grpc/src/client.rs)：Call 只带目标、序号、请求和 Span，Layer 根据 Client timeout / metadata 新建本地截止点，PreparedCall 没有 execution Context | 在本地出站处理链承接 Request<T> / Context，保留适用扩展和取消，并以已有截止点和客户端配置共同限制预算。保留目标独立于 Context.service；根调用须显式建立范围，便捷入口不能要求调用方拆散后再拼接上游 Context。现有外层 execution Layer 可约束本地结果，但不能让被丢弃的扩展重新可见，也不能保证远端取得上游剩余预算 |
| 2 | [游戏 RPC 编码](../crates/tok-message/src/rpc/call.rs)、[普通业务客户端](../crates/tok-message/src/rpc/business.rs)、[玩家客户端](../crates/tok-message/src/rpc/player.rs)：只接收协议 CallContext / payload，自建发送 Span；[RpcExecutor](../crates/tok-gateway/src/player/executor.rs) 取 request.input 后不传 request.context | 游戏客户端承接执行请求，在编码正文时保留本地 Context，并交给通用 gRPC 出站适配。CallContext 继续表达远端目标、玩家与协议关联，不代替执行 Context。请求、事件、timer 三条 RpcExecutor 路径均需迁移，避免只修普通 call |
| 3 | [玩家准入链](../crates/tok-gateway/src/player/pipeline.rs) 的 admission 输出仅 LogicalMessage；[BoundSession::submit](../crates/tok-gateway/src/player/session/handle.rs) 准入后只将正文入队；[RequestJob / worker](../crates/tok-gateway/src/player/session/worker.rs) 出队后重建执行 Context | 准入中写入的自定义扩展无法进入请求执行链；队列没有携带截止点，执行预算从出队后重新开始。需要保留执行承载并明确准入与已接受工作的范围边界；保留断线后在途业务收束的既有语义，不能直接将连接取消令牌替换为所有阶段共同取消令牌。即使声明准入与执行为独立范围，也须明确扩展转移及预算起点，不能静默丢弃 |
| 4 | [事件转发/通知组件](../crates/tok-gateway/src/player/events.rs) 调 deliver 时只传 Delivery；[SessionManager::deliver](../crates/tok-gateway/src/player/session/events.rs) 不接收 Context；EventJob 只保留事件与当前 Span；[下行入口](../crates/tok-gateway/src/player/session/handle.rs) 每次新建 Context | 事件中间件写入的本地扩展与消费期限无法接续到投递/下行；异步玩家事件执行只继承部分追踪。应将投递与下行适配接入共同执行承载，区分当前订阅者等待的准入范围与成功入队后的独立 session 工作。跨 session fan-out 由目标安装自身能力并显式筛选源元信息，不复制源 SessionHandle；补发仍不重复执行新下行中间件 |
| 5 | [GatewayRpcService](../crates/tok-gateway/src/rpc.rs) 四个交接方法直接 into_inner，再调用 SessionManager；[交接协调](../crates/tok-gateway/src/player/session/handoff.rs) 直接使用生成的 GatewayRpcClient 与 timeout_at | 内部交接服务未通过 Server::context / 共同执行链，接收端丢弃 metadata，出站与接收入口缺少一致的本地能力、追踪和预算适配。现有 budget_ms、handoff deadline、租约和补偿状态机必须保留；接入 Context 后合并限制，不能将已有取消/完成补偿简单绑定到已失效的原调用 |
| 6 | [NATS 发布](../crates/tok-events/src/nats.rs) publish / publish_event 接收 Message / Envelope，调用方通过正文追踪载体传递信息，没有接收执行 Context 的框架发布适配 | 原生传输出口可保留；需要为当前处理链内的发布提供显式、保持 Context 的适配，约束本地发布等待和追踪提取。不能因此将 Extensions 序列化，或让独立订阅消费继承发布者的短期调用期限；Core NATS 发布成功仍不是 broker / 订阅者确认 |

### 已符合或不应机械迁移的部分

- [Context::child](../crates/tok-execution/src/context.rs) 保留当前服务、原截止点、父子取消关系与 Span；输入扩展由 Request 转换与显式拆装保留；[通用分发表](../crates/tok-execution/src/registry.rs)、[protobuf 适配](../crates/tok-protobuf/src/lib.rs)、[游戏处理适配](../crates/tok-message/src/rpc/handler.rs) 转换输入时移交原 context。无需因文件拆分重建另一套上下文。
- [本地事件 Registry](../crates/tok-events/src/registry.rs) 从父范围派生事件操作，保留自定义扩展，显式覆盖事件头并清除不适用于新事件的原编码；这是有意的扩展作用域处理，不是任意丢失。
- [普通 RPC 接收端](../crates/tok-message/src/rpc/service.rs) 经 Server::request 重建本地范围，注入本地请求能力和可信协议关联，再进入共同执行链。远端重建本身符合规范，不要求恢复源进程的 Extensions。
- [NATS 消费](../crates/tok-events/src/nats.rs) 使用订阅服务身份、消费预算、停止信号与 [Subscriber 本地扩展](../crates/tok-events/src/subscriber.rs) 建立独立执行范围，随后提交 Request。无需复制发布者的本地能力或执行期限。
- timer 触发、独立生命周期通知和新登录初始化建立新范围有合理职责；应明确入口语义，而不是把所有 Context::new 都替换为 child。SessionManager / SessionHandle 的目标本地能力注入应保留。
- Extensions、生命周期初始化容器、发现/路由/锁/所有权后端、编解码和底层 Channel 不是业务处理链承载，无需机械增加 Request 参数。scene 仍是预留实现，不计为已完成的执行适配。

本次审查的判断是：共同处理核心已有承载能力，主要缺口在适配、排队与独立范围的衔接。不能以“最外层已有超时”认定内部 Context 已连续，也不应为统一而传播全部源能力、改写交接状态机或增加事件确认与重试语义。


## 执行上下文连续性修复（2026-10-21）

前述六类边界均已迁移：通用 gRPC、游戏协议客户端与 RpcExecutor、准入/FIFO、事件投递及下行、四个交接 RPC、NATS 发布执行适配。Request::map 保留上下文，所有消息/protobuf 注册便捷回调也改为接收 Request；不提供丢弃 Context 的旧签名兼容。真实调用方、指南和 smoke 示例已迁移。

客户端保留本地执行身份、扩展、追踪、取消和原截止点，Client 与 grpc-timeout 仅收紧预算；网络对端重建自身范围。准入返回 Request，FIFO 携带原截止点与标记，过期工作不进入业务链；连接停止关系在成功准入后显式转交，不改变在途收束。同步通知保留消费范围，已入队事件独立；目标已知能力覆盖源能力，自定义跨范围能力由游戏筛选。回应发布与交接取消补偿采用明确的独立有限范围，原业务不重试。

交接入口处理 metadata，快照和恢复回调接续相应执行范围；源端监督状态机继续兜底。Bus::publisher / publish_request 保持本地调用范围，NATS 消费独立安装订阅能力，不传播 Extensions 或发布者执行期限。原生传输出口与独立提交入口在 rustdoc 中说明范围边界，不将所有基础设施方法机械包装成 Request。

本轮 Windows 验收：工作区串行 306 项测试通过、32 项外部专项按默认规则跳过，包含 19 项指南示例；另在隔离临时 NATS 实例运行 16 项真实 NATS/多网关专项全部通过，发布 Future 的跨任务回归再次通过。最终期限收紧变更后，execution / grpc / gateway 共 143 项定向回归通过，准入边界最终 13 项再次通过。全目标编译与 Clippy、workspace rustdoc、rpc / client / application / events / dispatch 五个 smoke、格式、220 个文本文件 UTF-8/LF、119 处相对文档路径及空白检查通过。并行工作区回归曾遇到 Windows 套接字资源不足，串行完整重跑通过；不据此修改业务逻辑。未运行真实 Redis、Linux 或 release 构建；本轮修复随本次提交交付。


## Registry 标准 Service 入口（2026-10-21）

已提供 execution::Registry::into_service 与 registry::Service，显式注入 Request 取键函数；protobuf / message Registry 直接实现 Tower Service。公共 Layer 可直接包住整张分发表，注册项内的路由 Layer 保留。Context、未知键和原业务错误不被协议化，就绪等待继续在原期限内执行，取消或过期先于取键和查表检查。

protobuf 表克隆改为共享路由快照，注册时写时复制，服务调用不复制整张表；旧克隆不因后续注册变化，实际策略状态仍由 Layer 共享。普通业务 RPC 适配和本地公会示例已切换到 Service 入口；显式 dispatch 保留，编码适配复用 Service 路径。未新增 Registry Layer、路由 fallback、后台队列或自动重试。

本轮 Windows 验收：工作区串行 311 项测试通过、32 项外部专项按默认规则跳过，包含 19 项指南示例。新增 5 项回归覆盖公共与路由 Layer 的 Context 传递、原错误与未知键、非 Clone 取键函数共享、停止前不取键、就绪等待和非协作式取键的期限，以及 protobuf / message 直接 Service 与注册快照隔离。全目标编译与 Clippy、workspace rustdoc、dispatch / rpc / client 三个 smoke、格式、220 个文本文件 UTF-8/LF、119 处相对文档路径及空白检查通过。未运行真实 Redis/NATS、Linux 或 release 构建；本轮随本次提交交付。

## Request / Response 与扩展位置（2026-10-21）

已将 Extensions 从 Context 移到 Request，新增根导出的 Response<R>，私有实现分别位于 request.rs / response.rs。Context 保留服务、操作、截止点、取消与 Span；Request / Response 的构造、转换、拆装不依赖 ContextError，也不派生执行范围。原 Context::child 负责范围派生，适配器明确移交输入扩展。

execution / protobuf / message 注册器、事件处理、RPC 协议适配与网关 Pipeline 已使用 Response 包装成功结果；泛型执行/锁设施保持透明。输入与输出转换分别保留对应扩展，Err 不改为回应。事件聚合按注册顺序合并输出扩展，同类型后者覆盖；同步会话通知按遍历顺序合并，入队后的独立事件不回传业务结果。PendingResponse 保留本地业务输出扩展，notify_request / deliver 保留下行输出扩展；客户端编码和 replay 只消费协议正文。

gRPC Server::request 负责安装本地输入扩展，Server::context 只解析范围。Client::execute / execute_service 返回 Response<R>；内层发送 Service 使用 Response<tonic::Response<R>>，便利方法转换正文仍保留本地输出扩展。原生 FnOnce 发送回调继续使用 tonic 类型。gRPC / NATS 不序列化 Extensions，网络接收端重新安装自己的能力。指南、真实调用与示例同步迁移。

本轮 Windows 验收：工作区串行 317 项通过、32 项外部专项默认跳过，其中包含 20 项指南示例；隔离真实 NATS 的 14 项事件测试及 2 项多网关专项通过。新增回归覆盖借用输入转换、显式范围派生与输入隔离、输出转换及原始错误、protobuf 输出扩展、事件聚合顺序，以及网关独立回应与通知输出元信息。Clippy、无警告 workspace rustdoc、execution / events / dispatch / rpc / client / application 六个 smoke、格式与空白检查通过；222 个文本文件 UTF-8/LF 和 119 处相对文档路径检查通过。未运行真实 Redis、Linux 或 release 构建。

## Registry trait 与 Extensions 写时复制（2026-10-21）

execution::Registry 已从具体结构改为带关联类型的注册/派发 trait，默认实现更名 Table；registry::Handler 提供受原预算、取消及追踪保护的擦除处理链，registry::Service 支持泛型实现和 Arc<dyn Registry>。protobuf / message 注册器、普通业务 RPC 与玩家 HandlerRegistry 已支持注入自有或共享注册器。protobuf / message 注册错误保留具体实现类型；玩家 RPC 组装边界沿用 Configuration 文本映射。自定义注册器的克隆与更新可见性由其实现决定，默认 Table 仍保留注册快照；timer / 服务器事件的入口隔离保持不变。

Extensions 改为可选 Arc 表和写时复制：空集合无表分配，克隆 O(1)，实际修改共享表时只复制条目与值的 Arc；容器隔离和共享值语义不变。无效删除、空集合合并不复制表，向空集合合并直接共享源表。新增回归覆盖共享/分离行为、自定义注册错误、动态替换/拦截、trait 对象、派发期限与取消，以及完整编解码和游戏 RPC 适配。

本轮 Windows 验收：工作区串行 323 项通过、32 项外部专项默认跳过，其中包含 21 项指南示例；Clippy、无警告 workspace rustdoc、本地分发 / RPC / 客户端 TCP 三个 smoke、格式和空白检查通过。223 个文本文件 UTF-8/LF 和 119 处相对文档路径检查通过。未运行真实 Redis/NATS、Linux 或 release 构建。


### 执行错误契约校正（2026-10-21）

execution::Error<E> 与 Request / Response 一起从根公开，统一表达 Stopped 与 Inner(E)；E 保留具体处理错误。
Registry 关联类型使用 HandlerError、DispatchError 与 RegistrationError；dispatch 和路由 Service 均返回
execution::Error<G::DispatchError>。默认 Table 的 DispatchError 仅包含 Unknown / Handler，不重复存放执行停止。
protobuf / message 适配保持这一层结构；自定义 RPC 注册器显式提供 DispatchError 到 Status 的映射，
默认构造保留默认映射。Error::map_inner 转换具体错误，Error::flatten 接续重复执行保护，
停止原因、错误来源与请求/响应扩展保持不变。原生 Tower Layer 仍可适配任意内层错误，
不要求第三方 Service 或后端采用共同返回类型。


本轮 Windows 验收：全工作区串行测试 326 项通过、32 项外部专项按默认规则跳过，其中包括 21 项指南示例。
全目标 Clippy、无警告 workspace rustdoc、本地分发 / RPC / 客户端 TCP 三个 smoke、格式与空白检查通过；
223 个文本文件 UTF-8/LF、119 处相对文档路径检查通过。未运行真实 Redis/NATS、Linux 或 release 构建。


### gRPC 调用准备阶段命名校正（2026-10-21）

Outgoing 更名为 client::PreparedCall，根同时重导出 PreparedCall，不保留旧名称别名。
连接准备前使用 client::Call，客户端 Layer 准备 Channel 和元数据后向内层发送器传递 PreparedCall。
Prepared 只描述本地准备阶段，不承诺远端已连通或就绪；取消、预算与发送行为保持不变。

验收：tok-grpc / tok-message 共 51 项测试、21 项指南示例通过；全工作区全目标 Clippy、无警告 rustdoc、格式与空白检查通过，223 个文本文件 UTF-8/LF、119 处相对文档路径检查通过。未执行 release 构建。

Call 与 PreparedCall 的实现聚合在 client/call.rs，分别表达连接准备前后；公开路径和执行行为不变。

模块聚合验收：tok-grpc 19 项测试、全目标 Clippy 与无警告 rustdoc 通过。


### 通用 gRPC 调用移除业务关联序号（2026-10-21）

Call / PreparedCall 不再携带 request_id；Client::execute / execute_service 移除关联序号参数，
通用失败日志保留目标、状态与关联追踪。业务协议中的 request_id、回应编号校验及玩家会话语义仍由
tok-message 等上层维护；可选本地关联信息使用 Request 扩展或 span，不自动传播到网络。

移除通用关联序号验收：全工作区串行测试 326 项通过、32 项外部专项默认跳过，包含 21 项指南示例；全目标 Clippy、无警告 rustdoc、格式与空白检查通过。222 个文本文件 UTF-8/LF、119 处相对文档路径检查通过，未执行 release 构建。


### execution / gRPC 审查问题修复（2026-10-21）

共同执行驱动在内层 poll_ready 成功后、调用业务前复查原期限与取消；迟到就绪不再启动业务。
Call::new 消费单次 Service 并支持借用，不要求 Clone；execution::Layer 和 gRPC 客户端共用同一驱动，
移除 gRPC 重复的期限、取消和就绪执行循环。协议预算收紧、准备失败与 Status 映射仍留在 gRPC。
客户端准备、取消、超时及发送失败日志显式进入 Context.span，不依赖调用者的当前任务 span。

通用锁移除自身 Stopped 变体，返回 execution::Error<lock::Error<E>>；原调用停止保持外层，
锁特有获取、执行和释放期限保持内层错误，RPC 适配显式映射。持锁执行先收紧 Context 至执行上限，
迟到就绪不调用业务，执行上限失败仍释放租约；原调用取消/丢弃的未完成释放继续由有限 TTL 兜底。

本轮 Windows 验收：全工作区串行测试 330 项通过、32 项外部专项默认跳过，包含 21 项指南示例；全目标 Clippy、无警告 workspace rustdoc、execution / dispatch / rpc / client 四个 smoke、格式与空白检查通过。222 个文本文件 UTF-8/LF、119 处相对文档路径检查通过。未运行真实 Redis/NATS、Linux 或 release 构建。
