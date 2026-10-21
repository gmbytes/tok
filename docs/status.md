# 当前实现状态

核对日期：2026-10-21。P6 已提交为 `06d9dd2`，P7 应用组装与接口清理已提交为 `017a9ea`。框架的“服务 + 处理链”方向校正已完成[七批计划](implementation-plan.md)本轮验收，历史结果见[实施历史](implementation-history.md)；本页只维护当前能力和边界。

## crate 与基础契约

玩家运行契约已独立为 [tok-player](../crates/tok-player/src/lib.rs)，分库提交为 `9f32c91`；游戏协议 CallContext 位于 [tok-game-rpc](../crates/tok-game-rpc/src/context.rs)。Context / StopReason 归 tok-execution，Extensions 独立为 tok_core::Extensions。core 删除 tokio-util 与 tracing 依赖，当前只依赖 serde / thiserror。

| core 范围 | 当前职责与接口 |
| --- | --- |
| service | Identity 表示无地址的服务身份，ServiceDescriptor 表示可发现的网络服务 |
| discovery | ServiceDirectory 读本地快照，ServiceRegistry 发布/撤销，DirectoryWatch 合并变更通知 |
| routing | ServiceRouter 根据服务名与业务 ID 选点，不负责发现写入或自动重试 |
| lock | Key / Lease / Backend 定义请求级租约，后端在 infra、中间件在 RPC |
| 根导出 | Extensions 是类型化能力集合，BoxFuture 是不隐含 Result 的 Send Future 别名 |

锁、所有权和发现统一使用 BoxFuture，并显式保留 core::Result 或 player::Result。原 Operation 与 RegistrationTask 别名移除；根导出的 ServiceDirectory 等角色名前缀有辨识用途，继续保留。DirectoryStateUnavailable 表达目录本地状态不可用，Unsupported 表达当前组件不支持能力；错误类别按 core 声明的契约定义，不收集 Redis、RPC 或玩家业务专有错误。

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
| RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 提供连接、端点和通用执行；[tok-game-rpc](../crates/tok-game-rpc/src/lib.rs) 提供游戏消息、事件、timer 及可选请求锁 | RPC 工厂初始化必须安装处理链，未安装不发布；同一端点不能覆盖同名 protobuf 服务 |
| 事件 | [Registry](../crates/tok-events/src/registry.rs) 本地借用及远程解码共用处理表，有序多处理者、原始错误及 Arc 保留 | 本地 dispatch 显式等待；远程发布不等待业务结果，不提供事务或隐式去重 |
| topic / group | Event::topic 默认全局，可固定或由正文字段派生；[Pool](../crates/tok-events/src/interest.rs) 按组件/主题聚合固定与会话引用 | group 属于订阅组件；无组独立接收，同组分担不等于玩家归属路由 |
| NATS | [Bus](../crates/tok-events/src/nats.rs) 按 namespace、topic、类型安全路由，有限 SID、建立操作、缓冲与处理预算 | Core NATS 即时事件；发布只表示接受，订阅建立有限回环就绪；没有 JetStream |
| gateway / session | [Gateway](../crates/tok-gateway/src/server.rs) 作为共同设施的内置扩展，提供快照、状态工厂、生命周期观察及准入/执行/新下行 Layer | 通信、FIFO、timer、ACK、恢复与交接保留各自状态机；不强制普通服务具备 session |
| 动态成员 | 异步 join/leave 等待所选组件就绪，可信策略在登录前准备初始主题；目标交接提交前准备兴趣 | 宽限期与冻结期间保留兴趣；离线/恢复/冻结不投递即时事件，冻结窗口不补收 |
| 发现与协调 | Memory / Redis 发现、会话所有权、请求级锁；发现缓存和路由分离 | Memory 仅单进程；租约与超时是尽力保护，不保证严格 fencing 或恰好一次 |
| 工程与文档 | 十个框架 crate、demo、构建期 protobuf/消息 ID 生成、中文 rustdoc、workspace lint、LF、Windows/Linux CI | CI 配置不等于本地已执行 Linux 验证；真实外部服务与 release 结果分别记录 |

## 使用与失败边界

普通服务的构造仍是一套 Factory / Init，RPC、事件都通过 Request 和 Tower 执行。Application::register_rpc_factory 自动按配置创建入口，将 game_rpc::Binding 注入工厂；在工厂内初始化状态、路由和 Layer 后安装。初始化装配用的 game_rpc::Binding 不保存回自身执行扩展，避免引用环。Application::bind_rpc 用于自定义 protobuf 服务，监听继续由 Application 合并管理。

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

通用 tok-rpc 保留 RpcClients / RpcEndpoint / Server，通过泛型请求和 Tower 接入任意协议；正常依赖移除 tok-player / tok-events / tok-proto。新增 tok-game-rpc 承载游戏 CallContext、Handler / HandlerRegistry、玩家与普通业务协议客户端、绑定槽、通知编码及 timer / 事件 / 请求锁适配。门面从 game_rpc 访问游戏协议，rpc 访问通用设施；移除通用 CallHandler 上的事件契约，不提供旧路径兼容别名。Application 的普通自定义 protobuf 绑定继续使用通用端点，游戏 register_rpc_factory 使用新协议适配。客户端 Frame、内部 protobuf、事件 topic、timer、租约和无自动重试语义保持原有约束；事件与 timer 入口仍仅供可信服务端。

本轮 Windows 验收：工作区 255 项测试通过（包含 12 项指南示例和 2 项编译失败文档测试），31 项外部专项跳过；通用 RPC 新增 3 项无游戏类型执行测试，游戏 RPC 的事件入口隔离、身份、timer、共享连接、追踪及请求锁专项通过。全目标 Clippy、workspace rustdoc、application / lifecycle smoke、格式、UTF-8/LF、内部文档链接及空白检查通过。cargo tree 核对通用 RPC 的正常依赖不含玩家、事件或游戏 protobuf。不做 release 构建；未运行真实 Redis/NATS 专项或 Linux，本轮随本次提交交付。

## 调用执行与生命周期分库（2026-10-21）

原 tok-service 拆为 tok-execution 与 tok-lifecycle。tok-execution 保留 Context / StopReason、Request、Tower Layer / Service、Call 和 SyncHandler；tok-lifecycle 根导出 Factory / Init / InitFuture、Builder / Prepared / Running、Handle / Phase / Error，task 模块保留 Supervisor / Handle / Reservation / Task 等通用监督能力。两库共同依赖 tok-core，彼此无依赖；tok-lifecycle 不依赖 Tower、RPC、事件或玩家协议。门面路径改为 tok::execution / tok::lifecycle，不提供旧 tok-service 或 tok::service 兼容路径。

应用、RPC、事件、网关、示例及指南使用新路径；服务启动就绪、任务容量、预算取消、退出等待及线协议语义不变。本轮不做 release 构建。

本轮 Windows 验收：工作区 255 项测试通过，31 项外部专项按约定跳过，包含 12 项指南示例及 2 项消费式生命周期编译失败文档测试；全目标 Clippy、workspace rustdoc、lifecycle / execution smoke、格式、200 个文本文件 UTF-8/LF 及 82 处内部文档链接检查通过。依赖树确认两库无相互依赖、tok-lifecycle 无 Tower 依赖。未运行真实 Redis/NATS 专项、Linux 或 release 构建；本轮随本次提交交付。

## 执行上下文移除固定来源（2026-10-21）

Context 移除 source 字段、Source 枚举和构造/child 的来源参数。执行核心只管理服务、操作、期限、取消、追踪和 Extensions。RPC、事件与网关在入口提供类型化扩展：复用游戏 CallContext、EventId / Topic / Arc<Message>、TimerTrigger，仅网关新增无正文的 ClientRequest 关联标识。RPC 关联信息与事件元信息可以共存，不再将传输方式、触发原因和客户端入口混成互斥枚举。

事件 Registry 的本地与远端分发提供相同事件头，派生子上下文覆盖当前事件头，不修改父扩展；本地派生仍清除继承的原始编码。客户端无法发布事件、请求与 timer 分表、会话身份校验、预算和取消约束保持不变；Extensions 元信息不作为认证证明。调用方和指南已迁移，不保留 Source 兼容类型。

本轮 Windows 验收：工作区 255 项测试通过、31 项外部专项跳过；回归覆盖 RPC 与事件元信息共存、当前事件头覆盖继承值但不修改父上下文、原始消息保留和客户端请求标识。全目标 Clippy、workspace rustdoc、execution / application smoke、格式、UTF-8/LF 与内部链接检查通过。未做 release 构建，未运行真实 Redis/NATS 或 Linux；本轮随本次提交交付。

## 玩家计时触发契约归属校正（2026-10-21）

TimerTrigger 从 tok-core 移至 tok-player，与 TimerSpec / TimerUpdate 聚合，门面路径为 tok::player::TimerTrigger。core 移除 timer 模块及根导出，不保留兼容别名；网关调度、玩家执行与游戏 RPC 统一消费新路径。通用执行测试和示例使用本地扩展类型，不为计时元信息依赖玩家库。内部 protobuf 与计时调度、预算及取消语义不变。本轮不做 release 构建。

本轮 Windows 验收：受影响的 core / execution / player / game-rpc / gateway 共 132 项测试通过、2 项外部专项跳过；demo 生产测试 2 项、指南示例 12 项通过。全目标编译与 Clippy、workspace rustdoc、execution smoke、格式、199 个文本文件 UTF-8/LF 与内部文档链接检查通过。未重复无关外部专项或完整工作区测试；未做 release 构建或 Linux 验证，本轮随本次提交交付。
