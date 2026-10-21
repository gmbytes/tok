# 服务与处理链方向校正实施计划

本计划替代旧的阶段零至八推进顺序，是当前重构与实施的唯一批次入口。P1 已提交为 `f54b545`，文档校准为 `2552274`；P2 公共工厂与生命周期已提交为 `c6ffdc3`；P3 统一入口已提交为 `368fc74`；P4 gateway 扩展已提交为 `e8dda17`；P5 传输 topic 路由已提交为 `bb015ad`；P6 动态兴趣与主题策略已提交为 `06d9dd2`；P7 完成应用组装与清理并通过本轮验收，已提交为 `017a9ea`。旧阶段与验收见[实施历史](implementation-history.md)，代码现状见[实现状态](status.md)。

目标：框架提供“服务 = 状态 + 入口路由 + 处理链”，处理链由中间件与 handler 组成；tok-gateway 成为共同设施的内置扩展，tok crate 保持门面与应用组装入口。[总体设计](design.md)规定边界，[服务运行模型](service-runtime.md)规定目标契约。

## 顺序与依赖

| 批次 | 内容 | 前置 | 状态 |
| --- | --- | --- | --- |
| P1 | 验证统一执行抽象 | 已确认的设计 | 已完成最小模型验收 |
| P2 | 下沉服务构建与运行设施 | P1 契约成立 | 已实施，验收见下文 |
| P3 | 统一 RPC 与事件执行入口 | P1、P2 | 已实施，验收见下文 |
| P4 | gateway 迁移为服务扩展 | P2、P3 | 已实施，验收见下文 |
| P5 | 实现传输 topic 路由 | P3 统一事件契约 | 已实施，验收见下文 |
| P6 | 动态兴趣与主题策略 | P4 生命周期、P5 路由 | 已实施，验收见下文 |
| P7 | 全面迁移、旧接口清理与验收 | P1 至 P6 | 已完成本轮验收并交付 |

先完成 P1 的可编译最小模型，再决定公共模块/crate 的承载位置。不是先新增一批空 trait 或移动所有目录。每批结束都必须有可运行示例、有效测试和同步文档，后续批次不以“接口已存在”代替完成。

## P1 验证统一执行抽象

**目标：** 用最小代码证明同一执行协议能同时承载 RPC、事件、timer 以及借用状态的本地执行。

**工作：**

- 明确 ExecutionContext 的公共身份、可信来源、操作、追踪、预算与取消信息；网关、manager、session 和业务状态通过类型化能力扩展提供。
- 验证带上下文的类型化输入、不同结果类型、通用 Layer 和路由级 Layer。通用执行核心不依赖 tonic::Status、protobuf、NATS 或 gateway。
- 验证可借用 &mut C 的异步 handler、同步回调适配、错误保留和首错顺序；本地执行不被迫编码、spawn 或包装为 Arc<Mutex<_>>。
- 用普通服务与最小 gateway 组装示例验证扩展方向；只验证组装和能力接入，不在本批迁移生产连接与交接循环。
- 根据编译样例记录公共设施落点、生命周期归属与 Tower 适配限制，避免形成相互竞争的处理抽象。

**验收：** 同一个日志/预算 Layer 在 RPC 形态、事件和 timer 调用上工作；本地借用保持有效，业务错误可辨认，取消和预算不被重置；例子没有 gateway → tok 门面依赖。记录不适用于借用调用的 Layer（如要求静态后台执行者）的边界。

**边界：** 本批不搬迁 Application/Gateway，不改客户端协议、subject 或动态订阅，不要求提前实现全部服务 API。本批只完成原型验证，不自动扩张到 P2。

### P1 已形成的落点

- tok-core 的 execution::Context / Source / Extensions 与 service::Identity 提供通用契约，不依赖协议或 gateway。
- 新增 tok-service 承载实际的 Request<T>、Tower Layer/Service 与受预算和取消保护的调用 future；tok 门面通过 service 导出，tok-service 的正常依赖不包含 gateway、RPC、事件或 NATS。
- 本地异步 handler 可借用状态，普通调用、事件与 timer 保留不同输入/结果类型；最小 gateway 示例只安装查询/任务句柄能力，没有迁移生产 worker。
- P1 保持 Application/Gateway 结构不变；P2 已将公共 Factory 与生命周期监督放入 tok-service。
- 独立示例为 `examples/demo-game/examples/execution.rs`，命令为 `just execution-smoke`。Layer 的克隆、readiness 预算和借用/静态任务兼容限制在用户指南记录。

P1 上下文边界校正：移除通用 `target`；`operation` 明确为服务内稳定入口标识，`service` 只标识当前执行服务。实体身份按需通过类型化输入/扩展提供；路由输入、选定端点及熔断状态分离，契约见[服务运行模型](service-runtime.md#操作身份路由与熔断边界)。通用路由器和熔断器尚未实施，本次不扩大 P1 范围。

### P1 验收记录（2026-10-20 Windows）

新增 11 项执行/上下文测试与 1 项 rustdoc 示例，工作区共 187 项测试通过。覆盖 RPC/客户端请求/事件/timer 的类型化结果、同一 Layer、跨 await 本地状态借用、同步适配、首错停止及原错误、readiness 预算、超时/取消/Drop、非协作式迟到结果、父子取消、能力隔离及追踪跨 await 关联。

全目标 Clippy、workspace rustdoc、格式和 LF 检查通过；execution-smoke、原有 rpc-smoke 和 client-smoke 通过。未改变 NATS/Redis/session 协议路径，本批不重复运行真实 NATS/Redis 专项或 release 构建，不将旧结果记作本批验证，也不声称已完成生产入口或生命周期迁移。

## P2 下沉服务构建与运行设施

**目标：** 普通服务和 gateway 能依赖同一套构建、任务及生命周期设施。

**工作：**

- 按 P1 决定的落点下沉公共工厂、初始化/运行任务、服务状态、就绪、受监督任务、停止与结束等待契约。
- 组装状态工厂、入口路由、服务级/路由级处理链；RPC 或事件可任选，也可同时安装。
- Application 保留节点配置、端点及整应用组装；tok 门面统一导出。共享端点只绑定一次，服务初始化成功后才发布就绪。
- 普通服务与 gateway 复用任务容量、取消与清理规则；专用监听循环和 session 状态机不为统一而强行合并。

**验收：** 普通服务和最小 gateway 都能构建、初始化、停止与等待结束；初始化失败/超时/取消、运行异常、正常退出和强制收束不遗留任务或注册。依赖无环，下层不导入 player/scene 专用状态。

### P2 已形成的落点

- `tok_service::lifecycle::{Factory, Init, InitTask}` 与 `tok_service::task::Task` 提供公共工厂契约，支持 trait 实现及闭包。工厂自行初始化状态、组装类型化入口和 Tower 链，不另建平行的 Handler/中间件体系；RPC 与事件的统一注册表留在 P3。
- task::Supervisor / Handle / Reservation / State 只负责任务范围；lifecycle::Builder → Prepared → Running 消费式编排服务启动和退出，服务 Handle / Phase 提供查询。只有 start 推进就绪，不公开 mark_ready，不允许重复初始化或直接运行未初始化的服务。
- 预留、排队和运行共用后台容量；初始化/主任务不占名额；used_capacity 包含预留。连接通过 Supervisor::track 跟踪，Supervisor::child 将子范围回收纳入父结束等待。shutdown 消费任务范围，成功后 wait_finished 立即完成；超时返回错误并保留未释放外部工作对应的 Stopping。
- Application 保留节点配置、共享 RPC 端点、目录注册和应用级退出。工厂改用下层 Factory；端点和发现通过类型化 `application::Binding` 扩展注入，每服务后台容量默认 64，可显式调整。
- Gateway 后台监督已复用 Supervisor，连接与 session 循环保持专用；工厂使用 Gateway::new_in 关联所属服务的父运行范围。Gateway::tasks 提供任务范围查询和操作，完整服务阶段/退出由所属 lifecycle::Handle 提供；完整业务生命周期扩展及消息 Layer 仍属 P4。
- 应用模块改为 application，服务生命周期模块为 lifecycle，构建器为 Builder::build，初始化参数为 Init（service 字段提供生命周期句柄）；Gateway 直接复用 task::Handle / Reservation，无专用上下文包装。应用错误使用 application::Error，失败计数为 tok_application_failures_total。
- 服务主任务在发现发布前交给监督，避免发布失败或取消时漏掉主任务清理；共享端点仍只绑定一次，初始化成功才开启入口并发布。
- `just lifecycle-smoke` 同时验证无端点普通服务（状态 + 请求/本地事件类型化链）与 TCP Gateway 的初始化、停止、异步清理和结束等待。

任务范围可读性收敛：ScopeState 维护共享状态与活动计数，ActivityGuard 通过 Drop 维护活动生命周期，Submission 的容量/活动守卫均说明持有和释放作用。lifecycle::Handle 保留服务阶段、停止和结束等待，任务提交、预留及容量查询统一经 tasks()。本次不改变任务调度或退出策略，外部专项沿用本批此前记录。

### P2 验收记录（2026-10-20 Windows）

工作区 207 项测试通过（P1 基线 187 项，本批新增 9 项服务生命周期测试、5 项任务范围测试、3 项应用/网关测试、1 项指南示例及 2 项编译失败文档测试）。覆盖初始化失败/超时/取消/panic、启动期后台故障、共同容量、正常清理、退出超时、Drop、连接跟踪、父子运行范围与结束等待，以及初始化/后台任务追踪关联。另验证消费式退出成功后立即完成结束等待、外部工作未释放时超时保留 Stopping、组件清理晚于子任务范围退出，以及重复初始化/启动被类型拒绝。

全目标 Clippy、workspace rustdoc、格式/LF、84 处 Markdown 仓库内链接检查通过；execution-smoke、lifecycle-smoke、rpc-smoke、client-smoke、nats-smoke 通过。真实 NATS 13 项测试通过；Redis 发现 2 项测试及发现双进程、共享所有权、三进程交接 3 项集成均通过。本次外部回归串行运行，避免 Windows 示例 exe 占用影响构建。

本批未执行 release 构建或 Linux 本地验证，不将 Windows 结果代替 CI。P2 验收时 P3–P7 尚未实施；P2 现已提交为 `c6ffdc3`。

## P3 统一 RPC 与事件执行入口

**目标：** 接入适配器复用共同执行核心，业务只维护一套事件封装、上下文与处理注册契约。

**工作：**

- 迁移 CallHandler、事件 Handler、Registry 与本地 Dispatcher 的执行能力，保留方法分发、事件分发和编解码各自职责。
- Event / Envelope / Registry / Subscriber 统一；支持多种事件与有序多个处理者。普通本地 Rust 事件不要求 prost，远程编解码由安装适配提供。
- RPC、本地事件、NATS 接收与 timer 形态复用 Layer 协议；明确 dispatch 完成结果与 publish 接受结果，通用错误不暴露 gRPC 类型。
- 保留原始 Arc<Message> 与 event_id 的转发路径；本地派生上下文继承预算、取消与追踪，不隐式重新排入自身 session 队列。
- 示例同时演示同一回调的本地执行与远程接收，不增加仅名字相同、类型与调用方式不同的平行注册表。

**验收：** 同一注册和 Layer 可服务不同入口；本地无序列化、无隐式队列，首错规则成立；RPC 只生成对应回应，事件不能生成客户端 Response；未知类型、坏包、业务错误和系统错误分别处理，追踪不丢失。

### P3 已形成的落点

- CallHandler / TowerHandler 改用共同 `Request<(CallContext, Payload)>`；RPC 方法和 timer 注册仍负责各自编解码，但 `register_service` / `register_timer_service` 与事件使用同一 Tower 协议和执行 Layer。
- 删除同步 local::Dispatcher。统一 `Registry<C, O, F>` 按稳定事件名与 Rust 类型注册多个有序处理者；`register` 支持跨 await 借用状态，`register_service` 安装类型化处理链，`sync_handler` 适配同步处理。
- 本地 dispatch 与远程 receive 使用同一 Envelope 和处理者，默认不安装解码器；enable_protobuf 为已注册类型安装显式远程适配，接收时解码一次。泛型业务错误、协议/类型错误及取消/预算分别表达。
- 事件上下文由稳定名称及原事件 ID 派生，继承期限、取消、追踪和能力；远程原始 Arc<Message> 保存在扩展中，网关转发不重新编码。本地事件仅在需要编码的会话/传输边界编码，不继承父事件的原始正文。
- Subscriber 声明当前执行服务身份，可安装本地能力；NATS 接收适配建立有限预算和追踪上下文。发布仍只表示接受发布，分发结果表示当前处理链完成，不混为远端业务确认。
- RPC 边界按来源和注册 ID 建立稳定 operation；服务默认执行上限 30 秒，grpc-timeout 只缩短预算。请求、timer、事件结果保持分离，事件没有客户端 Response。
- RpcService::deferred / rpc::Binding 将共享端点与工厂安装的实际处理链衔接，绑定校验身份及一次安装；demo 的 player/guild 状态和处理链移入工厂初始化，不再使用空生命周期工厂替代业务初始化。
- 新增 `just events-smoke`，验证同一回调本地借用与远程解码执行；保留 execution/lifecycle 示例，用户指南已迁移真实接口。

本批不迁移 gateway/session 业务准入、连接/会话状态扩展或新下行 Layer，也不改变客户端 Frame、NATS subject、topic 路由或订阅聚合；这些仍由 P4–P6 完成。Application 仍显式组装端点及工厂，完整便捷服务装配入口在 P7 汇总迁移，不能把本批的绑定槽等同于全部应用 API 已收敛。

### P3 验收记录（2026-10-20 Windows）

工作区 214 项测试通过（含 10 项指南示例及 2 项编译失败文档测试）。覆盖同一 Layer 处理 RPC、timer、本地及解码事件，同一事件表的多类型/有序多处理者、跨 await 状态借用、原始错误、坏包/未知类型、容量/类型冲突、原始 Arc 与 ID 保留、本地子事件不继承父正文、取消/预算，以及初始化绑定的身份/重复安装/停止行为。网关测试还验证本地与远程事件共用转发链，timer 效果按序合成且没有客户端 payload。

全目标 Clippy、workspace rustdoc、格式、UTF-8/LF 和 84 处 Markdown 仓库内链接/锚点检查通过。events-smoke、execution-smoke、lifecycle-smoke、rpc-smoke、client-smoke、nats-smoke 六个示例通过；真实 NATS 13 项、Redis 发现 2 项及发现双进程/共享所有权/三进程交接 3 项集成均通过。外部回归串行运行，使用测试独立命名空间。

本批未运行 release 构建或 Linux 本地验证，不将 Windows 结果替代 CI。以上为 P3 验收时记录；P3 已提交，P4 当前验收另列。

## P4 gateway 迁移为服务扩展

**目标：** gateway 使用通用构建/运行设施，并提供游戏定制所需的能力。

**工作：**

- 迁移 Gateway 的公共监督与执行部分，Ingress、连接、session FIFO、timer、下行和交接仍保留专用状态机。
- 补齐 gateway、SessionManager、Session 的身份/阶段/容量/连接/主题快照、停止请求、取消信号和结束等待；阶段变化后操作必须重新校验。
- 提供作用域状态工厂及生命周期扩展点，覆盖初始化、绑定变化、恢复、交接准备/取消/提交与关闭。必需状态迁移需显式快照适配，不能静默丢失。
- 为业务准入、服务/session 执行、新下行安装共同协议的 Layer；复用认证、所有权、队列、序号和补发的不变量。
- 事件处理者与消息中间件取得同样的运行时能力；普通服务无需构造 gateway 或 session。

**验收：** 游戏通过扩展完成查询、生命周期管理与业务拦截，无需复制 Gateway 循环；登录、顶号、租约失效、timer、FIFO、ACK、恢复、交接和取消回归通过。新下行在编号和编码前处理，缓存补发不再次执行 Layer。

### P4 已形成的落点

- Gateway::new 显式接收服务身份；new_in 从 Init 继承身份、服务句柄和扩展。构造闭包完成所有配置后统一 initialize，避免提前注册 RPC 持有旧 manager 配置。
- gateway::Handle、SessionManager 和 SessionHandle 提供各自范围的快照、取消、停止及结束等待。Gateway 完整等待包含 ingress.finish，tasks().wait_finished 仅代表任务范围。
- Pipeline 使用共同 Request / Tower Layer，覆盖业务准入、FIFO 内请求/事件/timer 执行与新下行。上下文包含实际 gateway、manager、session 能力及作用域状态；不开放内部状态锁。
- 准入绑定当前连接代次，重连/顶号使旧等待失效；已经执行的任务继续按原预算收束。新下行异步 Layer 位于编号、编码和缓存之前，最终重验绑定及主题，补发不重复执行 Layer。
- session::extension::Factory 显式定义初始化、快照、校验和恢复；同步生命周期观察回调在框架锁外执行。跨网关快照必须携带扩展种类与正文，提交前校验，提交后按新身份恢复；恢复失败释放目标所有权，不复活源会话。
- demo 安装同一个观察 Layer 和显式快照的会话统计扩展；指南、生命周期与执行示例迁移至真实 API。客户端 Frame 仍为 v3，未发布的内部 HandoffSnapshot v1 增加必需扩展字段。

### P4 验收记录（2026-10-20 Windows）

工作区 226 项测试通过（含 11 项指南示例及 2 项编译失败文档测试）。新增覆盖三层能力和生命周期查询、请求/事件/timer 共用 Layer、下行改写与补发不重复执行、等待准入时顶号/重连、等待下行时退出主题、初始化失败释放所有权、扩展状态迁移与失败边界、完整网关退出等待。原有 FIFO、timer、续租、ACK、同网关恢复和跨网关交接回归通过。

全目标 Clippy、workspace rustdoc、格式、UTF-8/LF 及仓库内文档链接检查通过。events-smoke、execution-smoke、lifecycle-smoke、rpc-smoke、client-smoke、nats-smoke 通过；真实 NATS 13 项、Redis 发现 2 项及发现双进程/共享所有权/三进程交接 3 项集成通过。外部回归串行运行，demo 实际安装观察 Layer 和可交接的会话统计扩展。

P4 已提交为 `e8dda17`。未运行 release 构建或 Linux 本地验证。下一批为 P5 传输 topic 路由，P6 动态兴趣及 P7 整体组装仍未实施。

## P5 实现传输 topic 路由

**目标：** topic 同时参与发布与订阅路由，而不再仅用于网关本地筛选。

**工作：**

- 在 Envelope、Message、EventEnvelope 和 subject 映射中明确 topic；主题安全编码集中在适配器，默认主题与普通同名主题/通配兴趣区分。
- Subscriber 分别声明事件类型、topic、可选 group；订阅组件可自定义转发、广播及其他业务。
- 保留固定主题订阅与类型过滤，接收时校验主题、事件名和 subject 一致。
- 更新旧示例和协议使用方；未发布结构直接调整，不维护开发中间版本别名。

**验收：** 环境、topic、类型隔离；同组分担、不同组独立、无组实例接收；非法主题与伪造正文被拒绝。发布不逐条等待回环，订阅建立仍有界确认就绪。

### P5 已形成的落点

- Event::topic 由事件自身声明默认全局、固定或正文派生路由，Envelope 自动取得主题并在构造/编码/Registry 解码时校验；不提供发布时覆盖路由。group 继续属于订阅组件。
- 新增 Topic 与 TopicFilter，发布只接受具体主题；默认、具名同名主题及 All 订阅明确区分。主题有界且集中校验，不允许 NATS 通配符注入。
- Envelope / Message / EventEnvelope 与内部 EventRequest 保留相同 topic；本地分发不编码，远程与玩家 RPC 转发不丢失主题。
- Subscriber 显式接收主题兴趣与 group，类型仍由 Handler 声明。NATS 以无碰撞单 token 编码路由主题，接收时校验兴趣、正文及完整 subject。
- 每个兴趣/事件类型计入 SDK 订阅和缓冲预算；不改变无逐条回环发布、有限订阅就绪和串行 handler 行为。
- 内置 forward / notify 回调读取完整 Envelope，demo 显式组装固定区服主题。客户端没有事件或订阅管理入口，动态会话兴趣留在 P6。

### P5 当前实现验收（2026-10-20 Windows）

工作区 230 项测试通过（含 11 项指南示例及 2 项编译失败文档测试）。新增验证默认/具名主题区分、Unicode 与长度边界、无效线协议 topic、本地与远程解码保留 topic、子事件不继承父事件路由；既有 RPC 事件测试增加具名主题端到端断言。另验证类型化事件篡改主题、无效派生路由以及正文/路由不一致在进入业务前被拒绝。

真实 NATS 14 项专项通过（tok-events 13 项及双网关 1 项），覆盖固定/所有主题兴趣、环境和类型隔离、同组分担与跨组独立，以及 subject/正文主题或类型伪造。events-smoke、rpc-smoke、client-smoke、nats-smoke 通过。全目标 Clippy、workspace rustdoc、11 项指南文档测试、格式、UTF-8/LF 和 81 处仓库内文档链接/锚点检查通过。

以上为事件契约决定路由的 Topic API 验证；P5 已提交为 `bb015ad`。本批未重复运行 Redis 专项、release 或 Linux 本地验证。动态会话兴趣、引用合并与自动主题策略仍留在 P6。

## P6 动态兴趣与主题策略

**目标：** 聊天、区服等需求由网关按本地会话聚合订阅。

**工作：**

- 明确组件与会话主题的绑定关系；固定兴趣、动态成员兴趣和可信身份策略共用协调入口。
- 以组件、topic、事件类型和 group 管理逻辑兴趣，同一需求由本地多个会话共享；不默认按 player_id 创建 SID。
- 主题加入改为有界异步：预留预算、共享或建立订阅、确认就绪、按会话代次复验后激活成员；失败只撤销本次引用。
- 退出先使本地成员失效，再释放最后一个动态兴趣；固定兴趣单独计数。在途事件最终重验成员，旧会话清理不能影响新代次。
- 按服务运行模型处理宽限期、恢复和交接准备/取消/提交，限制建立中操作、主题、SID、命令与缓冲预算。

**验收：** 多人共享一个主题兴趣、不同主题隔离、最后退出释放、加入失败/取消回滚、旧代次迟到完成、关闭清理、恢复就绪和交接预算均通过；真实多网关的同主题成员均能接收。共享 group 示例必须有正确目标路由，不能仅查任意网关本地成员。

### P6 已形成的落点

- 通用 interest::Pool 与 Transport 适配按组件和具体 Topic 合并引用，保留模板的类型、group、身份和能力；NATS 复用同一订阅实现，不引入平行业务处理协议。
- 固定与成员兴趣分别持有 Lease，等待中的引用计入资源，最后释放后有界退出；同键新获取等待旧消费清理，并发取消只撤销自己的引用。
- Ingress 显式选择跟随成员变化的组件，弱绑定避免 Pool → handler → manager 的强引用环。消费者由公共任务范围监督，不创建独立运行时或无界命令队列。
- join_topic / leave_topic 改为异步：等待多个组件就绪、最终代次和状态复验、先失效后释放，失败回滚当前预留；既有 FIFO、事件、下行校验保留。
- 初始主题策略在可信身份建立后执行，登录开放前通过同一加入流程完成；demo 以此订阅区服，指南增加聊天主题及独立固定引用示例。
- 保留逻辑会话宽限期与冻结期间的兴趣，恢复前检查就绪；交接目标提交前建立兴趣，取消撤销目标预留，提交后源端关闭释放。只迁移主题名，不迁移 SID/引用/future，冻结窗口仍可能丢即时事件。
- Pool 限制组件、兴趣、等待及期限，公共任务范围限制排队与运行；NATS 新增建立操作/探测 SID 容量，SDK SID 上界明确为 max_subscriptions + max_pending_subscriptions。

### P6 验收记录（2026-10-20 Windows）

工作区 243 项测试通过（含 12 项指南示例及 2 项编译失败文档测试）。新增 7 项兴趣协调测试、3 项会话/策略测试、2 项交接测试与 1 项指南示例，覆盖并发建立共享、取消等待者、固定引用保留、容量/超时/失败回滚、旧订阅清理后再建立、后台故障上报、Pool 弱持有、等待加入时退出/重连、多个组件部分失败、宽限期与同网关恢复、登录前初始策略、目标就绪后才提交及失败取消。

真实 NATS 15 项专项通过（通用事件 13 项、双网关 2 项），新增多人共享动态主题、跨网关独立接收、不同主题隔离、最后退出与固定兴趣保留；既有代理测试补充建立操作/探测 SID 容量和失败释放。真实 Redis 发现 2 项及发现双进程/共享所有权/三进程交接 3 项通过，共 20 项外部专项。events、execution、lifecycle、RPC、客户端和 NATS 六个 smoke 通过；最后的弱持有调整另通过完整工作区和相关 NATS 回归。

全目标 Clippy、workspace rustdoc、格式、UTF-8/LF 和仓库内文档链接检查通过。示例已使用可信初始区服策略，指南演示聊天兴趣与独立固定引用。P6 已提交为 `06d9dd2`；下一批为 P7 完整迁移清理与汇总验收。未运行 release 构建或 Linux 本地验证，不将 Windows 结果替代 CI。在线成员为零时退订的可选优化未提供；当前保留宽限期与冻结期间的兴趣，不增加即时事件补收承诺。

## P7 全面迁移与验收

**目标：** 用户可以仅通过“服务 + 处理链 + 扩展”理解和构建普通服务及 gateway。

**工作：**

- 完成 demo、公开导出、配置组装、测试辅助设施及所有入口的迁移，删除被替代的公共执行模型与兼容别名。
- 清理仅服务于旧抽象的转换与重复 Layer，检查命名、文件归属、参数、字段和缺失文档；不是把协议专用结构全部合并成一个大枚举。
- 用户指南使用真实 API 演示普通 RPC/事件服务、gateway 扩展、消息/事件 Layer、本地状态借用、聊天及区服主题。
- 更新实现状态与计划，记录实际验证平台、外部依赖、失败边界和未完成事项。

**验收：** 无平行生命周期/事件中间件体系、无旧入口残留；常规检查、文档示例、真实集成和示例构建通过。不能因一条 smoke 成功而标记所有批次完成。

### P7 已形成的落点

- Application::register_rpc_factory 按配置创建并注入一次性 RPC 绑定，实际状态和处理链仍由共同 Factory 初始化；未安装处理链不发布。bind_rpc 支持自定义 protobuf 按服务声明，启动前统一核对共享端点，保留完整手动端点组装的独立用途。
- ServiceConfig.endpoint 可省略，纯事件/后台服务复用同一监督与退出，不伪造地址或发布发现。注册校验失败不残留部分工厂/绑定，支持同一工厂在不同端点创建多个身份。
- demo 移除按服务名维护的 RPC 绑定表、端点拼接循环及占位生命周期类型，改用共同初始化入口；初始化绑定从执行扩展中排除，避免自身引用环。
- 新增 application-smoke，实际组合普通 RPC 与无端点事件服务，证明同一 Layer 处理远程请求及本地借用事件；与现有网关/聊天/区服、生命周期、执行及事件示例构成完整使用路径。
- 用户指南清除阶段原型和预初始化 handler 示例，当前状态压缩为能力/边界，历史验收保留在历史与本计划；CI 加入应用组合、Redis 请求锁和真实 NATS 专项。
- 保留协议适配所需的 CallHandler / TowerHandler / Executor，以及手动端点声明，它们不是兼容别名或平行执行系统；不为了清理统一成丢失语义的大枚举。

### P7 验收记录（2026-10-21 Windows）

工作区 249 项测试通过，包含 12 项指南示例及 2 项编译失败文档测试；全目标 Clippy、格式检查和 workspace rustdoc 通过。新增覆盖无端点服务生命周期、工厂漏装处理链、RPC 声明冲突与原子校验、同工厂多身份及初始化绑定引用环释放。

本轮真实 NATS 15 项、Redis 发现 2 项、所有权 5 项、请求锁 5 项及发现双进程/共享所有权/三进程交接 3 项，共 30 项外部专项最终通过。共享所有权多进程测试首轮在等待释放时发生 Redis 查询超时，单独复跑通过；未定位超时根因，不隐去首轮失败，也不因此增加生产重试或放宽超时。应用组装、execution、lifecycle、events、启动退出、RPC、客户端与 NATS 八个 smoke 通过。

release 示例构建通过；release 二进制对全部 8 份示例配置校验通过，RPC 与 TCP 客户端 smoke 通过。UTF-8/LF、仓库内 81 处 Markdown 链接及锚点、git diff 空白检查通过。CI 加入应用组合、Redis 请求锁及真实 NATS 专项；未在本地运行 Linux，不将 CI 配置视为已通过结果。

P1–P7 方向校正完成本轮验收，P7 随本次提交交付。后续功能仍按明确边界另行设计，当前不提供吞吐或延迟压测结论。

## 验证与交付

| 变化范围 | 必要验证 |
| --- | --- |
| 上下文与 Layer | 类型化输入/输出、借用与取消、预算、顺序、错误短路、身份来源不可伪造 |
| 构建与生命周期 | 初始化就绪、共享监听、失败清理、任务容量、停止与结束等待、Drop 与有界退出 |
| RPC / 事件适配 | 正常与业务失败、坏包、类型隔离、追踪、原始编码共享、无自动重试 |
| gateway / session | 同玩家 FIFO、timer、下行序号/ACK、缓存、顶号、同网关恢复、交接/取消及旧身份拒绝 |
| 动态兴趣 | 多人合并、首次失败回滚、最后退出、固定兴趣保留、迟到完成、断线/恢复/交接和资源预算 |
| 集成与文档 | 多网关 NATS + TCP/RPC、真实 Redis 回归、rustdoc 示例、LF、内部链接、release 示例构建 |

常规入口为 `just verify`、`just docs`；指南示例可单独用 `cargo test -p tok --doc --locked`。NATS 用 `just nats-test nats-smoke`，Redis 依受影响能力选择 `redis-test`、`ownership-test`、`locks-test`、`redis-smoke`、`ownership-smoke`、`handoff-smoke`；构建用 `just build`。外部专项按依赖执行并如实记录，使用独立命名空间，不清理共享数据。

验证与变化规模匹配，每批检查受影响的边界。完整矩阵在 P7 汇总，不能把历史结果当成本轮新结果，也不为无行为变化反复运行全部外部专项。Windows 和 Linux 结果分别记录，本地成功不能替代 CI。

每批形成实现、测试、示例与文档共同的可审阅变更。是否提交、推送按用户当次授权执行；本计划不自动授权提交。首批完成后用实际样例修正后续 API 细节，不以临时编译限制降低已确认需求。

## P7 后续 crate 边界校正（2026-10-21）

本轮不重新编号 P1–P7。新增 tok-player 归集玩家会话、消息、计时需求、所有权与恢复凭证契约；tok-core 保留通用基础，tok-service 保留执行、生命周期与任务监督。RPC 专用 CallContext 下沉到 tok-rpc，调用者迁移到新路径，不保留兼容别名。通用计时触发来源留在 core，不包含玩家消息或调度策略。

同步迁移网关、RPC、基础设施后端、测试与 demo；tok 门面导出 player，公开所有权路径按 Rust 模块风格提供 ownership::Store / Owner / Record。玩家专用错误从 core 移除，通用错误在 player::Error::Core 中保留。本轮已完成验收，随本次提交交付。

本轮 Windows 验收：`just verify docs` 通过，含 249 项工作区测试（12 项指南示例、2 项编译失败文档测试）、全目标 Clippy、格式检查和 rustdoc。application、execution、lifecycle、events、RPC、TCP 客户端及 NATS 七个 smoke 通过；真实 Redis 所有权 5 项及共享所有权/三进程交接 2 项外部专项通过。Cargo 依赖核对确认 tok-core 不依赖玩家契约，tok-service 与 tok-player 均只引用 tok-core 这一工作区基础库，tok-events 不依赖 tok-player。UTF-8/LF、内部文档链接及空白检查通过。按用户要求未做 release 构建；未运行 Linux、完整 NATS 专项或与迁移无关的发现/请求锁外部专项。本轮随本次提交交付。

## 本轮不并入的工作

JetStream、离线玩家加载、通用跨节点去重、自动玩家归属扇出中继、Redis Cluster、Kubernetes 与生产安全、Scene 接入、配置式中间件语言、存储实现及无缝场景切换保持后续事项。现有租约尽力保护、超时不回滚、无自动业务重试和客户端协议边界继续生效；不为统一模型增加严格 fencing 或分布式事务。

后续能力必须建立在本轮统一核心上，另行形成设计、计划与验收，不沿用已被替代的阶段编号推进。
