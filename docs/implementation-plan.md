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

本轮不重新编号 P1–P7。新增 tok-player 归集玩家会话、消息、计时需求、所有权与恢复凭证契约；tok-core 保留通用基础，tok-service 保留执行、生命周期与任务监督。RPC 专用 CallContext 下沉到 tok-grpc，调用者迁移到新路径，不保留兼容别名。通用计时触发来源留在 core，不包含玩家消息或调度策略。

同步迁移网关、RPC、基础设施后端、测试与 demo；tok 门面导出 player，公开所有权路径按 Rust 模块风格提供 ownership::Store / Record，持有者复用 core::Identity。玩家错误不经 core 聚合；所有权和凭证分别提供所属领域的错误类型。玩家运行契约分库已完成验收并提交为 `9f32c91`。

本轮 Windows 验收：`just verify docs` 通过，含 249 项工作区测试（12 项指南示例、2 项编译失败文档测试）、全目标 Clippy、格式检查和 rustdoc。application、execution、lifecycle、events、RPC、TCP 客户端及 NATS 七个 smoke 通过；真实 Redis 所有权 5 项及共享所有权/三进程交接 2 项外部专项通过。Cargo 依赖核对确认 tok-core 不依赖玩家契约，tok-service 与 tok-player 均只引用 tok-core 这一工作区基础库，tok-events 不依赖 tok-player。UTF-8/LF、内部文档链接及空白检查通过。按用户要求未做 release 构建；未运行 Linux、完整 NATS 专项或与迁移无关的发现/请求锁外部专项。本轮随本次提交交付。

## 执行上下文归属校正（2026-10-21）

将 Context / Source / StopReason 从 core 移至 tok-service 根导出，与 Request 和执行 Layer 聚合；Extensions 独立为 tok_core::Extensions，TimerTrigger 保留在 core。移除 tok-core 的 tokio-util / tracing 依赖，迁移原上下文测试、所有调用者与 rustdoc 示例，不保留旧 execution 路径。不增加抽象 trait 或新 crate，不修改执行行为。本轮已完成验收，随本次提交交付；按用户要求不做 release 构建。

Windows 验收：`just verify docs` 通过，含 249 项工作区测试（12 项指南示例及 2 项编译失败文档测试）、全目标 Clippy、格式检查与 rustdoc。application、execution、lifecycle、events、RPC、TCP 客户端和真实 NATS 转发/广播七个 smoke 通过。依赖树确认 tok-core 只依赖 serde 与 thiserror，tok-player 不再通过 core 引入 tokio-util 或 tracing。198 个仓库文本文件 UTF-8/LF、83 处内部 Markdown 链接与锚点及 git diff 空白检查通过。按用户要求未做 release 构建；本轮未重复 Redis、完整 NATS 专项或 Linux 验证。改动随本次提交交付。

## 请求锁模块命名校正（2026-10-21）

将只剩请求锁契约的 tok-core::coordination 改为 tok-core::lock，并按模块限定名提供 Key / Lease / Backend / Operation。迁移 Memory/Redis 后端、RPC Layer、demo 和指南；实现侧必要时使用明确的本地导入别名区分自身 Lease 或 Backend 类型，不提供公共兼容别名。不改变锁算法。本次已完成验收，随本次提交交付；按用户要求不做 release 构建。

本次 Windows 验收：15 项常规锁测试（Memory 4 项、Redis 参数/不可达 1 项、RPC 锁 Layer 10 项）、12 项指南文档测试与 5 项真实 Redis 锁专项通过；全目标 Clippy、workspace rustdoc、格式、UTF-8/LF、83 处内部链接与锚点及空白检查通过。未在这次命名调整后重复完整工作区测试或其他外部专项；前一轮上下文迁移的 249 项测试与七个 smoke 验收记录保留。按用户要求未做 release 构建；两项调整均随本次提交交付。

## 后端操作命名校正（2026-10-21）

锁及所有权契约中的 Future 别名由 Task 改为 Operation，公开路径为 tok_core::lock::Operation 与 tok_player::ownership::Operation，明确区别于受监督任务。迁移后端、自定义实现测试及指南，不改变执行行为或引入公共兼容别名。此项已完成验收，随本次提交交付；不做 release 构建。

本次 Windows 验收：21 项相关常规测试（内存所有权/路由 5 项、Memory 锁 4 项、Redis 所有权与锁的参数/不可达各 1 项、RPC 锁 Layer 10 项）及 12 项指南文档测试通过；全目标 Clippy、workspace rustdoc、格式、UTF-8/LF、内部文档链接与空白检查通过。此次仅修改 Future 别名，未重复完整工作区、真实外部专项或 smoke；此前上下文和锁模块调整的验收记录保留。未做 release 构建，改动随本次提交交付。

## core 职责与命名复核（2026-10-21）

复核 tok-core 的模块与 trait：发现目录读取、注册写入与变更通知分别承担缓存查询、控制面发布及合并通知，拆分合理；路由只选择目标，锁只提供后端契约，均应继续独立。service::Identity 不携带地址，ServiceDescriptor 描述网络服务，两者不强制合并。Extensions 和通用 TimerTrigger 保留在 core，错误类型描述契约失败，core 仍只依赖 serde / thiserror。

ServiceDirectory / ServiceRegistry / ServiceRouter / ServiceDescriptor 是根导出的公开角色名，前缀有辨识用途，不因统一模块风格而批量删除。Task 与 Operation 是语义选择，不是 Rust 强制规则；锁、所有权及发现的 Future 别名最终统一为 BoxFuture，具体迁移与验收见下节；不因角色名前缀而批量改动根导出的 trait。此次仅修正 DirectoryUnavailable 将后端误指为内存实现、NotImplemented 将组件能力缺失误写为工程骨架的说明与错误文本，不改变错误变体、分发或返回语义。

本次不继续批量重命名 trait 或根导出；错误文本调整运行 core 编译与格式检查。未做 release 构建，既有改动随本次提交交付。

## BoxFuture 统一（2026-10-21）

在 tok-core 根提供 BoxFuture<'a, T>，T 为完整输出类型，不隐含 Result。锁、所有权与发现统一引用此别名，显式表达 core::Result 或 player::ownership::Result；移除各模块的 Operation 和 RegistrationTask，包括发现变更通知中重复写出的 boxed Future 类型。根导出的 ServiceDirectory / ServiceRegistry / DirectoryWatch / ServiceRouter / ServiceDescriptor 保留，不增加兼容别名或第三方依赖。此项已完成验收，随本次提交交付；不做 release 构建。

本次 Windows 验收：tok-infra 与 tok-grpc 的 40 项常规测试、12 项指南文档测试通过；所有目标编译、全目标 Clippy、workspace rustdoc、格式、198 个文本文件的 UTF-8/LF、83 处内部链接与锚点及空白检查通过。锁、所有权、发现及自定义实现的错误类型保持原样，未新增第三方依赖。未重复完整工作区、真实 Redis/NATS 专项、smoke 或 Linux 验证，按用户要求未做 release 构建。此轮改动随本次提交交付。

## core 错误契约校正（2026-10-21）

按接口语义核对 Error 的覆盖面，保留共享的基础契约类别，不扩充为整个工程的错误全集。LockTimeout / LockCapacity / NotImplemented / DuplicateService / DirectoryUnavailable 分别改为 LockWaitTimeout / LockCapacityExceeded / Unsupported / RegistrationConflict / DirectoryStateUnavailable。保留已有 RPC 状态码映射，补全注册冲突中的节点/服务身份说明，发现失去租约归 DiscoveryUnavailable，注册容量耗尽归 RegistrationCapacityExceeded，不再冒充新注册冲突。

同步迁移调用者，增强既有内存/Redis 测试的错误分类断言，并在 core rustdoc 列出各接口主要错误类别。未知的具体后端故障通过已定义类别表达，不引入 Redis 专有变体；玩家、RPC、游戏业务错误继续由所属 crate 管理。本次已完成验收，随本次提交交付；不做 release 构建。

Windows 验收：infra/RPC 的 40 项常规测试与 12 项指南测试通过；真实 Redis 锁 5 项、发现 3 项通过。发现测试增强注册冲突/续租失效分类断言，新增容量耗尽、重复身份优先识别及撤销后恢复注册的边界用例；最终全目标 Clippy、workspace rustdoc、格式和空白检查通过。错误覆盖面按契约语义维护，不以 Rust enum 的穷尽匹配冒充外部故障覆盖证明。未重复完整工作区、其他外部专项、smoke 或 Linux 验证；未做 release 构建，改动随本次提交交付。

## 公开 API 规则落地（2026-10-21）

将有官方依据的公开 API 组织规则写入 AGENTS.md，明确根重导出与公开模块可并存，不以形式统一删除前缀或路径。按规则审查 tok-core/lib.rs：保留 discovery / lock / routing / service 功能命名空间，将 error 设为私有，错误覆盖说明迁到公开 Error 上，补充 Identity 根入口；Extensions / TimerTrigger / BoxFuture 维持根入口。同步设计、指南与状态，检验公开导入、rustdoc 链接和文档示例。本次已完成验收，随本次提交交付；不做 release 构建。

本次 Windows 验收：全目标 Clippy、workspace rustdoc、格式与 12 项指南文档测试通过；指南验证 Identity 根导入。检查生成的 tok-core 文档确认根页提供 Identity、私有 error 模块不作为公开入口，公开 Error 页保留完整分类表。198 个仓库文本文件 UTF-8/LF、84 处内部文档链接与锚点及空白检查通过。本次仅调整公开入口与文档，未重复完整工作区、外部专项或 smoke；未做 release 构建，全部改动随本次提交交付。

## 本轮不并入的工作

JetStream、离线玩家加载、通用跨节点去重、自动玩家归属扇出中继、Redis Cluster、Kubernetes 与生产安全、Scene 接入、配置式中间件语言、存储实现及无缝场景切换保持后续事项。现有租约尽力保护、超时不回滚、无自动业务重试和客户端协议边界继续生效；不为统一模型增加严格 fencing 或分布式事务。

后续能力必须建立在本轮统一核心上，另行形成设计、计划与验收，不沿用已被替代的阶段编号推进。

## tok-service API 与错误校正（2026-10-21）

保留调用执行、task 与 lifecycle 的职责边界及公开功能命名空间；Request 与同步 handler 移入私有实现模块，根导出不变。InitTask 更名 InitFuture，复用 core::BoxFuture；task::Task 保留监督任务语义。任务预留和提交改用 SubmitError，区分容量不足、停止、入口关闭及名称非法；任务与服务错误的 Task 变体改名 Join。执行 Error::Inner 通过标准 source 暴露原始错误，任务 panic 时完成报告保留原名称；修正通用任务范围、初始化取消及 256 字节名称限制的中文文本。调用方与 rustdoc 指南同步迁移，不保留旧名称兼容别名。本轮不做 release 构建。

本轮 Windows 验收：工作区常规测试（含 12 项指南示例、2 项编译失败文档测试）、全目标 Clippy（警告视为错误）和 workspace rustdoc（警告视为错误）通过；新增标准错误链、提交失败容量释放及多任务 panic 名称保留回归测试。真实 Redis/NATS 专项未运行；本轮随本次提交交付。

## RPC 协议适配分层校正（2026-10-21）

通用 tok-grpc 保留 RpcClients / Listener / Server，通过泛型请求和 Tower 接入任意生成的 gRPC 服务；正常依赖移除 tok-player / tok-events / tok-proto。新增 tok-message 承载游戏 CallContext、Handler / HandlerRegistry、玩家与普通业务协议客户端、绑定槽、通知编码及 timer / 事件 / 请求锁适配。门面从 message::rpc 访问游戏协议，grpc 访问 gRPC 设施；移除通用 CallHandler 上的事件契约，不提供旧路径兼容别名。Application 的普通自定义 protobuf 绑定继续使用通用端点，游戏 register_rpc_factory 使用新协议适配。客户端 Frame、内部 protobuf、事件 topic、timer、租约和无自动重试语义保持原有约束；事件与 timer 入口仍仅供可信服务端。

本轮 Windows 验收：工作区 255 项测试通过（包含 12 项指南示例和 2 项编译失败文档测试），31 项外部专项跳过；通用 RPC 新增 3 项无游戏类型执行测试，游戏 RPC 的事件入口隔离、身份、timer、共享连接、追踪及请求锁专项通过。全目标 Clippy、workspace rustdoc、application / lifecycle smoke、格式、UTF-8/LF、内部文档链接及空白检查通过。cargo tree 核对通用 RPC 的正常依赖不含玩家、事件或游戏 protobuf。不做 release 构建；未运行真实 Redis/NATS 专项或 Linux，本轮随本次提交交付。

## 调用执行与生命周期分库（2026-10-21）

原 tok-service 拆为 tok-execution 与 tok-lifecycle。tok-execution 保留 Context / Source / StopReason、Request、Tower Layer / Service、Call 和 SyncHandler；tok-lifecycle 根导出 Factory / Init / InitFuture、Builder / Prepared / Running、Handle / Phase / Error，task 模块保留 Supervisor / Handle / Reservation / Task 等通用监督能力。两库共同依赖 tok-core，彼此无依赖；tok-lifecycle 不依赖 Tower、RPC、事件或玩家协议。门面路径改为 tok::execution / tok::lifecycle，不提供旧 tok-service 或 tok::service 兼容路径。

应用、RPC、事件、网关、示例及指南使用新路径；服务启动就绪、任务容量、预算取消、退出等待及线协议语义不变。本轮不做 release 构建。

本轮 Windows 验收：工作区 255 项测试通过，31 项外部专项按约定跳过，包含 12 项指南示例及 2 项消费式生命周期编译失败文档测试；全目标 Clippy、workspace rustdoc、lifecycle / execution smoke、格式、200 个文本文件 UTF-8/LF 及 82 处内部文档链接检查通过。依赖树确认两库无相互依赖、tok-lifecycle 无 Tower 依赖。未运行真实 Redis/NATS 专项、Linux 或 release 构建；本轮随本次提交交付。

## 执行上下文移除固定来源（2026-10-21）

Context 移除 source 字段、Source 枚举和构造/child 的来源参数。执行核心只管理服务、操作、期限、取消、追踪和 Extensions。RPC、事件与网关在入口提供类型化扩展：复用游戏 CallContext、EventId / Topic / Arc<Message>、TimerTrigger，仅网关新增无正文的 ClientRequest 关联标识。RPC 关联信息与事件元信息可以共存，不再将传输方式、触发原因和客户端入口混成互斥枚举。

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

core 根校验错误的归属迁移已实施，最终落点见下文“移除 core 根校验错误”。

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


## 执行上下文连续性校正

依据[连续性规范](service-runtime.md#执行上下文连续性规范)与[源码审查](status.md#执行上下文连续性审查2026-10-21)，按以下依赖顺序修正。这是 P1–P7 形成的处理链在真实边界上的校正，不重启旧阶段编号。以下四项均已实施，具体边界与验收见 status.md 的“执行上下文连续性修复”；此前 Layer 化的验收不能代替本轮端到端验证。

1. **通用 gRPC 出站承载（已实施）。** 使框架出站 Layer / Service 与便捷入口接续 execution::Request<T> / Context，协议 Call / PreparedCall 只表达传输所需内容，避免重复保存可漂移的预算与追踪。已有范围使用原截止点和本地配置共同限制，根调用显式建范围；Context.service 保持本地身份。若重复的输入转换需要公共辅助方法，在 tok-execution 提供保留 Context 的转换，具体 API 以实现审查为准。验收外层策略到发送器的自定义扩展、取消、排队和准备耗时、剩余 grpc-timeout、借用及完整回应。
2. **游戏 RPC 与网关执行接续（已实施）。** 改造 business / player 客户端、普通 call 与内部事件编码和 RpcExecutor 的请求/事件/timer 三条路径。协议 CallContext 与执行 Context 保持分工。验收从网关执行中间件到本地出站策略的扩展保留、原预算和追踪；真实网络对端仅重建允许元数据与自身能力，不取得源 SessionHandle。
3. **网关调度、事件投递与下行范围（已实施）。** 校正 admission 输出、RequestJob / EventJob、deliver 与新下行适配，显式定义准入、已接受工作和下行发布是否属于同一范围，以及何时派生新范围。同一范围队列携带 Context 并扣减等待时间；独立范围声明新的有限预算、停止关系和扩展筛选。验收准入标记抵达执行、队列过期不执行业务、断线后的在途收束、消费处理到通知的范围、独立事件入队后不依赖发布者存活、fan-out 目标能力正确和补发不重复执行中间件。
4. **内部交接 RPC 与事件发布边界（已实施）。** GatewayRpcService 接入标准服务端执行适配，协调端复用出站承载；预算取现有调用与交接限制的较小者，必要补偿保持独立有界并受监督。NATS 原生出口外提供显式的执行发布适配，继承当前本地范围约束并提取允许追踪；订阅消费仍独立。验收四个交接接口的 metadata / Context、原状态机失败恢复、取消后的必要补偿、本地发布取消/超时和独立消费能力，不新增 broker 确认、重试或远端取消承诺。

每项实施后更新 status 与 rustdoc 指南，仅将已提供的 API 写入可运行示例；新增回归围绕边界行为，不用与实现逐行对应的测试替代契约验证。既有发现、所有权、队列容量、会话接管、错误映射和借用支持保持回归。本轮同时修复注册便捷回调剥离 Context 的额外缺口，旧多参数回调已迁移；继续按实际边界行为判断契约是否满足。


上下文连续性校正四项已实施并通过验收；本轮还统一了 protobuf / message 便捷注册回调。工作区串行 306 项通过、32 项外部专项默认跳过；隔离真实 NATS 的 16 项专项及发布跨任务回归通过，最终 execution / grpc / gateway 的 143 项回归与准入 13 项复核通过。Clippy、rustdoc、五个 smoke 和文本/文档检查通过。详细环境边界见 status.md；本轮修复随本次提交交付，不宣称已提供远端取消、事件确认或通用熔断策略。


## Registry 路由 Service 校正（2026-10-21）

本批已实现三项：通用分发表显式取键的 into_service 适配、protobuf / message 直接 Service 入口、普通业务 RPC 与本地示例接入及 rustdoc 指南同步。分发表是路由末端，公共 Layer 和路由专用 Layer 分别组合，不为形式统一新增 Registry Layer。

验收覆盖公共/路由策略的 Context 传递、未知键和原错误、停止前不取键、路由就绪与非协作式取键的原预算，以及注册后的快照隔离；工作区串行 311 项通过、32 项外部专项默认跳过，Clippy、rustdoc（19 项指南示例）、dispatch / rpc / client smoke、格式及 UTF-8/LF 和文档链接检查通过。环境边界与详细结果见 status.md；本轮随本次提交交付。

## Request / Response 扩展分离校正（2026-10-21）

本批交付：Context 与输入扩展分离、独立 Response 成功封装、请求/结果转换与拆装、各注册器和协议适配迁移，以及 gateway 准入/FIFO/投递结果接续。Request / Response 不依赖 ContextError，Context::child 与能力转移明确分工。泛型保护中间件不强制包装结果；生命周期与后端契约保持各自类型。

验收已覆盖输入能力穿过转换/排队、输出元信息经内层处理到外层策略、编解码后的双向扩展、事件聚合冲突规则、独立范围、传输边界和原始 Err。AGENTS、运行模型和 rustdoc 指南已同步；工作区 317 项、真实 NATS 16 项、Clippy、rustdoc 和六个 smoke 通过，详细结果见 status.md。不增加线协议兼容层或自动重试。

## Registry trait 与写时复制校正（2026-10-21）

本批交付 Extensions 写时复制、Registry trait / 默认 Table / Handler / 泛型路由 Service，以及 protobuf、message、普通 RPC 与玩家适配器的自定义注册器注入。公开 API 和 rustdoc 示例同步迁移，不保留旧 execution::Registry 结构别名，不新增动态管理或重试系统。

验收以自定义实现穿过真实适配器为准，覆盖关联类型及注册错误、自定义键与动态可见性、拦截/重定向、trait 对象、停止和预算、编解码及输出扩展；默认实现和原玩家 timer/事件边界保留回归。工作区 323 项、Clippy、rustdoc 和三个 smoke 通过，详细环境与验证结果见 status.md。


### 执行错误契约校正（2026-10-21）

execution::Error<E> 与 Request / Response 一起从根公开，统一表达 Stopped 与 Inner(E)；E 保留具体处理错误。
Registry 关联类型使用 HandlerError、DispatchError 与 RegistrationError；dispatch 和路由 Service 均返回
execution::Error<G::DispatchError>。默认 Table 的 DispatchError 仅包含 Unknown / Handler，不重复存放执行停止。
protobuf / message 适配保持这一层结构；自定义 RPC 注册器显式提供 DispatchError 到 Status 的映射，
默认构造保留默认映射。Error::map_inner 转换具体错误，Error::flatten 接续重复执行保护，
停止原因、错误来源与请求/响应扩展保持不变。原生 Tower Layer 仍可适配任意内层错误，
不要求第三方 Service 或后端采用共同返回类型。

本次错误契约校正已完成；工作区 326 项测试、Clippy、rustdoc 与三个 smoke 通过，验收详情见 status.md。


### gRPC 调用准备阶段命名校正（2026-10-21）

Outgoing 更名为 client::PreparedCall，根同时重导出 PreparedCall，不保留旧名称别名。
连接准备前使用 client::Call，客户端 Layer 准备 Channel 和元数据后向内层发送器传递 PreparedCall。
Prepared 只描述本地准备阶段，不承诺远端已连通或就绪；取消、预算与发送行为保持不变。

验收：tok-grpc / tok-message 共 51 项测试、21 项指南示例通过；全工作区全目标 Clippy、无警告 rustdoc、格式与空白检查通过，223 个文本文件 UTF-8/LF、119 处相对文档路径检查通过。未执行 release 构建。

模块聚合：Call / PreparedCall 共同位于 client/call.rs，不为同一调用概念的两个阶段单独拆分实现模块。


### 通用 gRPC 调用移除业务关联序号（2026-10-21）

Call / PreparedCall 不再携带 request_id；Client::execute / execute_service 移除关联序号参数，
通用失败日志保留目标、状态与关联追踪。业务协议中的 request_id、回应编号校验及玩家会话语义仍由
tok-message 等上层维护；可选本地关联信息使用 Request 扩展或 span，不自动传播到网络。

移除通用关联序号验收：全工作区串行测试 326 项通过、32 项外部专项默认跳过，包含 21 项指南示例；全目标 Clippy、无警告 rustdoc、格式与空白检查通过。222 个文本文件 UTF-8/LF、119 处相对文档路径检查通过，未执行 release 构建。
