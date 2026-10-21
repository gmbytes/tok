# 服务执行、事件与网关扩展设计

本文维护服务执行、上下文、中间件、事件及网关扩展契约，并说明当前实现边界。总体定位见[总体设计](design.md)，能力与验证见[实现状态](status.md)，方向校正的七批实施与验收见[实施计划](implementation-plan.md)。

## 当前落点与边界

| 范围 | 当前实现 | 边界 |
| --- | --- | --- |
| 组装与运行 | Application 复用 Factory / Init；RPC 按配置自动绑定，纯事件服务可无端点 | 端点、发现和处理链就绪分别检查，初始化失败不发布 |
| 扩展能力 | 网关、manager、session 身份、快照、状态工厂、停止和等待 | 不暴露内部锁，不将具体游戏状态放入通用核心 |
| 处理链 | Request / execution::Context / Tower Layer 覆盖 RPC、事件、timer 与网关业务 | 保留各入口的协议和结果边界、FIFO 及所有权检查 |
| 事件路由 | Event::topic 默认全局，可固定或由正文派生；NATS 负责安全 subject 编码 | group 属于订阅组件，不强制广播或提供玩家归属路由 |
| 主题兴趣 | Pool 按组件和主题聚合独立引用，异步成员操作，初始策略和交接就绪 | 逻辑会话宽限期/冻结保留兴趣，不补收即时事件 |

不把 topic、group、player_id 混为同一维度；不默认按玩家创建 NATS 订阅，不要求所有网关订阅环境中的全部事件。

## 框架核心与 gateway 定位

本批统一工作的根本目标是建立框架通用的“服务 + 处理链”基础设施。tok-gateway 是复用该设施的内置扩展实例，面向通用游戏运行时提供连接和 session 等能力；不是要求其他服务继承或依赖的核心基类。普通 RPC/事件服务、gateway 和游戏自定义服务使用相同的构建、上下文、中间件和生命周期扩展契约。

当前 tok crate 承担统一导出与 Application 组装，已经依赖 tok-gateway。共同设施必须位于二者可共同依赖的下层：通用基础契约归 tok-core，调用执行设施由 tok-execution 承载，服务构建、生命周期和任务监督由 tok-lifecycle 承载；玩家会话、消息、计时需求、所有权与恢复凭证独立归 tok-player。tok-player 只依赖 tok-core，tok-execution、tok-lifecycle 和 tok-events 不依赖玩家契约。禁止通过 gateway 依赖 tok 门面形成循环，也不在 gateway 内复制共同执行器来绕过分层。完整职责表见[框架核心与扩展分层](design.md#框架核心与扩展分层)。

构建时以普通服务和 gateway 两个最小样例验证：共同设施不导入 player/scene 或网关线协议；gateway 通过组合增加连接和会话能力；游戏从 tok 门面取得便捷组装入口，无需理解内部拆分。通用任务监督和生命周期由基础设施复用，各接入状态机继续承担自己的职责。

## 统一服务执行模型

RPC、客户端业务消息、服务器事件和 timer 都是触发服务执行的入口。事件处理必须经过与消息处理相同的中间件执行协议，不另建 EventMiddleware、RpcMiddleware 等相互独立的体系。统一的是服务执行核心，网络接入、订阅、会话调度和返回适配分别作为边界组件。

对业务使用者，服务由状态、入口绑定和处理链组成；处理链由可组合的 Layer 与末端 handler 组成，运行时负责初始化、就绪、任务监督和退出。RPC 和事件入口可任选或同时安装，不能要求每个服务必须同时提供两者。

```text
RPC / 客户端业务消息 / 本地或 NATS 事件 / timer
  → 入口适配：校验、解码、建立可信上下文
  → 调度：直接执行、session FIFO 或用户选择的实体互斥
  → 统一中间件协议 + 对应路由的业务 handler
  → 结果适配：RPC 回应、客户端回应或事件执行结果
```

服务级 Layer 可覆盖所有业务入口；路由级 Layer 按方法、事件类型或来源选择。两者使用相同协议与上下文，不要求每个入口安装完全一样的链或同一个中间件实例。服务工厂一次组装路由、状态和 Layer，入口适配器复用生成的处理核心；传输适配器不再分别实现一套鉴权、限流、日志或上下文注入机制。

业务鉴权可进入共享执行链，但分帧、可信身份建立、来源判定、协议边界和 player 所有权等不可跳过的校验仍由框架执行。session FIFO 属于调度约束，不能通过移除一个用户 Layer 绕过。准入层的限流与执行层的业务限流是不同安装位置，分别定义预算，不能悄悄重复计费或重复执行业务。

### 抽象执行上下文

采用与消息类型、protobuf 和 gRPC 无关的 Context。固定部分提供服务/节点身份、操作身份、追踪、剩余执行预算、取消信号和 Extensions。RPC 关联信息、事件 ID / topic、timer 触发及客户端标识由各层提供类型化元信息，可以同时存在；不使用互斥 Source 枚举，也不把它们解释为同一序号。

运行时能力通过类型化扩展提供：普通服务取得自己的生命周期、状态与发布能力；网关入口增加 gateway 和 session manager 句柄；会话内执行再增加对应 session 句柄。普通服务不强制依赖 player，也不把大量可选网关字段堆入公共上下文。业务状态和消息正文分别传递，生命周期查询能力不等于任意写入内部状态的权限。

上下文由当前进程的入口适配器创建。跨 RPC 或 NATS 只传播允许的身份、追踪、关联及预算元数据，不序列化任务/生命周期句柄、SessionHandle、扩展对象或取消令牌。远程服务建立自己的上下文，不能承诺直接访问源网关内存；需要源节点操作时使用显式内部接口。

### 操作身份、路由与熔断边界

`Context.service` 表示当前执行所属的服务；`operation` 是入口适配器按注册路由填写的稳定操作标识，在该服务内标识执行入口。RPC 方法、客户端消息、事件处理和 timer 各自映射到明确标识；不拼接玩家 ID、请求 ID 等动态值。字符串适应开放的业务入口集合；当前构造器只校验非空和长度，不能代替入口注册校验。

公共上下文不提供泛化的 `target` 字段。业务实体身份由有需要的入口通过类型化输入或扩展提供；实体身份、目标逻辑服务、路由约束和选定端点具有不同语义，不能复用一个字符串表达。扩展是承载机制，使用它的 Layer 仍须约定具体类型、作用域、必需字段及缺失时的错误；不能默默退回错误的默认目标。

路由输入包含目标逻辑服务及必要的分片键、区域等约束；路由结果包含选定节点和端点。具体类型由消费这些信息的路由设施定义，不先在 P1 增加未使用的通用结构。路由 Layer 根据类型化输入选择目标，再把结果交给后续传输及需要端点信息的 Layer。实体 ID 不必就是路由键，当前执行服务也不等于出站目标服务。

熔断 Layer 持有统计窗口、阈值和半开探测等状态，上下文只携带识别统计范围所需的信息。按逻辑服务/操作熔断可在路由前判定，按节点/端点熔断必须在选择目标后判定；错误分类须区分业务拒绝、调用方取消和下游故障，不能把全部错误直接计作下游故障。计数规则、拒绝结果与 Layer 顺序由实际设施明确，不由 `Context` 自动实现。

`Context.child` 仅派生当前服务内的执行，保留原期限、取消关系、追踪和扩展。未来出站调用设施须显式建立出站调用元信息，继承剩余预算、取消关系和追踪，并选择可传播的身份与元数据；接收服务通过自己的可信入口建立上下文。不得把全部扩展自动当作出站目标能力或远端元数据。

当前仅完成上下文边界及类型化扩展基础；尚未提供通用路由器、熔断器或出站调用上下文适配器，不将这些契约记为已实现功能。

### 处理与结果契约

以带 ExecutionContext 的类型化调用作为统一输入，handler 返回与调用种类匹配的类型化结果；Tower Layer / Service 是组合中间件的共同基础。通用日志、预算、业务鉴权和并发策略面向公共上下文；需要正文或特定输出的 Layer 通过泛型或路由约束声明适用范围，不通过全量转成 Vec<u8> 丢弃类型信息。

RPC 调用产生对应回应，事件产生当前 handler 的执行结果或允许的业务效果，timer 保留其内部结果约束。事件完成可以被日志和中间件观察，但不因此变成给客户端发送 Response，NATS 发布也不因此等待远端 handler。边界适配器负责把执行错误转换为 Status、客户端错误码或消费日志，通用执行层不依赖 tonic::Status。

事件“收到后转发进 session”和“player 实际执行”是不同的处理阶段。二者都使用共同的上下文与中间件协议，但分别声明入口身份与结果，不因为层间复用而执行两次同一个业务 handler。用户可分别给转发路由和 player 事件路由安装所需的链。

本地事件在已有调用中派生事件上下文，继承预算、取消和追踪，在当前任务执行同一事件处理链，不再次获取同一 session 门锁、重新排入自身 FIFO 或重置执行预算。是否继续执行当前调用后的其他本地处理者，沿首错停止规则处理。借用 &mut 业务状态的本地执行不能为接入某个要求静态生命周期的 Tower Layer 而被隐式移入后台队列；相关 Layer 必须明确其兼容范围，适配器与借用契约在实施首步验证。

构建与运行分别保持精简：构建阶段注册状态工厂、入口路由和 Layer，运行阶段由统一监督者启动所需适配器并管理停止和清理。事件专用注册表只负责事件类型分发、顺序和 fan-out，RPC 方法表只负责方法分发，二者不各自承担另一套生命周期或中间件引擎。P3 的 游戏协议 Handler 与事件 Handler 已成为各自边界适配，复用共同 Request / Layer；gateway Executor 的业务入口迁移留在 P4，不作为独立中间件引擎保留。

## P1 已验证的基础与适用边界

执行上下文使用 `tok_execution::Context`，同 crate 提供 StopReason，负责调用期限、取消传播及追踪范围。类型化能力容器独立使用 `tok_core::Extensions`，只依赖标准库。服务身份使用 `tok_core::service::Identity`，不携带网络地址。

实际执行设施放在 tok-execution，使用既有 Tower 协议，不增加平行 Handler trait：Request<T> 可携带 owned 输入或局部借用，Layer 返回 Service<S>，调用 future 保留真实输入生命周期。执行核心只依赖基础契约、Tokio、Tower、tracing 等通用设施，不依赖 gateway、RPC、事件或 NATS。tok 门面统一导出；gateway 能力在游戏示例的组装层注入，不形成反向依赖。

就绪等待放在预算 Layer 的调用 future 内；外层 poll_ready 只表示可以开始受保护的等待。原期限在派生调用中保留，取消在等待及执行期间唤醒并丢弃内层 future，非协作式处理者迟到返回也不能作为按时成功接受。此机制不回滚副作用，不等于进程级抢占或远端取消。

跨 await 的本地借用、同步 ready 适配、同一 Layer 对不同结果类型的使用、原业务错误及顺序首错已由原型测试验证。借用调用不能进入要求静态任务的 Buffer/spawn；owned 输入可在满足 Send 条件时提交运行时。每次调用克隆内层 Service，限额必须由相应 Layer 在克隆间共享，不能把外层立即 ready 误读为已有容量。

P1 只验证执行核心；P2 已提供下述工厂与生命周期设施。RPC/事件处理注册已在 P3 迁移，P4 已迁移 gateway 业务入口及扩展，固定 topic 路由已由 P5 完成，动态主题兴趣仍待 P6。

## P2 公共构建与生命周期落点

公共工厂契约为 `tok_lifecycle::Factory`；Init 提供服务 Identity、服务 Handle 和类型化 Extensions。工厂初始化业务状态、入口和 Tower Layer，返回 `tok_lifecycle::task::Task` 主任务。闭包可直接实现契约，不要求普通服务有网络端点，也不增加平行的中间件或 handler trait；P3 已接入统一事件注册与 RPC 处理链适配。

公共设施按职责分成两层：`tok_lifecycle::task` 管理通用任务范围，`tok_lifecycle` 编排服务生命周期。前者不接收工厂，不知道服务 Ready，也不决定主任务提前结束是否错误。后者驱动 Factory，监督启动及运行故障，管理服务阶段与主任务清理。

服务启动顺序固定为 Builder → build → Prepared → start → Running → run。各步消费前一个对象，不能重复初始化、重复启动或跳过初始化。初始化成功仍为 Starting；只有 Prepared::start 将主任务交给监督并推进 Ready，Running::run 不隐式改变就绪。若启动前已收到停止通知，阶段保持 Stopping，主任务仍获得协作清理机会。句柄查询阶段统一由服务启动标记及任务范围状态计算，用户不能直接修改阶段。

Application 通过 `tok::application::Binding` 扩展注入监听地址、发布描述和发现查询，继续管理共享端点、注册可见性及整应用停止。服务 start 后才开启 RPC 准入并发布；主任务在发布前已纳入监督，发布失败或取消仍有清理路径。初始化参数 Init 不包含可选 gateway/端点字段。

任务范围状态为 Open、Stopping、Stopped；服务阶段为 Starting、Ready、Stopping、Stopped。预留、排队和运行共用容量，used_capacity 统计占用名额，包含尚未提交的预留；初始化、主任务和另有容量的连接由跟踪机制覆盖。Supervisor::next 返回一次任务完成，服务编排层把后台错误/panic 或主任务提前完成视为服务失败，有限后台任务允许正常完成，不做自动重试。

任务范围只有消费式 shutdown(timeout) 退出入口；Some 指定期限，None 使用外层期限。成功返回意味着所有被跟踪工作和子范围已释放，wait_finished 已完成，不要求调用者额外 drop。超时返回 ShutdownTimeout，中止并回收当前监督者拥有的任务；外部跟踪 future 和子监督者仍须由其所有者回收，尚未释放时保留 Stopping。Drop 只发起取消和中止，通过 wait_finished 观察实际结束，不保证异步清理完成，也不能抢占不让出执行权的同步代码。

Supervisor::child 建立独立容量的子范围，父取消向子传播，父结束等待包含子范围回收；子组件负责拥有和驱动自己的监督者。Supervisor::track 跟踪另有容量/调度策略的工作。服务 Running::run 收束主任务清理后关闭任务范围，完成语义包含组件清理及子范围；工厂不得把子范围脱管到服务之外。

Gateway::new_in 接收 Init，继承所属服务身份、生命周期句柄和扩展，后台任务复用通用 Supervisor；连接仍由专用循环拥有，通过 track 纳入结束等待。Gateway::tasks 返回任务范围句柄，其结束不等于 ingress.finish 已完成。Gateway::handle 返回完整网关范围句柄，wait_finished 包含连接、后台任务及 ingress.finish；所属 lifecycle::Handle 还包含服务级清理。独立 serve 的整体期限由调用方提供；Application 运行时由应用期限控制。未启动或强制 Drop 只执行同步停止，不承诺异步清理。


### 配置驱动的应用组装

ServiceConfig 的 factory 可选，默认按服务名查找共同 Factory；endpoint 也可省略，无端点服务仍参与初始化、运行监督与退出，但不生成或发布网络描述。网络服务保留 application::Binding 能力，事件服务可由组装层注入发现查询或其他状态。

Application::register_rpc_factory 为匹配配置创建 message::rpc::Service 及一次性 message::rpc::Binding，按所属服务注入 Init.extensions，在同一工厂中安装真实状态、路由与 Layer。工厂结束时检查绑定是否完成，缺失安装会在发布前失败；初始化绑定不保存回自身执行扩展，防止引用环。

自定义 protobuf 通过 bind_rpc 按业务服务声明，与其他逐服务绑定在启动前合并并校验端点；高级调用可一次注册完整 Listener，同端点不混用两种方式。重复 protobuf、错误协议或缺失端点在监听前拒绝，工厂注册失败不留下半份声明。协议转换不会自动选择锁或中间件，普通服务仍是一套 Factory 契约。

## topic、事件类型与 group

| 维度 | 含义 | 示例 |
| --- | --- | --- |
| 事件类型 | 正文契约和处理逻辑 | `chat.message.v1` |
| topic | 发布与订阅共同选择的业务路由主题 | `chat:7`、`realm:42` |
| group | 匹配同一路由的订阅实例如何分担消费 | `chat.audit` |
| player_id | 正文中的业务目标；转发组件据此查找会话 | `4242` |
| 本地 Target | 处理组件收到事件后，实际向哪些会话投递 | Players、Topic、All |

NATS subject 是传输编码，包含 namespace、topic 和事件类型。具体布局集中在适配器内部生成和校验，不让业务拼接原始 subject。主题编码必须可逆或采用无碰撞映射；不得直接把用户字符串中的点、空格、`*`、`>` 拼入订阅表达式。默认作用域使用显式值，与同名普通主题区分；默认订阅不代表订阅全部主题。

P5 已为 Envelope / Message / EventEnvelope 及内部 EventRequest 增加一致的路由主题信息，接收时同时校验 subject、主题和事件类型。topic 不是消息正文中的 player_id，也不自动意味着客户端 Notify。处理组件可以把路由主题映射为同名本地 Target::Topic，也可以选择其他目标；此映射由组件显式声明。

无 group 的每个匹配实例各自接收；同名 group 的匹配实例分担事件，不同 group 独立接收。框架支持所有合法组合，不根据 gateway、player 或组件名称强制选组。NATS 的组内分担不提供玩家归属或跨网关扇出，参见 [NATS 客户端协议](https://docs.nats.io/reference/protocols/client) 和 [Core NATS 模型](https://docs.nats.io/learn/core-nats/)。

如果聊天主题在两个网关都有成员，直接本地广播的组件应让两个网关独立接收；将两个网关放进同一 group 只会让其中一个收到事件。若选择共享 group，组件必须能够完成所有目标的路由或扇出。当前批次提供组装能力和正确示例，不额外实现通用跨网关广播中继。

### P5 当前路由实现

Event::topic(&self) 默认返回 Topic::default，普通全局事件只需声明事件名。事件可覆盖为固定主题，或通过正文的频道/区服字段确定主题；必须是无 I/O、无副作用的确定性计算。Envelope::new 自动取得主题，不提供发布时任意覆盖接口，构造/编码及 Registry 解码均核对事件路由契约。自定义原始 Handler 绕过 Registry 时自行校验正文。

Topic::default 表示当前 namespace 内的默认作用域，Topic::named 创建最多 256 个 UTF-8 字节的具名主题，不作大小写或 Unicode 归一化，拒绝空名、空白、控制字符和通配符。Subscriber 显式接收 TopicFilter::Exact(topic) 或 All；All 包含默认和具名主题，不能用于发布。不提供部分主题通配表达式。

适配器生成 tok.events.<namespace>.<topic-token>.<event_type>：默认 token 为 d，具名 token 为 n 加 UTF-8 小写十六进制，无碰撞且只有一个 token；All 在该位置使用 *。接收时从正文恢复 Topic，并核对兴趣、类型及完整规范 subject。每个兴趣/事件类型占用一个 SDK 订阅与对应缓冲预算，All 不按实际主题数量展开。发布仍只确认客户端接受，订阅就绪仍使用原有有界回环。

本地 dispatch 显式调用注册表，保留相同的 Envelope.topic，不模拟 broker 的订阅筛选或 group；远程解码和内部 RPC 转发保留 topic。内置 forward / notify 的选择回调读取完整 Envelope，可显式把传输主题映射为 Target::Topic，也可按正文选玩家或其他本地目标。demo 固定订阅 realm:demo；会话主题成员不会隐式改变该兴趣。

## 订阅组件与动态兴趣

订阅组件声明处理表、事件类型、可选 group 以及主题绑定方式。主题绑定是可组合的输入，不使用固定“玩家/广播/自定义”组件枚举，也不把 NATS 依赖塞入 Gateway 或 SessionHandle。

支持三种组装方式：

1. 固定兴趣：服务启动时订阅明确的主题，与在线玩家数量无关。例如运维通知或该节点负责的区服主题。
2. 会话驱动兴趣：可信服务器调用会话主题 API，网关把本地成员需求汇总到所绑定的订阅组件。同一聊天主题的多个玩家复用订阅。
3. 策略驱动兴趣：游戏根据已验证的身份或业务状态计算会话主题，例如认证得到的 realm_id；通过同一成员 API 生效，不另建一套区服订阅逻辑。

同一会话可同时加入聊天、区服等多个主题。固定兴趣和动态兴趣可共存；本地主题成员登记不会自动绑定所有订阅组件，工厂显式选择哪些组件跟随这些成员变化，哪些仅处理固定主题。

区服信息由认证结果或可信游戏状态提供，框架不从 player_id、服务名或客户端上报字符串猜测。客户端仍不能直接修改传输订阅；游戏可以注册经过认证和权限校验的聊天加入请求，由服务器执行主题操作。

传输兴趣由网关级协调组件持有，按订阅组件、topic 和事件类型合并，并保留 group 语义。相同组件下多个会话对同一主题只增加成员引用，不重复创建 broker 订阅。不同组件、不同消费组的逻辑消费不能误合并成只执行其中一个 handler。

首个需求建立订阅并等待就绪；最后一个需求释放时退订。固定兴趣计入独立引用，不因最后一个玩家离开而误删。动态订阅按不同主题与事件类型的组合计数，而不是按 player_id 计数；若游戏刻意为每个玩家创建独有 topic，数量仍会随玩家增长，框架不承诺消除这种业务建模成本。

### 加入与退出的边界

涉及传输确认的主题操作为异步接口，不能在 ChannelState、索引或目录的同步锁内等待网络。P6 的 join_topic / leave_topic 已改为有界异步操作，本地索引变更由通道内部步骤承担。

加入流程：预留主题、SID 和缓冲预算，建立或共享订阅并确认就绪，再按完整会话身份和当前代次重新检查，最后激活本地成员并返回。并发加入共享建立中的订阅。建立失败、超时、会话关闭或旧代次迟到完成时撤销本次需求；不得撤销其他成员或固定兴趣。需要多个组件就绪时，全部完成后才能激活此成员关系；失败回滚本次预留，不回滚已经发生的外部业务。

返回成功表示绑定组件已具备接收能力，且当前会话完成成员登记；不保证加入生效前发布的事件被补收。订阅已就绪而成员尚未激活的窗口允许跳过即时消息，不额外增加历史缓冲。

退出先取消本地成员资格，再释放传输需求；即使退订失败或有在途事件，最终投递仍在会话锁内重验主题成员。关闭及替换按完整 session_id / epoch 清理，旧实例的迟到回调不能影响新会话。传输兴趣在途操作使用代次和引用状态协调；不默认增加延时退订、自动重试或重连编排。

离线宽限期保留逻辑会话主题，但不投递即时事件。内置在线成员兴趣模式在没有可投递成员时可释放动态订阅；恢复后必须重新完成兴趣就绪，再开放新业务。固定兴趣不受此影响。断线、重连与主题退出的语义分别保留，不能把暂时断线当成游戏主动离群。

交接只迁移业务主题和必要扩展快照，不迁移 NATS SID、引用数或临时任务。目标网关在快照准备阶段预留并建立所需兴趣，提交前完成就绪，但在取得所有权并开放会话前不向目标会话投递。源网关冻结时停止投递，取消交接时按原状态恢复；提交后释放旧会话兴趣。此过程仍允许 Core NATS 的冻结窗口丢失即时事件，不新增无损或跨网关去重承诺。

### 资源与失败

限制每会话主题数、组件数、活动主题/类型订阅数、建立中操作数、命令队列、缓冲字节及等待期限。不同主题、group 和临时就绪探测的实际 SDK 订阅都应纳入明确预算；不沿用“只有事件类型数决定订阅数量”的首版容量描述。

P6 的 Pool 通过调用方提供的 task::Handle 提交工作，由共同 Supervisor 或服务生命周期编排监督；停止新兴趣、取消建立中操作、清理本地引用及退订按正常退出流程收束。连接故障交给监督者，不自动补发事件。发布仍只等待客户端接受操作；就绪确认只发生在订阅建立阶段，不逐条探测发布。

### P6 当前实现边界

interest::Pool 持有组件模板并按组件名与 Topic 协调引用，固定兴趣和会话兴趣各自取得不可 Clone 的 Lease；drop 只释放自身引用，最后释放取消消费。新引用等待同键退出中的旧订阅清理，防止重叠消费。并发建立共享结果，单个等待者取消不撤销其他需求；建立失败返回调用者，活动消费失败交给监督者，不隐藏重启。

Ingress::with_topic_interests 指定跟随会话主题的组件，Pool 由 Ingress 拥有，manager 和 Lease 都使用弱绑定，避免经处理者、目录和会话成员形成引用环。Pool 使用已有任务提交队列及容量，不另建命令循环。初始化期获取固定引用应使用正在驱动的 Init.service.tasks，不等待尚未 serve 的 Gateway::tasks。

会话加入预留有界操作容量，在状态锁外等待所有绑定组件就绪，然后校验连接代次、取消、冻结和待加入版本后登记成员。退出先使成员与待加入版本失效，再有限等待释放。初始策略由可信服务器根据已认证身份及能力返回主题列表，在成功登录前完成同一加入流程；失败关闭新会话并释放所有权。

当前保留整个逻辑会话生命期内的兴趣，宽限期和源端冻结不退订；即时投递仍只向在线且未恢复/冻结的成员准入。同网关恢复前检查保留引用仍就绪；目标交接在提交前建立主题兴趣但不登记目标成员，失败撤销目标引用，成功后源端关闭并释放。在线成员为零时释放兴趣只是可选优化，本批未提供，不新增无损恢复或冻结窗口补收。

Pool 限制组件、兴趣（含建立/活动/退出）、等待操作及期限，任务提交容量由公共范围限制。NATS 的 max_subscriptions 限制实际 topic/类型订阅，max_pending_subscriptions 限制建立操作及对应临时探测 SID，因此 SDK SID 上界为二者之和，缓冲预算另行限制。

## 生命周期与查询上下文

事件处理者、生命周期扩展和消息中间件通过相同的运行时能力句柄获取信息，不要求持有或借用正在 serve 的 Gateway 对象。

| 范围 | 基本查询 | 生命周期操作 |
| --- | --- | --- |
| Gateway | 节点/实例身份、已绑定服务、运行阶段、连接与任务容量快照 | 请求停止、取消信号、结束等待、提交受监督任务 |
| SessionManager | 所属网关与服务身份、运行阶段、会话数量/容量、按 ID 查找与有界枚举 | 停止接受新会话、停止所属会话、等待收束；创建仍走认证流程 |
| Session | 完整身份、生命周期阶段、连接绑定/在线情况、主题成员快照 | 请求关闭、等待结束、连接变化通知、主题操作、既有消息提交与通知 |

网关能力通过类型化服务扩展提供，组合生命周期句柄、网关信息查询、SessionManager 和按需使用的 SessionHandle；不把这些能力堆入通用任务句柄。通用 Gateway 不强制依赖玩家模块。闭包不再需要自行猜测闭包捕获的是哪个 session 或哪个网关。

查询返回只读快照，不暴露内部 MutexGuard、发送队列、所有权存储或可随意修改的 ChannelState。查询与下一次操作之间可能发生变化，操作自身必须再次校验身份与阶段；跨多个 session 的统计不承诺原子快照。取消信号、停止请求和结束等待各自有明确含义，不能把 token 已取消视为清理已完成。

每个范围允许安装游戏自有的类型化扩展状态，由组装工厂创建，通过上下文按类型取得共享句柄。工厂负责限定扩展类型与作用域，避免把任意类型键值表作为协议或业务状态仓库。会话扩展在新登录时重建，同网关恢复复用；跨网关必须显式提供有界快照/恢复适配，否则准备交接时拒绝，不能静默丢弃必需状态。扩展不要反向强持有自身 SessionHandle 造成引用环。

生命周期覆盖创建/初始化、连接绑定变化、交接准备/恢复/取消和关闭收束。必须完成的资源创建放在有界、可失败的初始化步骤；主题策略也通过该步骤应用。普通通知不能替代必要资源的释放与注册撤销。正常关闭在收束任务后执行有界结束回调；强制取消或进程退出不承诺异步回调必达，资源仍须具备作用域释放路径。

回调不能持有框架同步锁跨 await。需要改变会话业务状态的事件与回调通过 session 串行入口执行；只读查询和独立网关状态按各自并发规则运行。初始化/关闭回调不得等待同一会话在当前步骤尚不可能执行的队列任务，避免自等待。

## 消息与事件中间件

消息、事件及 timer 共同采用上述执行上下文与中间件协议。沿用项目已有 Tower Layer / Service 思路，提供明确的上下文和安装位置，不建立另一套 before/after 拦截器体系。由用户在工厂中选择链、顺序和参数；先完成 Rust 代码组装，不同时引入配置语言。

| 安装位置 | 用户可以处理的内容 | 框架边界 |
| --- | --- | --- |
| 业务请求准入前 | 限流、业务准入、上下文补充 | 分帧、大小、版本及身份校验在前；不得把客户端请求改成内部事件或 timer |
| 服务执行链（包含 session 内执行） | RPC、本地/NATS 事件、客户端请求与 timer 的业务执行策略；共享 Layer 协议，可按路由安装不同链 | 任务先进入同一 FIFO，执行前所有权与剩余预算校验不可绕过；三种分发表及结果类型仍隔离 |
| 新下行提交前 | 业务回应与通知的观察和转换 | 大小和结果种类校验后才分配 server_seq、编码并写入缓存；补发直接发送缓存字节，不重复执行此链 |

中间件共享生命周期与查询句柄；session 执行上下文携带可信身份、来源、执行期限和取消信号。来源由框架入口生成，不能信任客户端或 NATS 正文自报的“内部来源”。限额、所有权、队列容量、客户端 ACK 与交接状态机属于框架约束，不交给用户中间件决定是否执行。

Layer 可以拒绝调用、调用一次下游并观察结果，不默认提供自动重试。异步就绪等待与执行计入对应阶段预算，取消和错误沿原入口收束。不得在准入层等待自身入队任务，或在同一 session 执行链内等待再次排入自身队列的任务。

认证仍通过 Authenticator 等专门入口扩展；本批业务消息 Layer 不替代登录、ACK、恢复凭证、交接等协议状态机。若后续需要改写控制协议，单独提出，不开放任意客户端帧转发到内部接口的通用中间件。

### P4 当前扩展实现

Gateway 构造闭包完成 Pipeline、状态工厂和自定义 RPC 等配置后，才调用 Ingress::initialize 注册最终 manager。不要在配置完成前导出 manager 句柄。manager 扩展与工厂创建的每会话扩展按作用域合并，执行上下文再注入实际三层句柄。能力是 Send + Sync 的共享句柄；本地 Registry 仍支持借用业务状态，静态安装的网关 Pipeline 使用拥有所有权的类型化输入，不暴露 SessionState 的可变借用。

session::extension::Factory 的 initialize / snapshot / validate / restore 为同步有界步骤；on_change 在框架锁外观察初始化、绑定、恢复、交接和关闭。回调应短小，不阻塞、不等待自身队列；panic 会关闭当前会话。通知中的连接代次标识发生变化的绑定，查询时可能已进入更新阶段。manager.wait_finished 等待已接纳的协调操作与会话任务，网关完整清理另由 gateway::Handle 等待。

准入、FIFO 执行、新下行各有独立阶段预算，阶段内的就绪与 Layer 等待共享预算；队列等待受容量限制。停止阻止新业务，已开始的执行按原预算收束，不因 session 停止立即中断。异步准入继承连接取消令牌，重连/顶号使旧提交失效；异步新下行最终再次验证连接、主题和状态。普通通知拒绝返回错误，回应无法发布时关闭会话，均不重新执行业务。

## 统一事件接口

统一到一套 Event、Envelope<E>、事件处理上下文、Registry 和 Subscriber 描述。local 与 nats 不再分别拥有不同的业务回调注册契约；本地分发器和远程解码适配都进入同一处理表。一个注册表支持多种类型和明确顺序的多个处理者，容量及重复组件标识有明确限制；同一类型的合法多个处理者不能再被“重复事件名称”一概拒绝。

Envelope<E> 为本地与远程共用的强类型封装，包含稳定事件 ID、topic、追踪信息和正文；构造器显式接收 EventId，可用 EventId::generate 生成新 ID，原事件转发保留 ID。Message 是编码后的通用表示，EventEnvelope 是线协议，不让本地事件为了复用回调而强制执行 protobuf 编解码。

处理上下文统一提供事件元数据、可信来源、运行时能力以及显式传入的业务状态。回调采用可借用当前上下文的统一异步契约；本地处理在当前任务中等待，不 spawn、不隐藏排队，也不为了满足静态生命周期而强制把 &mut 业务状态包装成 Arc<Mutex<_>>。同步处理函数通过适配注册，不额外增加同步事件系统。P3 已移除 local::Dispatcher，本地 dispatch 为显式 await，同步回调通过 sync_handler 适配，这是公开 API 与执行契约调整。

Registry 的本地类型注册不要求 prost::Message；远程订阅安装时提供 codec，默认支持 protobuf。回调可取得强类型 E；远程来源还可通过上下文取得原始共享 Message，转发保留原编码和追踪信息，不反复解码重编码。本地自定义 Rust 事件无需为未使用的远程路径实现序列化。

统一门面保留两个具有明确完成语义的操作，不把“本地执行完毕”和“远程发布被接受”伪装成同一个结果：

| 操作 | 语义 | 成功与错误 |
| --- | --- | --- |
| dispatch(context, envelope) | 在当前任务调用同一 Registry，依次执行本地处理者 | 等待处理完成，返回当前阶段 Outcome；首个业务错误原样返回，先前副作用不回滚 |
| publish(envelope) | 经所选传输适配器发布，接收侧仍进入同一 Registry | 只确认传输适配器接受操作，返回传输错误；不等待接收侧业务结果 |
| subscribe(subscriber) | 根据同一订阅描述绑定 topic、类型和 group，安装本地或远程接收适配 | 返回受监督的订阅生命周期句柄，主题建立按后端就绪边界返回 |

业务回调、上下文和注册只维护一套；dispatch / publish 是同一门面的显式投递方式。公开 API 的泛型、借用和 trait object 细节在第一步用最小 Rust 编译样例验证后定稿，不能退回仅名称相同而回调、封装不同的两套接口。业务错误类型与传输错误分别保留；不为统一而把本地业务错误全部改为静态字符串。

本地串行处理顺序由 Registry 定义，远程只承诺每个具体订阅的接收顺序，不增加跨 topic、跨类型或跨网关全局排序。group 是订阅选项，不属于事件契约；全局事件可以被无组组件逐实例处理，也可被同组组件分担，不改变本地 dispatch 的处理者执行顺序；本地不模拟分布式消费组。转发到 player 仍在 session FIFO 内调用处理表，不绕过现有串行与所有权约束。

生命周期通知可复用本地 Event / Registry，消息中间件复用相同运行时上下文，但职责不合并：事件通知不自动成为准入拦截器，中间件也不隐式发布生命周期事件到 NATS。

## 与实施计划的对应

具体顺序、批次状态和验收条件统一维护在[七批实施计划](implementation-plan.md)，本文只维护契约，不再保留另一套批次划分。

| 本文契约 | 实施批次 |
| --- | --- |
| 统一执行协议、借用上下文及 Layer 可行性 | P1 |
| 公共构建、生命周期与下层承载位置 | P2 |
| RPC、本地/远程事件共同处理核心 | P3 |
| gateway 扩展、查询/生命周期/状态与消息 Layer | P4 |
| topic 进入封装、协议和 subject | P5 |
| 动态兴趣、自动主题策略与迁移清理 | P6 |
| 示例、旧入口清理、文档与全面验收 | P7 |

JetStream、离线加载、全局去重、通用扇出中继、热配置、生产部署和任意游戏状态自动迁移不并入这七批。旧的固定类型订阅、本地主题成员和两套事件处理契约属于迁移前状态；P3 已统一执行契约，P5 已接通固定 topic 路由，P6 已进一步接通动态兴趣；P7 收敛应用组装与示例，不据此宣称后续能力已完成。

## RPC 协议适配分层校正（2026-10-21）

通用 tok-grpc 保留 Client / Listener / Server，通过泛型请求和 Tower 接入任意生成的 gRPC 服务；正常依赖移除 tok-player / tok-events / tok-proto。新增 tok-message 承载游戏 CallContext、Handler / HandlerRegistry、玩家与普通业务协议客户端、绑定槽、通知编码及 timer / 事件 / 请求锁适配。门面从 message::rpc 访问游戏协议，grpc 访问 gRPC 设施；移除通用 CallHandler 上的事件契约，不提供旧路径兼容别名。Application 的普通自定义 protobuf 绑定继续使用通用端点，游戏 register_rpc_factory 使用新协议适配。客户端 Frame、内部 protobuf、事件 topic、timer、租约和无自动重试语义保持原有约束；事件与 timer 入口仍仅供可信服务端。

## 调用执行与生命周期分库（2026-10-21）

原 tok-service 拆为 tok-execution 与 tok-lifecycle。tok-execution 保留 Context / StopReason、Request、Tower Layer / Service、Call 和 SyncHandler；tok-lifecycle 根导出 Factory / Init / InitFuture、Builder / Prepared / Running、Handle / Phase / Error，task 模块保留 Supervisor / Handle / Reservation / Task 等通用监督能力。两库共同依赖 tok-core，彼此无依赖；tok-lifecycle 不依赖 Tower、RPC、事件或玩家协议。门面路径改为 tok::execution / tok::lifecycle，不提供旧 tok-service 或 tok::service 兼容路径。

应用、RPC、事件、网关、示例及指南使用新路径；服务启动就绪、任务容量、预算取消、退出等待及线协议语义不变。本轮不做 release 构建。

## 执行上下文移除固定来源（2026-10-21）

Context 移除 source 字段、Source 枚举和构造/child 的来源参数。执行核心只管理服务、操作、期限、取消、追踪和 Extensions。RPC、事件与网关在入口提供类型化扩展：复用游戏 CallContext、EventId / Topic / Arc<Message>、TimerTrigger，仅网关新增无正文的 ClientRequest 关联标识。RPC 关联信息与事件元信息可以共存，不再将传输方式、触发原因和客户端入口混成互斥枚举。

事件 Registry 的本地与远端分发提供相同事件头，派生子上下文覆盖当前事件头，不修改父扩展；本地派生仍清除继承的原始编码。客户端无法发布事件、请求与 timer 分表、会话身份校验、预算和取消约束保持不变；Extensions 元信息不作为认证证明。调用方和指南已迁移，不保留 Source 兼容类型。

## 通用 RPC 期限与诊断（2026-10-21）

通用入口使用 tok_grpc::{Client, Listener, Readiness, Server}。Server 负责逻辑服务的执行范围，Listener 负责共享监听和协议绑定；内部实现模块不公开。

客户端配置期限须大于零且不超过一天，与服务端预算配置一致。execute 取请求 grpc-timeout 与默认期限的较小值，准备和发送共用截止点；无效超时返回 InvalidArgument，零期限或迟到结果返回 DeadlineExceeded，不自动重试、不回滚副作用。直接使用 channel 的适配器须自行管理预算；Channel 延迟建连，不承诺获取时已连接。

Readiness::set 返回是否匹配已绑定服务，允许 Application 向多个端点广播状态更新；直接指定服务时应检查命中。空服务名、重复业务绑定和重复 protobuf 绑定分别报告配置原因。Error 保留 Configuration / Transport / Io 分类，传输与监听文本提供中文操作说明且保留 source；运行期继续使用 tonic::Status，不新增业务错误码。

监听接口 bind / serve 直接返回 `Result<BoxFuture<'static, Result<(), Error>>, Error>`，复用 tok_core::BoxFuture，不提供 ServerTask 别名或重导出。返回的 Future 不创建任务，须由调用方驱动和监督；外层错误表示绑定或组装失败，内层错误表示运行失败。

## RPC 传输配置扩展（2026-10-21）

Client::with_endpoint_factory 接收从 ServiceDescriptor 创建 Tonic Endpoint 的同步工厂；默认保持明文 URI 和默认连接期限。自定义工厂可设置 HTTPS/TLS、连接期限和 HTTP/2 参数，execute 的整体预算仍生效。工厂在缓存锁外执行，并发未命中允许重复构造；重新获取锁后优先复用已发布 Channel。节点和地址是连接共享边界，同一边界必须使用一致的 URI 和传输策略；替换工厂清空本实例缓存，不影响已有克隆。

服务端 Listener::from_server 接收配置好的 Tonic Server，首次路由绑定保留其 TLS/HTTP2 等参数；new/default 仍使用默认 Server。接口接受未添加 Tower Layer 的 Server，业务中间件可包装具体服务。服务绑定、就绪开关、监听 Future 与 Application::register_rpc_endpoint 的监督流程不变。TLS 由应用启用 Tonic 的 TLS feature 并提供证书，不新增框架证书模型、服务发现密钥或配置文件开关。

本轮 Windows 验收：RPC 两库与门面共 78 项测试通过（含 13 项指南示例）；全目标编译与 Clippy、workspace rustdoc、rpc-smoke / client-smoke、格式、202 个文本文件 UTF-8/LF 和空白检查通过。回归覆盖自定义 URI 实际使用、传入 Server 超时生效、缓存复用/撤销、替换工厂清理及工厂错误反馈。未做真实 TLS 握手、外部 Redis/NATS、Linux 或 release 验证；随本次提交交付。

内部接口补充校正：grpc-timeout 解析函数 request_budget 收紧为 pub(super)；OTel metadata 适配器明确命名为 MetadataExtractor / MetadataInjector。相关全目标编译与格式检查通过，随本次提交交付。

## 按键分发与协议适配（2026-10-21）

tok-execution::registry::Registry<K, T, R, E> 根据 K 选择 Tower Service<Request<T>, Response=R, Error=E>，就绪等待和执行共用原 Context。K 需 Ord + Clone；枚举、字符串和自定义键均可用，不限定 u32、数字非零或 Display。注册错误 registry::Error 保留重复键，运行错误 registry::DispatchError 区分 Unknown / Stopped / Handler 并保留原始业务错误，不生成协议状态码。

protobuf 正文适配位于 tok-protobuf，包装上述通用表，不让 tok-execution 依赖 prost。适配器 Registry<K, C, O, E> 根据键解码请求、执行强类型处理链、编码回应；编解码也处于原预算内。业务状态和附加结果为自有 Send + static 类型，借用调用仍可直接使用 execution 或事件处理。

游戏 RPC 明确选择 u32 消息 ID：HandlerRegistry 注册时校验请求与回应 ID 非零，business::Handler 拒绝零请求消息 ID 或零回应消息 ID。上下文转换、request_id 回应关联、计时来源隔离和 Status 映射均留在游戏协议边界。公会处理函数使用自身错误和业务目标，同一表可被本地和真实 RPC 调用；请求锁、玩家组合与通知的后续迁移不计入本批完成范围。

## 按功能域拆分基础错误（2026-10-21）

服务发现、请求锁和路由契约分别使用 core::discovery::{Error, Result}、core::lock::{Error, Result}、core::routing::{Error, Result}；不提供旧领域变体兼容别名。路由的 Discovery 包装保留原发现错误与错误链，Application::Error 增加 Discovery 聚合，注册、清理和故障传播均使用该类别。后端错误按相应领域映射，不返回其他领域的变体。

core 不提供根 Error / Result，参数、配置和可选能力校验由实际所属组件表达。所有权后端故障使用 player::ownership::Error::Unavailable，冲突、配置、凭证与不可用语义保持区分。Redis 发现与锁构造/校验采用所属领域错误，整体应用配置在边界映射为 config::Error::Invalid。

registry 注册错误定名为 Error<K>，运行失败定名为 DispatchError<K, E>；后者覆盖未知键、执行停止与业务处理错误，避免 HandleError 误指只有处理者内部失败。protobuf 适配与示例同步新路径，不保留 RegistrationError 旧类型。

## 移除 core 根校验错误（2026-10-21）

删除 tok-core/error.rs 及根 Error / Result，领域发现、锁和路由契约不变，不提供兼容类型。剩余校验能力回到所属模块：execution::ContextError 表达无效服务身份、操作名和不可表示期限；lifecycle::task::CreateError 表达无效容量和父范围退出，lifecycle::CreateError 表达服务身份及底层任务范围构造失败。构造失败与运行错误分开，组合处保留原始来源。

应用配置使用 tok::config::{Error, Result}，Application 以 Configuration / Create / Discovery 聚合配置、构造和发现错误，Unsupported 表达应用尚未实现的可选能力。所有权和凭证错误分别归 player::ownership::Error 与 player::credentials::Error，不再经 Core 包装；玩家接入运行参数单独使用 gateway::player::ConfigError。示例业务状态构造采用自己的标准 I/O 参数错误，不依赖框架根错误。

## 消息与传输边界校正（2026-10-21）

tok-game-rpc 已调整为 tok-message，通用正文适配独立为 tok-protobuf，tok-rpc 更名为 tok-grpc。生成协议仍在 tok-proto，不依赖执行层。门面提供 tok::protobuf、tok::message 与 tok::grpc；旧 crate 与门面路径不再提供。

tok-message 根 Registry 提供不依赖 Tonic 的非零编号注册、本地分发与原始业务错误；玩家身份、事件/timer RPC 组合、线协议结果与 Status 映射集中于 message::rpc。公会示例和本地/RPC 共用处理链采用根 Registry。通用 protobuf 表继续允许字符串及零键，RPC 不自动添加锁或重试，事件与 timer 仍只能由可信服务器触发。

本轮只校正库与命名空间边界，不宣称通用请求锁泛化、玩家结果模型及通知构造后续批次已经完成。请求锁执行已迁入 execution::lock，message::rpc::lock 只保留协议策略与映射，仍按有限 TTL 和执行预算约束运行。

## 玩家所有权与凭证契约校正（2026-10-21）

所有权 Store 使用 core::Identity 表达持有网关，以 ownership::Error / Result 返回领域故障。普通 acquire 原子更新租约、epoch 和最新登录标记；acquire_for_relogin 还须在同一操作中比较凭证携带的原 session_id。transfer 保留逻辑会话及最新登录标记，renew / release 不清除标记；后端回收标记后只能拒绝相应快速重登并要求普通鉴权，不能将不存在视为通过。

ResumeTokenCodec 使用 credentials::Error / Result，verify 的 None 表示非法或过期，Err 表示本地组件故障；校验仍完全本地，不访问所有权后端。所有权与凭证错误不互相包含。LogicalMessage 的错误码可来自框架或游戏，未发布下行也没有 server_seq；客户端不能直接声明 TimerSpec。

## 请求锁处理链泛化（2026-10-21）

execution::lock::Layer 显式注入 Backend、Config、取键函数和结果判断函数。K: Fn(&Request<T>) -> Result<Key, E>，P: Fn(&R) -> bool；响应 R 与业务错误 E 不依赖 RPC。配置失败使用 core::lock::Error，调用失败使用 execution::lock::Error<E>，取键、获取、执行、释放和原上下文停止分别表达。

获取延迟从有限 TTL 保守扣除，内层就绪等待与执行共用持锁执行预算，接受获取、执行和释放结果前检查实际耗时，防止单次 poll 阻塞后的迟到成功绕过期限。原 Context 的取消、预算及追踪覆盖取键、获取、执行和释放；整体预算耗尽或外部丢弃 future 时不保证完成显式释放，有限 TTL 兜底。正常返回与内部执行超时后尝试有界条件释放，不续租、不重试、不回滚副作用。

释放失败时，原始业务 Err 或 P 判为失败的 Ok 回应保留，成功回应转换为释放错误；RPC 适配以非零 RpcError 判断业务失败，并映射为既有 Status。不将玩家、协议认证或业务错误字段固定到通用层。
