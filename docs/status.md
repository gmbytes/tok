# 当前实现状态

核对日期：2026-10-20。P1 基线为 `f54b545`，文档基线为 `2552274`；P2 已提交为 `c6ffdc3`，P3 已完成统一入口及其文档。本页描述代码现状，目标设计见[总体设计](design.md)与[服务运行模型](service-runtime.md)，迁移顺序见[实施计划](implementation-plan.md)。

## 已实现与本轮缺口

| 范围 | 当前能力与代码依据 | 方向校正仍需完成 |
| --- | --- | --- |
| 应用组装 | [应用组装](../crates/tok/src/application.rs) 使用下层 Factory / Builder，保留共享端点、Binding 扩展、发现注册和应用级期限 | P4 扩充 gateway/session 生命周期，P7 收敛完整应用装配入口 |
| 执行模型 | P1 提供 [execution::Context](../crates/tok-core/src/execution/context.rs) 与 [tok-service](../crates/tok-service/src/lib.rs) 类型化执行 Layer，RPC、事件及 RPC timer 路由已接入 | P4 迁移 gateway/session 业务入口和扩展 |
| RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 提供共享端点、强类型分发、请求锁、追踪、无自动重试客户端 | 方法、事件与 timer 使用共同 Request / Layer；网关准入及新下行链待 P4 |
| 本地事件 | [Registry](../crates/tok-events/src/registry.rs) 按类型及稳定名有序多处理者，本地可跨 await 借用状态，远程显式解码后执行同一链 | 完整服务装配便捷入口在 P7 收敛 |
| NATS | [Bus](../crates/tok-events/src/nats.rs) 按固定事件类型订阅，组件选择 group，串行处理及容量/期限有界 | topic 进入路由，订阅描述支持固定与动态兴趣；共同处理核心已接通 |
| gateway | [Gateway](../crates/tok-gateway/src/server.rs) 复用通用 Supervisor 管理后台任务，连接使用独立容量并纳入结束等待，Ingress 提供认证和 session 管理 | 成为通用服务设施的扩展实例，补齐查询、生命周期、状态扩展及消息 Layer |
| session | 串行请求、timer 与内部事件，完整身份、所有权续租、下行序列、ACK 缓存、恢复与交接 | 接入共同上下文和处理链；保留现有协议边界 |
| topic | [主题索引](../crates/tok-gateway/src/player/session/subscriptions.rs) 按本地会话成员筛选，支持加入/退出及交接恢复 | 尚无 NATS topic 路由、成员引用合并订阅、聊天/区服自动订阅策略 |
| 发现与协调 | Memory / Redis 发现、session 所有权及请求级锁；路由读取本地目录 | 本轮沿用既有语义；Redis Cluster 客户端仍待实现 |
| 可观测 | tracing、动态过滤、Prometheus、OTLP、RPC/NATS 追踪传播 | 在统一执行入口复用现有设施，维持有限指标标签 |
| 工程 | 九个框架 crate（含 P1 新增 tok-service）、demo、TOML、protobuf 构建期生成、rustdoc、just、Windows/Linux CI、LF 检查 | 持续验证迁移，不先增加没有实现用途的 crate 或配置 |

## 容易混淆的现状

当前 NATS subject 是 `tok.events.<namespace>.<event_type>`。没有为每个 player_id 建立订阅，但无 group 示例会把该事件类型送到各网关后本地筛选。当前 Target::Topic、join_topic / leave_topic 仅维护本地成员，不改变 NATS 订阅。

Envelope<E> 是强类型事件封装，Message 是带编码正文的运行时表示，EventEnvelope 是 protobuf 网络封装。本地 dispatch 接收带执行上下文的 Envelope 借用，远程解码后进入同一处理表。

发布成功仅表示客户端接受操作，不等待 broker 或业务确认；仅订阅初始化等待有限回环就绪确认。事件 handler 的 Outcome、本地投递 Report、player 执行和客户端 ACK 是不同阶段。

当前 Gateway::tasks 提供后台/连接任务范围 Handle、状态、停止及结束等待；完整服务阶段和退出使用 lifecycle::Handle，SessionHandle 提供部分操作；完整网关/session 信息快照、统一扩展状态与业务生命周期接口仍待 P4。CallContext 保留 RPC 专用身份与关联字段，execution::Context 提供共同执行元信息，两者职责不同；RPC/事件/timer 路由已使用共同执行 Layer。

任务范围可读性收敛：ScopeState 维护共享状态与活动计数，ActivityGuard 通过 Drop 维护活动生命周期，Submission 的容量/活动守卫均说明持有和释放作用。lifecycle::Handle 保留服务阶段、停止和结束等待，任务提交、预留及容量查询统一经 tasks()。P2 的命名调整不改变任务调度或退出策略；本轮 P3 外部回归另列。

P3 统一入口：RPC CallHandler、方法及 timer 路由、事件 Handler 都使用带 execution::Context 的 Request 和 Tower 链。事件 Registry 在本地直接借用状态，远程安装 protobuf 解码器后执行同一组处理者；原始编码共享，错误首停且不重试/回滚。Subscriber 显式声明执行服务身份，追踪、期限与能力由接收适配提供。

RPC 提供 deferred 入口及一次性 rpc::Binding，工厂初始化真实状态和处理链后安装；Application 仍管理共享监听和就绪。demo 的 player/guild 已迁移，GatewayRpc 自定义协议仍由网关组件提供，完整 gateway/session 扩展在 P4 完成。

## 现行使用边界

客户端 Frame 为版本 3；断线宽限期默认 30 秒，在线恢复凭证有效期默认 1 小时。恢复凭证使用本地 HMAC-SHA256 验签；顶号通过 session 标记与所有权协调，凭证验证不读 Redis。跨网关交接默认关闭，显式启用共享所有权、共用密钥及内部端点后可用。

请求、timer 和转发事件共用 session FIFO；通知进入下行序列与补发缓存，不调用 player。客户端没有服务器事件入口，不能通过消息 ID 或正文伪造内部事件/timer 来源。

Memory 只用于单进程协调；多进程可选择 Redis。租约与 RPC 的时间检查是尽力保护，超时不表示远端执行停止，不承诺绝对互斥、回滚或恰好一次。生产安全和内部调用身份认证尚未完成，字段校验不能代替认证。

## 未实现的后续能力

P1 上下文边界校正：移除通用 `target`；`operation` 明确为服务内稳定入口标识，`service` 只标识当前执行服务。实体身份按需通过类型化输入/扩展提供；路由输入、选定端点及熔断状态分离，契约见[服务运行模型](service-runtime.md#操作身份路由与熔断边界)。通用路由器和熔断器尚未实施，不引入通用路由器或熔断器。

P1 执行原型已经提供，不代表全部运行模型统一。P2 命名已收敛为 application 应用组装、lifecycle::Builder 服务构建、Init 初始化参数和 Gateway::tasks 通用句柄；通用工厂与任务生命周期已区分 task 任务范围和 lifecycle 消费式服务编排；P3 已接通 RPC/事件/timer 共同执行链，gateway 扩展和动态订阅属于 P4–P7 的工作。JetStream、离线玩家加载、跨节点去重辅助、通用玩家归属中继、Kubernetes 发现与部署、生产安全、Redis Cluster 和 Scene 接入属于后续范围。配置可校验或模块已预留不等于后端可运行。

## 验证记录与复现

P1 提交时已通过 187 项工作区测试（其中新增 11 项原型测试和 1 项指南文档测试）、全目标 Clippy、workspace rustdoc、格式/LF，以及 execution-smoke、rpc-smoke、client-smoke。现有 NATS/Redis 协议和生产执行循环没有迁移，该批未重复执行外部专项或 release 构建。P2 验证及本轮 P3 验证分别见下文。

基线此前已通过 175 项工作区测试（含文档测试）及 13 项真实 NATS 专项；后续命名调整另通过网关测试和 Clippy。该记录不是本次文档重构重新运行的结果，也不证明 Linux runner 或生产负载行为。详细历史见[实施历史](implementation-history.md)。

2552274 文档整理时已通过 workspace rustdoc 构建、7 项指南文档测试、LF 和差异检查；12 份 Markdown 的 81 处仓库内链接及标题锚点检查通过。未重复运行全部工作区或外部服务专项。代码批次应按[实施计划的验证矩阵](implementation-plan.md#验证与交付)选择验证，不把需要真实 Redis/NATS 的专项伪装成已执行。

工作区 207 项测试通过（P1 基线 187 项，本批新增 9 项服务生命周期测试、5 项任务范围测试、3 项应用/网关测试、1 项指南示例及 2 项编译失败文档测试）。覆盖初始化失败/超时/取消/panic、启动期后台故障、共同容量、正常清理、退出超时、Drop、连接跟踪、父子运行范围与结束等待，以及初始化/后台任务追踪关联。另验证消费式退出成功后立即完成结束等待、外部工作未释放时超时保留 Stopping、组件清理晚于子任务范围退出，以及重复初始化/启动被类型拒绝。

全目标 Clippy、workspace rustdoc、格式/LF、84 处 Markdown 仓库内链接检查通过；execution-smoke、lifecycle-smoke、rpc-smoke、client-smoke、nats-smoke 通过。真实 NATS 13 项测试通过；Redis 发现 2 项测试及发现双进程、共享所有权、三进程交接 3 项集成均通过。本次外部回归串行运行，避免 Windows 示例 exe 占用影响构建。

本批未执行 release 构建或 Linux 本地验证，不将 Windows 结果代替 CI。以上为 P2 提交前记录；P2 已提交，本轮 P3 记录另列，不自动提交。

### P3 验收记录（2026-10-20 Windows）

工作区 214 项测试通过（含 10 项指南示例及 2 项编译失败文档测试）。覆盖同一 Layer 处理 RPC、timer、本地及解码事件，同一事件表的多类型/有序多处理者、跨 await 状态借用、原始错误、坏包/未知类型、容量/类型冲突、原始 Arc 与 ID 保留、本地子事件不继承父正文、取消/预算，以及初始化绑定的身份/重复安装/停止行为。网关测试还验证本地与远程事件共用转发链，timer 效果按序合成且没有客户端 payload。

全目标 Clippy、workspace rustdoc、格式、UTF-8/LF 和 84 处 Markdown 仓库内链接/锚点检查通过。events-smoke、execution-smoke、lifecycle-smoke、rpc-smoke、client-smoke、nats-smoke 六个示例通过；真实 NATS 13 项、Redis 发现 2 项及发现双进程/共享所有权/三进程交接 3 项集成均通过。外部回归串行运行，使用测试独立命名空间。

本批未运行 release 构建或 Linux 本地验证，不将 Windows 结果替代 CI。P3 已完成本批验收，下一批为 P4；P4–P7 尚未实施。
