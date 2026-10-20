# 当前实现状态

核对日期：2026-10-20。代码基线为 `2dbb2c8`；当前未提交改动为文档校准。本页描述代码现状，目标设计见[总体设计](design.md)与[服务运行模型](service-runtime.md)，迁移顺序见[实施计划](implementation-plan.md)。

## 已实现与本轮缺口

| 范围 | 当前能力与代码依据 | 方向校正仍需完成 |
| --- | --- | --- |
| 应用组装 | [tok runtime](../crates/tok/src/runtime.rs) 提供 ServiceFactory、启动/退出期限、共享端点、服务就绪与任务监督 | 把可复用构建/运行能力下沉，tok 保留应用组装与门面 |
| 执行模型 | [CallContext](../crates/tok-core/src/execution.rs)、RPC CallHandler、网关 Executor 和事件 Handler 分别可用 | 统一执行上下文、输入/结果契约和 Layer 协议 |
| RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 提供共享端点、强类型分发、请求锁、追踪、无自动重试客户端 | 当前 TowerHandler 只适配 call，不等于事件与 timer 已统一执行 |
| 本地事件 | [local::Dispatcher](../crates/tok-events/src/local.rs) 在当前调用内借用状态、同步执行、首错返回 | 与异步事件 Registry 统一封装、上下文和注册契约 |
| NATS | [Bus](../crates/tok-events/src/nats.rs) 按固定事件类型订阅，组件选择 group，串行处理及容量/期限有界 | topic 进入路由，订阅描述支持固定与动态兴趣，复用统一处理核心 |
| gateway | [Gateway](../crates/tok-gateway/src/runtime.rs) 监督连接及后台任务，Ingress 提供认证和 session 管理 | 成为通用服务设施的扩展实例，补齐查询、生命周期、状态扩展及消息 Layer |
| session | 串行请求、timer 与内部事件，完整身份、所有权续租、下行序列、ACK 缓存、恢复与交接 | 接入共同上下文和处理链；保留现有协议边界 |
| topic | [主题索引](../crates/tok-gateway/src/player/session/subscriptions.rs) 按本地会话成员筛选，支持加入/退出及交接恢复 | 尚无 NATS topic 路由、成员引用合并订阅、聊天/区服自动订阅策略 |
| 发现与协调 | Memory / Redis 发现、session 所有权及请求级锁；路由读取本地目录 | 本轮沿用既有语义；Redis Cluster 客户端仍待实现 |
| 可观测 | tracing、动态过滤、Prometheus、OTLP、RPC/NATS 追踪传播 | 在统一执行入口复用现有设施，维持有限指标标签 |
| 工程 | 八个框架 crate、demo、TOML、protobuf 构建期生成、rustdoc、just、Windows/Linux CI、LF 检查 | 持续验证迁移，不先增加没有实现用途的 crate 或配置 |

## 容易混淆的现状

当前 NATS subject 是 `tok.events.<namespace>.<event_type>`。没有为每个 player_id 建立订阅，但无 group 示例会把该事件类型送到各网关后本地筛选。当前 Target::Topic、join_topic / leave_topic 仅维护本地成员，不改变 NATS 订阅。

Envelope<E> 是强类型事件封装，Message 是带编码正文的运行时表示，EventEnvelope 是 protobuf 网络封装。当前本地分发仍直接接收 &E，不代表已经完成统一 Envelope 与处理表迁移。

发布成功仅表示客户端接受操作，不等待 broker 或业务确认；仅订阅初始化等待有限回环就绪确认。事件 handler 的 Outcome、本地投递 Report、player 执行和客户端 ACK 是不同阶段。

当前 GatewayContext 能监督任务并取得取消信号，SessionHandle 提供部分操作；完整的信息快照、结束等待、统一扩展状态与生命周期接口尚未提供。CallContext 也不是目标 ExecutionContext。

## 现行使用边界

客户端 Frame 为版本 3；断线宽限期默认 30 秒，在线恢复凭证有效期默认 1 小时。恢复凭证使用本地 HMAC-SHA256 验签；顶号通过 session 标记与所有权协调，凭证验证不读 Redis。跨网关交接默认关闭，显式启用共享所有权、共用密钥及内部端点后可用。

请求、timer 和转发事件共用 session FIFO；通知进入下行序列与补发缓存，不调用 player。客户端没有服务器事件入口，不能通过消息 ID 或正文伪造内部事件/timer 来源。

Memory 只用于单进程协调；多进程可选择 Redis。租约与 RPC 的时间检查是尽力保护，超时不表示远端执行停止，不承诺绝对互斥、回滚或恰好一次。生产安全和内部调用身份认证尚未完成，字段校验不能代替认证。

## 未实现的后续能力

统一服务执行/构建设施、完整生命周期上下文、统一事件接口和动态订阅属于本轮校正工作。JetStream、离线玩家加载、跨节点去重辅助、通用玩家归属中继、Kubernetes 发现与部署、生产安全、Redis Cluster 和 Scene 接入属于后续范围。配置可校验或模块已预留不等于后端可运行。

## 验证记录与复现

基线此前已通过 175 项工作区测试（含文档测试）及 13 项真实 NATS 专项；后续命名调整另通过网关测试和 Clippy。该记录不是本次文档重构重新运行的结果，也不证明 Linux runner 或生产负载行为。详细历史见[实施历史](implementation-history.md)。

本次文档整理已通过 workspace rustdoc 构建、7 项指南文档测试、LF 和差异检查；12 份 Markdown 的 81 处仓库内链接及标题锚点检查通过。未重复运行全部工作区或外部服务专项。代码批次应按[实施计划的验证矩阵](implementation-plan.md#验证与交付)选择验证，不把需要真实 Redis/NATS 的专项伪装成已执行。
