# 实施历史与验收归档

本文保存方向校正前的路线、阶段划分和验收记录。快照日期为 2026-10-20，包含后续补充的设计校准。

这里的阶段编号、测试数量、“待实现”“未提交”及旧 API 均对应记录时点，不表示当前状态；Core NATS 首版现已提交为 `2dbb2c8`。当前能力见[实现状态](status.md)，后续执行只按[当前实施计划](implementation-plan.md)，架构以[总体设计](design.md)及[服务运行模型](service-runtime.md)为准。

以下原文仅作为历史证据保留，不继续追加新的实施计划。

---

# tok 实施大纲

## 实施目标与范围

当前项目已接通局外请求链路、在线 timer 调度、统一下行序列与 ACK 缓存、Redis 开发发现与共享 session 所有权：客户端登录后，经串行 session 调用 player，收到业务响应，并可由 player 调用 guild。同网关恢复、显式开启的跨网关交接及可选请求级锁已接通，Core NATS 即时广播/转发也已接通，后续推进持久化事件、离线加载、Kubernetes 部署和场景接入。

本大纲以 2026-10-12 的代码为基线，遵循 [设计文档](design.md)。各阶段的技术选择属于实施建议；已经确定的执行语义继续以设计文档为准。阶段完成以可运行示例和验收结果为依据，不以模块或接口存在为依据。

首个可用里程碑是：客户端登录后，经 PlayerGateway 的串行 session 调用 player，收到 Response；同一进程的 player 和 guild 共用一个真实 gRPC 监听端点。存储细节、跨实体事务和无缝场景切换保持后续独立设计的边界。

本轮架构目标：框架核心提供“服务 + 处理链”的构建与运行设施，tok-gateway 作为内置扩展复用这些设施；tok crate 保持门面与应用组装入口。共同能力应下沉，不能让 gateway 反向依赖 tok 或复制独立执行体系。该目标尚未完成实现，职责和依赖约定见[框架核心与扩展分层](design.md#框架核心与扩展分层)。

## 批次校准与验收状态

网关组装已校准为统一 Gateway 运行框架加可配置接入组件。当前重构拆出监听、连接容量与任务监督，Ingress 保留玩家登录和串行 session，Scene 按后续阶段实现；服务配置显式选择组件，业务服务名不承担类型判断。重构期间曾保留 PlayerGateway 兼容接口，现已清理并统一到 Gateway::player，验收覆盖现有 TCP 和 Redis 行为及自定义组件的生命周期。

第一批按“内部 RPC 基础可运行”完成；阶段零至二的全部生产能力仍按各自验收项推进。第二批以 [局外 TCP 协议与 session 实施约定](client-session.md) 为编码和验收依据。

| 范围 | 已实现或验证 | 剩余边界 |
| --- | --- | --- |
| 内部 RPC | 共享监听、强类型分发、连接复用、超时不重试及追踪传播已验证 | 上下文字段校验不代替内部调用身份认证 |
| 服务发现与路由 | 内存与 Redis 开发快照、合并通知、后端注入和稳定 bid 选点已验证 | Channel 失效入口为显式调用；生产监听与自动失效策略随生产发现补齐 |
| 并发和过载 | 用户 Tower 并发限制、session 队列满快速失败经测试，连接与下行容量有界 | 未默认配置 RPC 过载拒绝层；生产容量仍需负载验证 |
| 协调与会话 | 串行 session、Memory 与 Redis 所有权、epoch 比较、条件续租释放、双网关竞争及失效收束已验证 | Cluster 客户端待实现；接受超时后远端继续执行的竞态窗口 |
| 开发检查 | Windows 常规检查、文档、smoke 和 release 构建通过 | Linux 以对应 CI 的实际运行结果为准，不能由本地 Windows 验证替代 |
| 客户端协议 | 版本 3 TCP 分帧、统一序号、ACK 缓存、宽限期、同网关恢复与快速重登已实现 | 生产安全待实现 |

第二批的交付顺序是协议与错误码、认证契约、session 执行器、TCP 适配、示例业务与客户端 smoke。重复登录必须收束旧调用后建立新入口；断线立即关闭的本批行为在第三批扩展为宽限期保留和恢复。

## 当前实现与缺口

| 能力 | 实现现状与代码依据 | 主要缺口 |
| --- | --- | --- |
| 工程与构建 | Cargo workspace 包含八个框架 crate 和 demo-game；just、rustdoc 用户与 API 文档及双平台 CI 已配置 | 外部依赖和生产环境的集成验证 |
| 配置 | [config.rs](../crates/tok/src/config.rs) 校验节点、端点、服务引用、可观测及 player_gateway 容量和期限参数 | 中间件、生产接入安全及外部依赖配置 |
| 服务生命周期 | [runtime.rs（现 application.rs）](../crates/tok/src/application.rs) 提供工厂注册、共享端点、就绪入口、初始化期限及失败清理 | 生产依赖就绪策略与发现后端 |
| 发现 | [MemoryDirectory](../crates/tok-infra/src/discovery/memory.rs) 支持单进程；[RedisDirectory](../crates/tok-infra/src/discovery/redis.rs) 提供多进程开发租约注册、条件撤销、本地缓存与合并通知 | Kubernetes 本地缓存与资源映射；Redis 发现不代替 session 所有权 |
| 协议与 RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 提供 PlayerRpc、BusinessRpc、强类型分发、连接复用、超时、追踪传播、TowerHandler 和可选请求锁 Layer | 配置式中间件及生产调用身份认证 |
| 网关 | [Gateway](../crates/tok-gateway/src/server.rs) 统一接收、容量及监督；[Ingress](../crates/tok-gateway/src/player/ingress.rs) 提供 TCP 认证、串行 session、所有权续租、timer、下行缓存及同网关恢复 | 生产安全及 scene 接入 |
| 路由与 Redis | bid rendezvous 路由、Memory / Redis 所有权及请求锁、条件释放及本地时间预算检查已实现 | Cluster 拓扑与故障切换适配 |
| 事件 | [tok-events](../crates/tok-events/src/lib.rs) 提供同步本地分发、Core NATS protobuf 编解码及网关广播/转发、本地主题筛选与交接；示例已接通 | 统一事件接口、传输主题路由与动态订阅、网关生命周期及消息 Layer；后续再做 JetStream、离线加载和去重 |
| 可观测 | 日志动态过滤、Prometheus、OTLP、强追踪规则、RPC / NATS 上下文传播及调用、session / timer / event 基础指标已实现 | 持久化事件指标及生产告警基线 |
| 示例与部署 | player 与 guild 共享内部 gRPC；`just client-smoke` 经真实 TCP 验证登录、状态读写和 player 调用 guild | 容器构建和可部署清单 |

现有测试覆盖配置、目录变更、路由、内存所有权、共享端点、RPC 分发、连接复用、超时不重试、追踪及生命周期异常。第二批增加真实 TCP 测试，覆盖认证身份、同玩家串行与不同玩家并发、队列满、重复登录、通知、分帧限制、超时、panic 监督及退出清理。`just smoke` 验证进程启动退出，`just rpc-smoke` 验证内部请求回应，`just client-smoke` 验证完整客户端链路。生产 TOML 可通过配置校验，但 Application 会拒绝尚未实现的 Kubernetes 后端。

第一批 Windows 验证使用 Rust 1.99.0 与 just 1.58.0：格式检查、clippy、37 项测试、2 项文档测试、rustdoc 生成、生命周期 smoke、内部 RPC smoke、两份生产配置校验及 release 构建均通过。工具链覆盖仅在验证命令进程中调整为 1.99.0。双平台 CI 已配置，Linux 运行结果仍需由对应环境验证。

第二批 Windows 验证使用 Rust 1.99.0 与 just 1.58.0：`just verify` 通过 50 项测试与 3 项文档测试，格式与 clippy 无警告；`just docs` 生成中文指南及 API 文档；生命周期、内部 RPC 和真实 TCP 客户端 smoke、两份生产配置校验及 release 构建均通过。新增 11 项网关 TCP 测试、1 项示例端到端测试及 1 项配置测试。校准提交为 `7865907`，第二批代码、测试及文档作为独立实现提交。Linux 仍以 CI 的实际结果为准。

## 第三批前置补充：Redis 开发发现

在已完成的第二批上补齐多进程联调环境，不改变第三批 timer、恢复和跨网关协调的范围。

- ServiceRegistry 将注册与只读 ServiceDirectory 分离；Application 选取后端并监督发现任务，保留本进程 Memory 注册镜像。
- 增加 redis_discovery 配置和端点 advertise_address，发布与监听分开；namespace 隔离开发环境，实例 token 防止旧进程操作新记录。
- Redis 节点记录具备有限 TTL、周期续租与 SCAN 刷新；业务请求只读本地缓存，后端错误使应用退出，遗留记录由 TTL 清理。
- 提供 redis-game.toml 与 redis-gateway.toml；just redis-test 和 just redis-smoke 显式运行真实 Redis 测试，常规测试不依赖外部服务。
- 独立游戏与网关进程验证 TCP 登录、状态读写、player 到 guild 调用、正常注销、换地址重启和崩溃过期。CI 增加独立 Linux Redis 集成任务，本地结果不能代替 Linux CI 实际运行结果。

该次开发发现补充仅负责 discovery，不承担 session 所有权；后续共享所有权补充见下文。

2026-10-19 Windows 本地验证：Rust 1.99.0、just 1.58.0，`just verify` 通过 52 项常规测试与 3 项文档测试；使用独立 Redis 7.4 容器显式通过 2 项后端集成测试和 1 项双进程 smoke。中文 rustdoc、原有三类 smoke、两份 Redis 开发配置与两份生产配置校验、release 构建均通过。测试完成后独立 Redis 数据库无遗留注册记录，临时容器已清理。Linux 集成任务已配置，运行结果以 CI 为准。

## 共享 Redis 所有权与系统异常边界

RedisOwnership 已实现 OwnershipStore 的原子获取、expected_epoch 接管、条件续租和释放；ownership 与 discovery 独立配置，默认 Memory，redis-gateway.toml 显式启用 Redis。
按玩家保留不过期 epoch 计数器，租约 Hash 使用 TTL；Lua 按字符串校验身份与代次，覆盖 2^53 相邻整数和有符号 64 位计数器耗尽。
租约与计数器使用同一 hash tag，为 Cluster 共槽预留；当前客户端连接单 Redis 主端点，Cluster 拓扑与故障切换适配尚未实现。

网关仍拒绝抢占其他网关的活动租约，释放或过期后才可重新登录。独立网关进程验证竞争拒绝、旧入口续租失败关闭、条件释放保护新所有者以及崩溃后过期。
该次补充只完成所有权基础后端，不代表当时第三批完成；后续在线 timer、恢复与跨网关交接见下文，请求级锁是另一套契约。

系统接受有限的竞态窗口：调用期限与本地已知租约期限做尽力判断，续租失败或剩余预算不足时停止新驱动，不自动重试，也不强制逐次远程校验或存储 fencing。
RPC 超时后远端仍可能执行，存储层落地时统一加入写入前期限检查；业务按明确需求增加幂等、条件更新或事务。Redis 状态回退不作为绝对互斥保证。
验证入口为 just ownership-test 与 just ownership-smoke，常规测试不依赖 Redis；CI 独立 Redis 任务包含这些检查。

2026-10-19 Windows 验证：Rust 1.99.0 与 just 1.58.0，55 项常规测试、3 项文档测试通过；独立 Redis 7.4 容器通过 7 项集成测试（发现后端 2 项、发现双进程 1 项、所有权后端 3 项、独立网关所有权 1 项）。格式、clippy、中文 rustdoc、原有三类 smoke、两份 Redis 开发配置及两份生产配置校验、release 构建均通过。Linux 以 CI 实际结果为准。

## 统一网关运行框架重构

设计与实现已统一为 Gateway 加接入组件：运行时不判断 Player 或 Scene，GatewayTransport 提供连接，GatewayIngress 处理业务连接，GatewayContext 提交有界后台任务。Ingress 保留原登录、串行 session、所有权与通知行为，兼容包装已清理，统一使用 Gateway::player。

服务配置使用可选 factory 指定初始化工厂，省略时按 service_name 查找，普通 player 和 guild 无需重复填写；示例网关显式选择 player-ingress。Application::register_factory 按名称注册并复用工厂，不再提供 gateway 或 component 配置字段。具体协议与端点共享规则由组装层检查，scene-ingress 仍明确报尚未实现。现有 player_gateway 参数保留，Config 派生通用运行时容量。连接普通错误仅关闭连接，panic 与后台失败由网关统一监督；退出取消入口并收束任务，组件负责清理。

新增测试覆盖非 TCP 自定义组件、连接与后台任务容量、普通错误、panic、后台失败、正常及强制退出，以及组件配置与自定义服务名。既有 TCP 和 Redis 多进程测试用于验证行为兼容。此重构不代表第三批 timer、恢复或交接已完成。

2026-10-19 Windows 验证：Rust 1.99.0、just 1.58.0，60 项常规测试、3 项文档测试与 7 项真实 Redis 集成测试通过；格式、clippy、中文 rustdoc、生命周期及 RPC、TCP 三类 smoke、四份部署配置校验和 release 构建通过。临时 Redis 数据库无遗留数据，容器已清理；Linux 结果以 CI 为准。

同日组件选择校准后，61 项常规测试与 3 项文档测试通过，新增同一工厂创建两个不同身份服务的生命周期验证；格式、clippy 和中文 rustdoc 通过。重新执行 Redis 双进程发现与多网关所有权两项集成测试、三类 smoke、四份配置校验及 release 构建，全部通过。临时 Redis 数据库无遗留数据，容器已清理。

工厂选择最终简化为可选 factory：默认按服务名查找，显式填写时优先使用指定工厂；示例普通业务配置省去重复字段。61 项常规测试、3 项文档测试、格式、clippy、rustdoc、三类 smoke、四份配置校验及 release 构建通过。Redis 所有权联调首次发生一次查询超时，未修改参数的连续两次重跑通过；发现双进程联调通过。该次超时暂未复现，临时 Redis 容器均已清理。

## 设计约束与实施依赖

示例消息 ID 已统一为游戏 proto 的 MessageId 枚举，构建期按消息名称生成 GameMessage::MESSAGE_ID，并检查一一对应、唯一正数及未指定零值。原 player、guild 手写 ID 常量移除，客户端从请求与回应类型取 ID；handler 配对仍显式注册，原有 1 至 11 的协议数值保持不变。新增协议数值稳定性与名称关联失败测试，框架保持 u32 通用契约。

2026-10-19 Windows 消息 ID 校准验收：76 项常规测试、4 项文档测试、格式、clippy、rustdoc、内部 RPC 与客户端 TCP smoke、release 构建通过。映射生成使用与 prost 一致的大小写转换规则；数值声明只在 proto 维护，构建脚本和校验测试共用生成逻辑。

1. player 的客户端请求、timer 和转发事件必须进入同一个 session 执行序列；广播通知进入下行序列，不调用 player。本地事件在当前 player 调用内同步执行。
2. 全局唯一 session 是 player 执行模型的前提。单进程版本可使用内存所有权实现；多网关运行必须先接入原子所有权、epoch 校验与失效处理。
3. RPC 使用 tonic、prost 和可组合的 Tower Layer，无自动重试。超时后远端 handler 可能继续运行，这是设计明确接受的异常边界；验收不能宣称具备超时回滚或绝对互斥保证。
4. 业务服务名与 protobuf 服务显式绑定。共享端点只创建一次监听，服务分别配置中间件；就绪后才发布服务，退出先撤销可用性并停止接收新工作。
5. 所有队列、补发缓存和去重记录必须有界。并发、配额及锁的作用范围由用户选择，按 bid 加锁不等同于进程资源隔离。
6. 重连补发重用已编码字节和原 server_seq，不重新执行业务。Redis 保存所有权，不保存补发正文；新连接建立自己的安全上下文。

主要依赖顺序：基础契约 → 共享端点和 RPC → 单网关 session → timer 与同网关恢复 → Redis 所有权和跨网关交接 → 完整事件链路与生产部署。同步本地事件可随示例业务提前实现，Kubernetes 资源映射设计可在 RPC 完成后开展。

## 阶段零 开发与验证基线

**目标：** 使开发命令和 CI 能验证同一工程基线。

- 核对 rust-toolchain.toml、workspace rust-version 与实际工具链；处理 RUSTUP_TOOLCHAIN 覆盖造成的版本差异。
- 开发环境统一使用与 CI 一致的 just 1.58.0；本机已升级，现有 justfile 解析及 verify、smoke、config-check 配方已验证通过。
- 执行格式、clippy、workspace 测试、配置检查和生命周期 smoke，记录双平台 CI 结果。

**验收：** Windows 与 Linux 的常规检查均通过；明确区分生命周期 smoke 与后续业务链路 smoke。工具链修复独立于业务实现，不通过忽略 rust-version 绕过版本要求。

## 阶段一 基础契约与协议

**目标：** 建立后续组件共同依赖的类型和边界。

- 在 tok-core 定义业务调用上下文、逻辑消息、session 身份、所有权、请求级锁及路由契约；明确业务 ID 表达与转换规则。
- 在 tok-proto 增加 player 通用调用、timer 返回信息及必要的控制协议。游戏请求和回应仍放在 demo-game/proto。
- 定义消息 ID 注册和 Request 与 Response 配对，提供显式强类型分发及通知编码入口。
- 分开 request_id、session_id、epoch、server_seq 和 ack_seq；明确错误分类与客户端 ErrorCode 映射扩展点。
- 为单进程开发提供可替换的内存所有权实现，避免网关直接依赖 Redis；锁抽象放 tok-core，后端放 tok-infra，Layer 放 tok-rpc。

**验收：** 游戏可以注册一组请求、回应和通知；重复注册、未知消息及非法 payload 有明确结果；框架不依赖具体游戏类型。

## 阶段二 共享端点与内部 RPC

**目标：** 让目录中的服务地址对应真实监听和可调用的业务。

- 为 Application 增加端点组装与监督：先收集绑定到端点的服务，再统一创建监听，监听和服务随进程退出清理。
- 显式绑定 service_name 与 protobuf service；同一个 internal-rpc 端点承载 player 和 guild，避免工厂各自绑定端口。
- 将运行时发现依赖改为可注入契约，保留内存注册能力；设计快照变更通知与连接缓存失效方式。
- 在 tok-infra 实现可复现的 bid 选点，先按服务过滤可用节点，再选目标；无可用节点明确失败，节点变化不添加透明业务重试。
- 在 tok-rpc 实现客户端连接复用、超时、调用元数据、W3C TraceContext 传播及可组合的并发和过载控制。
- 初期由 Rust 代码组装 Layer；若需要配置组装，只允许选择已注册组件，校验参数、顺序和作用范围。

当前 local.toml 将 player-gateway 放在 player 之前。共享监听先绑定，业务完成初始化才开启自身 RPC 入口；工厂初始化不能等待尚未启动的其他服务，依赖调用应在运行阶段确认目标就绪后发起。

**验收：** 一个 gRPC 监听同时提供两个业务服务；服务绑定失败触发整体清理；超时和过载快速失败且不重试；跨 RPC 能关联 trace；连接与监听在退出时释放。

## 阶段三 单网关局外请求闭环

**目标：** 完成首个可交互的游戏示例。

- 建议优先使用现有配置中的 TCP，明确长度分帧、最大包长和控制消息布局；协议选择及生产安全组合在实施前落入设计约定。
- 分离 Connection 与 session；认证扩展点产生可信 player id，路由不能信任客户端自行填入的 player id。
- 实现有界 session 队列、串行 executor 和生命周期；接入多条连接或请求也保持同一玩家串行，不同玩家可以并发。
- 接通 player 通用 RPC、强类型 handler 和 Response ErrorCode；给广播通知保留下行入口，给 timer 和转发事件保留执行入口。
- demo-game 提供最小登录、状态读取和状态变更，并通过 player 调用 guild 展示服务组合。本阶段使用内存示例状态，持久化恢复另行设计。
- 增加真实客户端驱动与业务 smoke；示例认证方式清晰标注适用环境。

**验收：** 客户端能够登录、执行请求并收到对应回应；同一玩家并发请求按队列串行，不同玩家互不阻塞；未知消息、认证失败、队列满和 RPC 失败均有明确处理。

## 阶段四 timer 与同网关消息恢复

**目标：** 让单网关 session 可以持续调度并接续断线消息。

第三批第一步已实现在线 timer：HandlerReply 保留可选计时更新，Reply 交给 session 应用；ReplaceAll、Upsert、Cancel 原子校验，旧版本的排队触发失效；同一有界队列串行执行客户端与计时任务，繁忙时合并周期，失败或超时不重试。CallContext.timer 区分内部来源，强类型 timer 分发表拒绝客户端仅凭消息 ID 触发。数量、ID 和正文总字节有限，非法更新关闭 session。示例 START_PRODUCTION 只发起业务操作，服务端决定 timer 与产出，内部 FINISH_PRODUCTION 结算状态，client-smoke 覆盖完整链路。

当前已接通 ACK 缓存、断线宽限期、同网关恢复与快速重登；默认断线 30 秒内继续 timer 和续租。未增加固定的 player 登录 RPC，游戏初始化请求负责完整状态及全量 timer，demo 已支持。跨网关交接见第三批第四步约定。

2026-10-19 第二步校准与实施约定见[客户端会话约定](client-session-history.md#第三批第二步下行序列确认与缓存)，按以下顺序交付：

1. **下行序列与缓存（已实现）：** 客户端 Frame 升级为版本 2；已绑定 session 的 Response（含框架失败）与 Notify 统一发布、连续编号并缓存最终编码字节。独立 Ack 累计确认，不进入 player 队列；确认不超过已发布最大序号，重复及倒退忽略。缓存按单调时间、条数、字节淘汰最旧记录，支持查询连续范围和缺口。demo 已同步升级并验证真实 TCP。此步断线仍立即关闭 session，不开放恢复请求，也不实现 token 或宽限期。
2. **同网关恢复（已实现）：** 分离连接与 session 生命周期；宽限期继续驱动 timer，签发及校验 token，校验连接绑定代次，按原序号补发并串接新下行，发送恢复完成标记。缓存缺口或 session 不可恢复时返回 ResumeUnavailable，使用有效 token 快速重登，建立新逻辑会话并刷新业务状态及 timer。

第三批第三步协议已升为 Frame 版本 3；该步 token 为签发网关内存中的有界随机凭证，身份有效期默认 1 小时，断线宽限期默认 30 秒，重启后失效。跨网关 token 验证与完整交接由后续阶段五实现；游戏状态初始化负责完整业务状态与 TimerUpdate::ReplaceAll，框架不增加固定的 player 登录 RPC。详见[同网关恢复约定](client-session-history.md#第三批第三步同网关恢复)。

第三批第三步 Windows 验收（2026-10-19）：101 项常规测试、4 项文档测试通过；新增 7 项凭证/下行/会话单元测试和 6 项真实 TCP 恢复测试，扩展生产初始化用例验证新会话可读取全量 timer。覆盖在途回应不重执行、旧连接隔离、宽限期到期/撤销、补发期间新消息排序及缺口、凭证失效、快速重登、离线租约丢失和半帧断线恢复。格式、clippy（含 missing_docs）、中文 rustdoc、三类本地 smoke、两项 Redis 多进程 smoke 及 release 构建通过。Redis 回归使用独立临时容器，已清理；Windows 的可执行文件占用曾使所有权 smoke 启动失败，等待其他 smoke 退出后单独重跑通过。Linux 结果以 CI 为准。

第三批第二步先验证下行发布一致性、ACK 校验和缓存边界；断线期间 timer 继续、重绑定、真正补发和快速重登归后一步验收。缓存具体开发默认值及版本迁移规则以客户端会话约定为准，不把“缓存已实现”等同于“短线恢复已完成”。

第三批第二步 Windows 验收（2026-10-19）：Rust 1.99.0、just 1.58.0，88 项常规测试、4 项文档测试通过；新增 9 项下行与缓存单元测试、3 项真实 TCP 测试，并扩展队列拒绝、通知、重复登录和旧帧版本用例。格式、clippy（含 missing_docs）、中文 rustdoc、生命周期/RPC/客户端三类 smoke 与 release 构建通过。Redis 发现和共享所有权两项多进程 smoke 通过，测试使用独立临时 Redis，容器已清理。客户端 Frame 版本 2 是协议升级，旧客户端须同步升级；断线恢复尚未实现，Linux 结果以 CI 为准。

2026-10-19 Windows 验收：Rust 1.99.0、just 1.58.0，73 项常规测试、4 项文档测试与 7 项真实 Redis 集成测试通过；新增 10 项调度及会话测试、2 项 RPC 测试和 1 项文档测试，客户端既有用例增加计时状态变更与来源隔离。格式、clippy、中文 rustdoc、三类 smoke、四份部署配置校验及 release 构建通过。Redis 数据库无遗留测试数据，临时容器已清理。Executor 结果改为 Reply，CallContext 增加可选 timer 来源，迁移方式见 Rust 用户指南；Linux 结果以 CI 为准。

示例业务边界校准：移除客户端计时配置接口，改为无计时参数的“开始生产”请求，耗时、内部消息和产出全部由服务端规则决定。新增客户端夹带计时或产出字段不能覆盖规则的回归测试；74 项常规测试、4 项文档测试、格式、clippy、rustdoc、客户端 smoke、Redis 发现与所有权两项多进程联调及 release 构建通过。临时 Redis 数据库无遗留数据，容器已清理。

- 实现 timerSpec 的 ReplaceAll、Upsert 和 Cancel，以及调度版本检查；UTC due_at 转换为本地等待时间。
- timer 到期只入 session 队列；同一 timer 至多保留一个待执行触发，错过周期合并一次并沿原时间基准计算下次时刻；该次 RPC 失败不重试。
- 统一 Response 与 Notify 的 server_seq；先缓存编码字节再发送，校验客户端连续 ack_seq。
- 补发缓存配置时间、条数和字节限制；缓存不完整明确返回 ResumeUnavailable。
- 实现断线宽限期、token 校验、同网关恢复和恢复完成标记；补发期间的新下行不能越过待补发消息。
- 快速重登建立新的逻辑会话和全量 timer；token 的身份有效期与 session 恢复窗口分别管理。

**验收：** 覆盖 timer 更新后旧任务失效、繁忙期间周期合并、断线时 timer 继续执行、Response 与 Notify 按原序号补发、非法 ack、缓存淘汰及快速重登。时间相关测试使用可控时钟，补发不增加业务执行次数。

## 阶段五 Redis 所有权与跨网关交接

**目标：** 将单进程串行前提扩展到多网关部署。

当前已完成 Redis 所有权、完整身份条件转移、共享恢复凭证、跨网关冻结交接和失效入口收束；请求级锁与 RPC Layer 也已实现；本阶段的 Cluster 客户端仍未完成。

第三批第四步已按[跨网关交接约定](client-session-history.md#第三批第四步跨网关交接)实现共享凭证、冻结排空、稳定快照、旧网关协调提交和完整身份条件转移，并在新网关接续补发与 timer。正常交接保留 session_id、请求进度和下行序列，epoch 增加；旧网关宕机或缓存缺口则降级快速重登。Redis Cluster、请求级锁、NATS 和生产安全不并入本轮验收。

- 实现 Redis 所有权记录、原子获取与 epoch 更新、续租、条件释放和旧所有者失效；明确 Redis Cluster 键布局与原子操作范围。
- session 开始业务执行前检查所有权有效性。Redis 不可达、续租失败或租约不确定时停止新的有效驱动，避免继续假定自己拥有会话。
- 设计并实现交接状态机与内部 RPC：旧网关停止接收新业务并收束在途调用，导出下行缓存、确认位置、timer 状态和必要订阅信息，新网关取得所有权后接续驱动。
- 明确交接期间新通知和未完成请求的处理边界；原 session_id 和消息序列仅在完整恢复时保留，交接失败使用 ResumeUnavailable 和快速重登。
- 实现 Redis 请求级锁与注入后端的 Tower Layer；按服务命名空间和 bid 加锁，确定获取期限、租约、续租与条件释放规则。
- 锁租约丢失和 RPC 超时都不能视为业务回滚；业务写入需要更强保证时使用业务版本或其他校验，具体持久化方案另行设计。

**验收：** 覆盖双网关竞争、重复登录、旧 epoch、Redis 故障、旧网关宕机及交接中断；检测到租约失效或预算不足后停止新驱动，已发出或检查后暂停的调用遵循约定的竞态边界。正常交接保留序列与 timer，不丢交接边界内的下行。请求级锁验证同 bid 的正常路径互斥、不同 bid 并发和旧持有者不能释放新锁，不宣称异常路径下绝对安全。

2026-10-20 文档校准：同网关恢复、player 类型/方法/字段命名及非测试 unwrap/expect 检查已提交为 `c9365ae`。校准时 Windows 常规检查通过 102 项测试与 4 项文档测试，7 项依赖真实 Redis 的测试未在本次执行。当时跨网关交接仅完成设计校准；后续实现与验收见文末记录。

### 请求级锁与业务 RPC 接入（已实现）

本批已接通普通业务 RPC 服务端的执行策略，RPC 传输链路已经存在。只在显式安装的服务上，按“校验上下文 → 获取锁 → 执行 handler → 条件释放”执行；不接入 `GatewayRpc` 交接接口，不默认给 player 加锁。完整边界见[请求级锁设计](design.md#全局实例的请求级锁)。

1. 保持核心层的 `LockBackend` / `LockLease` 契约分层，在基础设施层实现有界 Memory 后端和 Redis 后端。Redis 获取使用唯一持有者标识及有限 TTL，释放使用原子身份比较，旧租约不能删除新持有者的锁。
2. 在 RPC 层实现可注入后端的 Tower Layer，适配现有 `(CallContext, Payload)` 输入。逻辑服务名作为固定命名空间、上下文 bid 作为实体键；空 bid 拒绝执行。同服务同 bid 跨节点共用锁，不同服务使用独立锁空间。
3. 明确等待、执行、后端操作和释放的有限预算。首版不自动续租；从获取开始保守计算本地有效期，执行前确认剩余预算，预留释放余量。获取竞争可在等待期限内尝试，handler 不重试。
4. 正常结束和 Layer 内部超时走显式条件释放；调用 future 被外层丢弃时由 TTL 兜底。覆盖获取结果不确定、执行超时与释放失败；成功业务遇到释放失败返回系统级失败，已有业务错误保留，均记录释放异常且不自动补偿或重试。
5. 以 Rust 组装代码给示例 guild 显式安装，验证 player 沿原 session 路径执行。首版不向 `ServiceConfig` 增加锁字段，不增加配置式 Layer 链、自动续租或方法级配置 DSL。Redis Cluster 客户端另列后续工作，本批不据此宣布阶段五全部完成。

**验收：** 单元测试覆盖同键互斥、不同 bid 并行、不同服务命名空间隔离、容量与等待上限、空 bid 拒绝、handler 错误、执行超时和取消后的租约过期；验证获取耗时会扣减有效预算。真实 Redis 测试覆盖跨进程竞争、过期后重新获取、旧持有者不能释放新锁、后端不可达及释放失败。示例 RPC / 客户端 smoke 验证业务路径及未安装服务；不将异常任务超时后的绝对互斥作为验收承诺。

## 阶段六 本地事件与 NATS 事件

**目标：** 按既定投递语义接通服务间通知。

- 同步本地分发器已在当前 player 调用内接通；使用有界注册、顺序执行和首错停止语义。
- 定义稳定 event_id、事件类型、DeliveryPolicy 及追踪上下文；网关目标和未来离线加载策略由会话适配处理，明确主题加入、退出和交接规则。
- Core NATS 提供即时投递；广播进入下行序列，转发进入 session 执行队列。离线玩家默认不拉起、不积压。
- JetStream 按事件类型配置存储与消费确认，重复发布沿用 event_id；框架提供有界去重辅助，业务幂等承担跨进程和资产变更的重复处理责任。
- 定义持久化转发事件的消费确认时机和失败处理，区分进入网关队列与完成业务执行；同时明确默认离线不消费与持久化保留策略如何配合。
- 允许离线加载的事件也必须获取所有权并串行驱动，明确临时 session 的释放条件。

**验收：** 指定玩家、主题和全体目标正确分发；广播不调用 player；转发与请求、timer 串行；验证重复、重新投递、离线跳过和临时加载。事件消费确认与客户端 ack_seq 各自独立。

### 第一批：事件契约与同步本地分发（已实现）

- 定义显式事件名称、按类型声明的投递策略与离线加载选项；提供可生成或恢复的稳定事件 ID、目标及追踪头载体。跨节点 Envelope 只定义数据契约，不宣称已有编码、发布或消费能力。
- 实现强类型、有界注册的本地分发器；显式借用当前业务状态，按注册顺序同步执行，首错停止并原样返回，无订阅者成功返回零。无后台任务、内部队列、隐式锁、自动重试或去重，部分修改不自动回滚。
- 示例在生产 timer 的原 player 调用内分发本地完成事件，将计时入口与产出业务处理分开，不改变客户端消息 ID 或 protobuf 协议。
- 验收名称、ID、目标及追踪载体校验，注册容量、执行顺序、错误短路与部分修改、重复调用及共享分发器；示例验证正常产出、过时与重复触发、产出溢出错误及原状态保护。执行常规测试、rustdoc、lint 和示例 smoke。
- 本批不实现 Core NATS / JetStream、网关事件入口、主题订阅、离线加载、跨节点去重或交接订阅迁移；不据此标记阶段六整体完成。

### 第二批：Core NATS 通用订阅与网关适配（已实现）

1. 接入有界 Core NATS 客户端、版本化 protobuf 编解码、操作期限、环境隔离和标准追踪传播；发布不等待业务结果，拒绝尚未支持的持久化策略，不加载离线玩家。
2. 任意服务可按类型发布和订阅，group 显式选择实例广播或组内分担；通用 Envelope 只承载 ID、正文和追踪，Registry 回调共享原始编码和强类型正文。网关通过游戏注入的处理者选择目标及通知/会话转发动作，内部 HandleEvent 返回独立 EventResponse。
3. 为主题提供可信服务端 API、配置容量和完整 session 生命周期；主题与事件进度随交接快照转移，验证冻结、取消和新登录边界。
4. 服务工厂组装订阅，GatewayContext / ServiceTask 监督消费；独立退订不关闭共享连接，数量/缓冲预算及 handler 等待有界。离线不拉起，错误或未知结果不自动重试。
5. 示例提供显式可选 NATS 配置和发布验证入口；验收内存队列、真实 RPC/TCP、真实 NATS 多网关投递、断线/坏包/容量/退出及交接恢复。JetStream 和离线加载仍独立推进。

### 第三批：统一事件接口、动态主题与网关扩展（设计已校准，未实施）

实施依据为[事件与网关扩展设计](service-runtime.md)。第二批完成的是首版固定类型订阅与本地 topic 筛选，不代表动态聊天/区服订阅、生命周期查询扩展及统一事件接口已完成。

1. 先将共同构建、执行与生命周期设施放到门面和 gateway 可共同依赖的下层，以普通服务和 gateway 样例验证无环及同一套运行扩展；统一 ExecutionContext 及消息/事件/timer 共同的中间件协议，再统一 Event / Envelope / Registry / Subscriber 和借用上下文的异步处理契约，同步回调使用适配；本地不强制序列化。保留 dispatch 的完成结果与 publish 的接受结果，避免隐藏执行语义差异。
2. 将 topic 纳入事件封装、网络协议和 subject，订阅组件分别声明类型、主题来源和可选 group，默认主题不等于通配订阅。
3. 补齐 Gateway、SessionManager、Session 的身份与状态快照、停止/结束等待、作用域状态工厂和生命周期回调，为事件和中间件提供相同上下文能力。
4. 根据本地成员合并动态传输兴趣，支持聊天主题、可信身份决定的区服主题和固定兴趣；按异步加入、退出、取消、关闭、恢复与交接规则管理引用和预算，不默认按玩家建订阅。
5. 将 RPC、客户端业务消息、本地/NATS 事件和 timer 接入共同执行核心，基于 Tower 组装业务准入、服务/session 执行和新下行 Layer；身份、所有权、FIFO 与协议状态机不可绕过，补发不再次执行中间件。
6. 通过真实多网关 NATS/TCP/RPC、资源失败和生命周期测试验收；各步更新用户文档并明确实际实现范围。通用扇出中继、JetStream、离线加载及热配置不并入本批。

## 阶段七 Kubernetes 与生产运行

**目标：** 使用同一游戏二进制按配置拆分服务并运行。

- 实现 EndpointSlice 监听与本地缓存，补齐 Service、Pod、node_id、启动代次、服务名及端点映射；请求路径不查询 Kubernetes API。
- 处理初始同步、watch 中断、重新列举、就绪变化、删除和地址变化；发布可访问地址，不能把 0.0.0.0 当作远程目标。
- 接入运行时发现后端；就绪状态结合监听、必要依赖和发现同步，退出时撤销可用性、停止接收、清理 session 和通信任务。
- 增加镜像构建、Deployment、Service、RBAC、配置注入、探针及资源配置。网关具备客户端接入与交接所需的内部端点。
- 选定并接入首个生产安全适配，明确证书或密钥配置、token 签发撤销及内部调用身份检查；开发明文配置与生产配置分开。
- 补齐队列、RPC、timer、缓存、所有权和事件指标；日志与 span 可携带 bid，指标不使用玩家或请求 ID 标签。

**验收：** 在测试集群中运行网关和游戏节点，验证发现同步、扩缩容、Pod 重启、滚动更新、SIGTERM 清理及依赖故障；Windows 与 Linux 常规构建继续通过。多进程开发测试可使用 Redis 开发发现、注入测试目录或使用测试集群，生产发现仍使用 Kubernetes。

## 阶段八 SceneGateway 最小接入

**目标：** 在局外链路稳定后接通异步场景消息。

- 先补齐场景定位、加入退出、连接 session 和故障恢复语义，再选择第一种传输与安全适配；不要求一次实现 KCP、UDP 和全部加密方案。
- 明确场景服务定位与 scene bid 的关系，接通 SceneGateway 到场景服务的异步链路。
- 复用适用的 codec、transport 和 security 组件；场景 session 的串行、所有权、补发和恢复按局内语义分别定义。
- demo-game 增加最小场景消息示例；无缝跨场景切换继续独立设计。

**验收：** 客户端能够进入场景并交换异步消息；场景服务异常和断线有明确处理；局内流量过载不突破配置的资源边界。

## 开始实施前需要细化的决策

| 决策 | 最迟确定阶段 | 应形成的结果 |
| --- | --- | --- |
| bid 表达、调用上下文和消息字段 | 阶段一 | 基础类型、proto 与显式注册 API |
| 共享端点和服务就绪依赖 | 阶段二 | 监听生命周期、服务绑定及初始化顺序 |
| hash 算法与节点变化行为 | 阶段二 | 路由规则及可复现测试 |
| 首个接入协议与容量限制 | 阶段三 | 分帧、能力描述、队列及包长配置 |
| token、宽限期、ack 校验和缓存窗口 | 阶段四 | 会话状态机、协议控制消息及错误映射 |
| epoch、租约及交接边界 | 阶段五 | Redis 原子操作和交接状态机 |
| 事件确认、主题迁移及离线加载释放 | 阶段六 | 消费生命周期和各类事件策略 |
| Kubernetes 资源映射与生产安全组合 | 阶段七 | 发现配置、部署清单与安全配置 |
| 场景定位和恢复 | 阶段八 | 局内协议及最小示例 |

队列、包长、缓存和期限必须可配置并具有有限默认值；具体数值通过实际负载验证确定，不在当前大纲中预设吞吐承诺。

## 交付与推进方式

第一批已接通基础类型与协调契约、内存开发后端、RPC service、共享监听、连接复用与调用验证，同时加入 Rust 原生用户文档生成。第二批已实现注入认证、串行 session、TCP 接入及示例客户端闭环，并完成本地验收。第三批已接通在线 timer、下行序列与 ACK 缓存及 Redis 所有权基础，已接通同网关恢复及显式开启的跨网关交接；请求级锁、可选 Tower Layer 和 Core NATS 已接通，JetStream、离线加载、生产安全和 Kubernetes 仍按后续阶段推进。

每个阶段按“契约与协议 → 组件实现 → 示例接通 → 故障与边界验证 → 文档更新”拆分变更。优先开始阶段零至三，得到可运行的局外链路后，再推进恢复与集群能力。

每次增加真实组件，同步更新 README 的当前能力说明和设计文档中的实现状态；新增业务 smoke 不能替换已有生命周期异常测试。Redis、NATS 和 Kubernetes 的集成验证显式声明依赖与运行方式，常规单元测试保持可独立运行。

最小交付顺序如下：

- 第一批：工具链基线、基础契约、RPC service 定义、共享端点及连接调用。
- 第二批：认证扩展、串行 session、TCP 接入、示例业务及真实客户端 smoke。
- 第三批：timer、下行序列、同网关恢复、Redis 所有权及跨网关交接。
- 第四批：跨节点事件、生产发现与部署、安全适配及故障验证。
- 第五批：局内场景接入；存储方案和无缝切换另开设计与实施计划。

## 第三批第四步实现记录

跨网关交接已实现，默认关闭。ResumeTokenCodec 提供纯本地签发与验证契约，HMAC-SHA256 固定共享密钥签发与验证并保留原有效期；不维护 Redis 凭证表；Redis 所有权条件转移比较完整旧身份，保留 session_id 并增加 epoch。

旧网关停止新准入与 timer 调度，排空已有队列后导出原字节缓存、请求进度及 timer 调度；新网关预留容量并校验快照后，由旧网关协调提交或取消。准备记录、目标暂存和后台任务均有界；受监督截止任务在 RPC 被取消后仍负责解冻或停驱动。提交结果不确定先查询，提交开始后不重新启用旧入口；客户端在排空期间断开也会进入目标网关的正常宽限期清理。

示例增加本地 resume_token 共享密钥配置、GatewayRpc gRPC 服务及 redis-handoff-a.toml / redis-handoff-b.toml。客户端仍使用版本 3 的 Resume/Resumed/Recovered，不增加字段；普通登录不能抢占活动远端所有权。网关宕机或缺少快照时返回 ResumeUnavailable，待租约释放或过期后用共享 token 快速重登，业务全量初始化负责重建状态和 timer。

新增 9 项真实 TCP/gRPC 交接测试，覆盖原回应补发、timer 接续、在途排空、通知冻结、取消与迟到提交、目标竞争、准备期限、未知提交结果和客户端中断清理。增加缓存迁移预算、完整身份条件转移、共享凭证及配置测试。just resume-token-test 离线验证签名凭证；真实 Redis 验证入口为 just ownership-test 和 just handoff-smoke，独立 Linux CI 包含三进程交接检查。

Redis Cluster、请求级锁、当时尚未实现的 NATS 订阅迁移及生产安全不计入本步完成范围；内部交接仅用于受信开发部署，不宣称超时回滚、异常路径恰好一次或绝对互斥。

2026-10-20 Windows 验收：Rust 1.99.0、just 1.58.0，119 项常规测试与 4 项文档测试通过，格式和 Clippy 无警告；中文 rustdoc、三类本地 smoke、两份新增交接配置校验和 release 构建通过。独立 Redis 7.4 容器通过 10 项外部集成测试：发现后端 2 项、所有权后端 5 项，以及发现、所有权、交接多进程 smoke 各 1 项。三进程交接验证已在最终实现上通过，临时数据库确认无遗留键后容器已清理。Linux CI 已配置，实际运行结果仍以对应 runner 为准。

本地验签校准：移除 ResumeTokenStore、Redis/内存逐 token 记录及 max_resume_tokens；改为同步 ResumeTokenCodec 和启动时加载的 HMAC-SHA256 固定共享密钥。顶号保证保留：新登录在所有权获取中原子更新最新 session_id，快速重登原子比较此标记，交接和释放不删除它。新增普通登录顶掉旧入口、旧 token 在新会话下线后仍不能重登的回归；凭证验签本身不访问 Redis。

凭证简化校准：移除多公钥、密钥 ID、用途域和运行时替换入口，改为 HMAC-SHA256 固定共享密钥。配置统一为 `[resume_token]` 下的 secret_key 与 clock_skew_ms，不同游戏和环境使用独立密钥。维护时统一更换密钥并重启，旧凭证失效；不提供在线轮换。

在线凭证刷新补充：版本 3 增加 RefreshResumeTokenRequest/Response 两个控制帧，已认证且完成恢复的当前连接可主动刷新，条件续租确认所有权后签发同一 session_id 的新凭证，不重建业务或序号。在线可在原凭证到期后刷新，离线过期须普通鉴权；旧凭证仍按原期限有效，顶号按 session_id 同时阻止整组旧会话凭证。刷新与交接/替换串行协调，不增加独立长期 refresh token 或密钥轮换。新增在线过期刷新及再恢复、准入/重复 ID/顶号、所有权失效和旧绑定/补发/冻结拒绝回归，并扩展双网关交接与客户端 smoke。

在线刷新验收（2026-10-20 Windows）：123 项常规测试及 4 项文档测试、格式、Clippy、rustdoc、更新后的客户端 smoke 和 release 构建通过。真实 Redis 三进程回归确认目标网关刷新后，源网关可在目标宕机、租约过期后验证新凭证并快速重登；临时数据库确认无遗留键后清理容器。

网关内部 RPC 入口校准：将玩家交接专用 HandoffService 提升为网关层 rpc::GatewayRpcService，协议统一为 GatewayRpc，交接方法显式命名 PrepareHandoff、CommitHandoff、CancelHandoff、CompleteHandoff。会话交接状态机继续保留在 player/session/handoff.rs；目录与管理器注册可供后续网关 RPC 共用，注册与 handoff_enabled 解耦。内部发现/工厂名改为 gateway-rpc，配置字段为 gateway_rpc_service_name；内部 gRPC 路径和旧配置不兼容，参与节点需同步更新，客户端协议不变。不预先增加未实现的查询或踢人接口。

统一网关 RPC 验收（2026-10-20 Windows）：124 项常规测试及 4 项文档测试、格式、Clippy、rustdoc、客户端 smoke、两份交接配置校验和 release 构建通过。真实 Redis 三进程交接与双网关所有权回归通过；临时数据库确认无遗留键后清理容器。

玩家模块结构整理：player 外层保留认证、配置、接入、连接协议、执行扩展和凭证能力；session.rs 只组织内部模块并导出会话类型。session 下按 manager、handle、state、worker、handoff、channel、replay、timer 分工，测试按长度和辅助代码规模组织：短小测试内联，较长测试保留独立文件。内部以 timer::Scheduler、timer::Ticket、channel::Channel、channel::Binding 等限定名称表达上下文；共享可见性限制在 session 或 player 范围。外部从 player 导入 SessionManager、SessionHandle、PendingResponse、ReplayBatch；移除 PlayerGateway 兼容包装，测试统一通过 Gateway::player 组装。公共模块路径的调整为 API 变更，客户端协议与业务行为不变。

玩家结构整理验收（2026-10-20 Windows）：124 项常规测试及 4 项文档测试、格式、Clippy、rustdoc、客户端 smoke 和 release 构建通过。真实 Redis 三进程交接与双网关所有权回归通过，临时数据库确认无遗留键后清理容器。

Clippy 检查补齐：workspace 显式禁止同步锁/RefCell 借用跨 await、匿名丢弃 Future，并加入 large_futures、unimplemented、module_name_repetitions、clone_on_ref_ptr、unused_async、let_underscore_must_use 的 deny 检查。手写引用计数克隆改为 Arc::clone；must_use 忽略经逐处审查，在取消、退出及测试清理边界用带明确原因的语句级 expect 保留；tonic 生成模块仅豁免其克隆风格。测试崩溃服务改为显式构造惰性 future，仍在任务执行阶段触发 panic。

player 公共名称同步精简为 Config、Ingress、Executor、RpcExecutor、Reply、Task、AuthenticatedIdentity，凭证配置使用 resume_token::Config；实现模块 config/executor/ingress 隐藏，经 player 导出，使用端可保留 player:: 限定避免裸名冲突。DefaultRpcErrorMapper 和 MAX_CREDENTIAL_BYTES 也从 player 导出。其他跨模块角色或后端名称逐项说明保留理由，不做全局风格豁免。此为 Rust API 命名变更，配置内容与客户端协议不变。

Clippy 补齐验收（2026-10-20 Windows）：新增规则下 workspace 全目标 Clippy 无告警，124 项常规测试及 4 项文档测试通过，格式、rustdoc、生命周期/RPC/客户端三类 smoke 和 release 构建通过。真实 Redis 的 2 项发现、5 项所有权和 3 项多进程 smoke 均通过；临时数据库确认无遗留键后清理容器。


## 请求级锁实施验收（2026-10-20 Windows）

已实现 `tok-infra::locks::{Memory, Redis, RedisConfig}` 与 `tok-rpc::lock::{Config, Layer}`。Memory 限制活动键和并发获取容量，Redis 以唯一 token 和有限 TTL 获取、Lua 比较后释放；后端与 session 所有权独立。Layer 校验逻辑服务名与 bid，以获取开始时刻保守扣减租期，为执行和释放预留预算；对迟到的获取及执行结果再次检查实际耗时，不把不让出执行权的 future 的迟到成功视为按时完成。释放异常记录系统错误，保留原 handler Status 或非零 RpcError，成功业务转为 Unavailable。外层取消后仅依靠有限租期兜底，不增加自动续租、业务重试或强制 fencing。

示例 guild 在 Rust 组装代码中安装锁，默认 Memory，通过 `application_with_locks` 显式注入共享后端；player 和 GatewayRpc 不安装锁。未新增 ServiceConfig 锁开关或配置式中间件链。README、设计文档与 rustdoc 用户指南已同步，指南包含可编译的组装示例。

workspace 常规测试和 5 项文档测试通过；锁 Layer 最终定向验证 10 项，覆盖双节点真实 RPC、预算扣减、内部 readiness、业务错误、释放异常、超时和取消。5 项真实 Redis 测试通过，覆盖独立进程竞争、过期及旧持有者条件释放、断线释放失败、已写入但回应丢失后的未知结果与 TTL 兜底；测试仅使用独有命名空间和有限 TTL，不清空共享数据库。格式、workspace 全目标 Clippy、rustdoc、生命周期 / RPC / 客户端 smoke 及 release 构建通过。此次未重复执行已有发现、所有权及跨网关交接的真实 Redis 专项，相关组件行为未改动。Redis Cluster 客户端、NATS 和生产安全继续保留为后续工作，阶段五未整体完成。


## 同步本地事件实施验收（2026-10-20 Windows）

阶段六首个子批次已完成：`tok-events` 提供 `Event`、`EventId`、`Envelope` 和有界追踪头载体，保留游戏自定义目标 ID 类型；事件名称、投递策略与离线加载选项显式声明，跨节点数据契约不等同于已有传输实现。`local::Dispatcher` 按事件类型有界注册订阅者，显式借用当前业务状态，在原调用栈顺序执行，首个错误原样返回；没有队列、后台任务、隐式锁、自动去重或回滚。该批本地分发无需依赖 tok-core 或异步运行时；后续 NATS 适配另行引入异步依赖。

示例生产 timer 在原 player 调用内分发 `production::Completed`，产出业务独立到 production 模块；通过保存的生产时刻处理过时与重复触发，先完成可失败的计算再写状态，溢出错误沿原调用返回。客户端 protobuf、消息 ID 和 timer 协议未变。README、设计和 rustdoc 用户指南已同步，包含可编译的本地分发示例。

验收通过：151 项常规测试、6 项文档测试（合计 157 项），其中事件契约与本地分发新增 11 项测试，示例生产流程共 2 项。覆盖注册容量、无订阅者、顺序执行、首错短路与部分修改、重复调用、独立状态共享分发器且留在调用线程、ID 与目标及追踪载体边界，以及示例正常产出、过时/重复触发和溢出状态保护。格式、workspace 全目标 Clippy、rustdoc、生命周期 / RPC / 客户端 smoke 及 release 构建通过。本批未修改 Redis 后端行为，未重复执行依赖真实 Redis 的专项。

Core NATS、JetStream、网关事件入口、主题订阅、离线加载及交接订阅迁移仍待后续子批次，阶段六未整体完成。Redis Cluster 客户端继续独立保留为阶段五剩余工作。


## Core NATS 实施与链路校准（2026-10-20 Windows）

本地事件批次已提交为 `a38f55e`，此后的 Core NATS 及相关校准尚未提交。以下描述当前实现，不保留开发中间结构的兼容层。

### 通用事件与订阅

- Envelope<E> 用于强类型发布；Message 合并编码正文、稳定 event_id、事件名和追踪上下文，不包含玩家目标或客户端 message_id。回调取得原始 Arc<Message> 及已解码 E，转发复用原始编码。删除单独的 Payload 包装。
- Core NATS 使用 `tok.events.<namespace>.<event_type>` 精确类型订阅；Handler 声明兴趣，Registry 按稳定事件名注册异步处理者。无 group 为实例广播，同组实例分担匹配消息，不同组各自接收。group 显式配置，不从服务名推导，不提供 bid 归属路由或业务去重。
- 消费串行执行，handler 有有限等待期限。只保证单类型订阅内的接收顺序，跨类型、跨发布者及 group 不提供全局执行顺序。session 继续按实际准入顺序 FIFO 执行。
- 连接可承载多个逻辑订阅；正常结束、取消或丢弃订阅不关闭共享连接。SDK 慢消费者事件不能通过公开 Subscriber 接口精确映射，因此该连接的所有消费任务停止并上报，发布仍可使用；细粒度隔离使用独立 Bus。断线、发布入队失败或取消会保守停用连接，不重试业务消息。
- 订阅按 类型数 × subscription_capacity × max_message_bytes 保守预留缓冲预算，并限制活动类型订阅总数，资源不足在组装时拒绝；不是包含 SDK 元数据、在途解码和异常超大输入的进程内存硬限额。发布在占用容量后才编码。
- 发布成功只表示客户端接受操作，不逐条回环、不等待 broker 确认；断线仍可能丢失事件。仅订阅初始化保留有界临时 inbox 自回环，确认该连接的订阅命令已被 broker 处理，不等待 handler 完成或 player 执行完成。Outcome 区分当前 handler 的 Ignored、Handled、Rejected、Partial；错误与超时独立记录。指标不使用玩家或请求 ID 标签。

### 网关、会话与 RPC 边界

- Subscriber 将 Handler 与可选 group 组合为独立组件；网关不固定消费策略。内置 forward / notify 处理者提供玩家转发和广播，游戏也可注入自定义 Handler。示例分别组装两个组件，各节点独立订阅仅是本地会话筛选实现的选择；同组消费需自行保证目标归属路由。游戏订阅者按需创建本地 Delivery（Target + Action），通知直接进入下行缓存，转发共享 Message 排入 session FIFO，经所有权检查后执行。普通订阅回调不能绕过 session 并发修改 player 状态。客户端没有事件入口，普通请求和 timer 与事件分发表隔离。
- SessionManager::deliver 为异步，主题走反向索引，仅访问本地成员，每 64 个目标让出调度；Players 按 ID 查询，All 遍历本地会话。目录锁不与 slot 锁嵌套查询，通知通过最终成员和在线校验后才复制正文。
- 索引按完整会话身份保存弱引用。加入/退出原子维护，关闭及析构清理，恢复保留，交接重建；旧会话不能误删新代次，投递在让出调度后重验状态。skipped 不计入无关主题成员，已准入操作不因后续取消回滚。
- 事件订阅及连接由服务工厂持有，交给通用 GatewayContext / ServiceTask 监督，工厂显式增加后台任务容量；Ingress 不依赖 NATS，不限制单订阅，不新增 player 配置中的固定事件开关。
- PlayerRpc.HandleEvent 返回独立 EventResponse（序号、错误及 timer 更新）；Executor::execute_event 只返回可选 TimerUpdate，不包含客户端回应。NATS 发布者不等待此业务结果。主题及事件执行进度随初始版本 1 的交接快照迁移，客户端 Frame 保持版本 3；事件协议也保持初始版本 1，未发布字段可直接调整。
- RpcClients 只管理通用连接缓存、撤销和传输配置；rpc::player::Client（call、handle_event）与 rpc::business::Client（call）共享 Arc<RpcClients>。网关 RpcExecutor 注入玩家协议客户端，保留 deadline、追踪、序号校验及无自动重试语义。

### 本批范围与验收

不增加并发工作池、跨订阅排序、批量发布协议、自动重试、通用去重、离线加载、自动 bid 路由或任意游戏状态的自动迁移。JetStream 留待后续批次，必须先定义业务完成后的消费确认，不能直接把网关入队当作持久化处理完成。Redis Cluster 客户端仍为阶段五剩余项。

已通过 168 项常规测试和 7 项文档测试（合计 175 项），另通过 13 项真实 NATS 专项，覆盖同组分担/跨组独立/无组广播、类型过滤、伪造类型拒绝、缓冲预算释放、独立退订、超时不重试、断线/坏包/过载、无回环发布与订阅就绪、双网关分发与追踪。常规测试同时验证原始编码共享、会话 FIFO、主题索引生命周期、分批调度、交接和客户端入口隔离。组件校准后全目标 Clippy、rustdoc、格式及 NATS smoke 通过。此前 RPC / 客户端 smoke 和 release 构建已通过，本轮未重复执行。真实 Redis 专项未重复执行，未提供吞吐或延迟压测结论。

## P3 统一执行入口迁移（2026-10-20 Windows）

以 P2 提交 `c6ffdc3` 为基线，移除同步 local::Dispatcher，统一事件 Envelope、Registry 与 Tower 处理链；RPC、事件接收及 RPC timer 路由使用共同 Request / execution::Context。远程适配显式安装 protobuf 解码器，保留原始 Arc<Message>，本地可跨 await 借用状态。通过 rpc::Binding 在服务工厂中初始化并安装实际处理链，demo player/guild 已迁移；客户端 Frame 和 NATS subject 未改变。

本批工作区 214 项测试、全目标 Clippy、rustdoc、六个 smoke、13 项真实 NATS 及 5 项 Redis 发现/多进程专项通过。该记录仅覆盖 Windows 本地验证；release 和 Linux 未在本批运行。P3 已完成本批验收，P4–P7 仍按当前实施计划推进。
