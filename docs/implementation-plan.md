# tok 实施大纲

## 实施目标与范围

当前项目已接通局外请求链路、在线 timer 调度、Redis 开发发现与共享 session 所有权：客户端登录后，经串行 session 调用 player，收到业务响应，并可由 player 调用 guild。下一步实现下行序列与缓存、同网关恢复和跨网关交接，再推进跨节点事件、Kubernetes 部署和场景接入。

本大纲以 2026-10-12 的代码为基线，遵循 [设计文档](design.md)。各阶段的技术选择属于实施建议；已经确定的执行语义继续以设计文档为准。阶段完成以可运行示例和验收结果为依据，不以模块或接口存在为依据。

首个可用里程碑是：客户端登录后，经 PlayerGateway 的串行 session 调用 player，收到 Response；同一进程的 player 和 guild 共用一个真实 gRPC 监听端点。存储细节、跨实体事务和无缝场景切换保持后续独立设计的边界。

## 批次校准与验收状态

网关组装已校准为统一 Gateway 运行框架加可配置接入组件。当前重构拆出监听、连接容量与任务监督，PlayerIngress 保留玩家登录和串行 session，Scene 按后续阶段实现；服务配置显式选择组件，业务服务名不承担类型判断。重构期间保留 PlayerGateway 兼容接口，验收覆盖现有 TCP 和 Redis 行为及自定义组件的生命周期。

第一批按“内部 RPC 基础可运行”完成；阶段零至二的全部生产能力仍按各自验收项推进。第二批以 [局外 TCP 协议与 session 实施约定](client-session.md) 为编码和验收依据。

| 范围 | 已实现或验证 | 剩余边界 |
| --- | --- | --- |
| 内部 RPC | 共享监听、强类型分发、连接复用、超时不重试及追踪传播已验证 | 上下文字段校验不代替内部调用身份认证 |
| 服务发现与路由 | 内存与 Redis 开发快照、合并通知、后端注入和稳定 bid 选点已验证 | Channel 失效入口为显式调用；生产监听与自动失效策略随生产发现补齐 |
| 并发和过载 | 用户 Tower 并发限制、session 队列满快速失败经测试，连接与下行容量有界 | 未默认配置 RPC 过载拒绝层；生产容量仍需负载验证 |
| 协调与会话 | 串行 session、Memory 与 Redis 所有权、epoch 比较、条件续租释放、双网关竞争及失效收束已验证 | Cluster 客户端与完整交接待实现；接受超时后远端继续执行的竞态窗口 |
| 开发检查 | Windows 常规检查、文档、smoke 和 release 构建通过 | Linux 以对应 CI 的实际运行结果为准，不能由本地 Windows 验证替代 |
| 客户端协议 | TCP 分帧、控制帧、错误映射与认证身份绑定已实现 | ack、补发、timer 调度和恢复在第三批 |

第二批的交付顺序是协议与错误码、认证契约、session 执行器、TCP 适配、示例业务与客户端 smoke。重复登录必须收束旧调用后建立新入口；断线立即关闭的本批行为在第三批扩展为宽限期保留和恢复。

## 当前实现与缺口

| 能力 | 实现现状与代码依据 | 主要缺口 |
| --- | --- | --- |
| 工程与构建 | Cargo workspace 包含八个框架 crate 和 demo-game；just、rustdoc 用户与 API 文档及双平台 CI 已配置 | 外部依赖和生产环境的集成验证 |
| 配置 | [config.rs](../crates/tok/src/config.rs) 校验节点、端点、服务引用、可观测及 player_gateway 容量和期限参数 | 中间件、生产接入安全及外部依赖配置 |
| 服务生命周期 | [runtime.rs](../crates/tok/src/runtime.rs) 提供工厂注册、共享端点、就绪入口、初始化期限及失败清理 | 生产依赖就绪策略与发现后端 |
| 发现 | [MemoryDirectory](../crates/tok-infra/src/discovery/memory.rs) 支持单进程；[RedisDirectory](../crates/tok-infra/src/discovery/redis.rs) 提供多进程开发租约注册、条件撤销、本地缓存与合并通知 | Kubernetes 本地缓存与资源映射；Redis 发现不代替 session 所有权 |
| 协议与 RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 提供 PlayerRpc、BusinessRpc、强类型分发、连接复用、超时、追踪传播和 TowerHandler | 配置式中间件、锁 Layer 及生产调用身份认证 |
| 网关 | [Gateway](../crates/tok-gateway/src/runtime.rs) 统一接收、容量及监督；[PlayerIngress](../crates/tok-gateway/src/player/ingress.rs) 提供 TCP 认证、串行 session、所有权续租、timer 调度和下行通知 | 下行缓存与恢复、生产安全及 scene 接入 |
| 路由与 Redis | bid rendezvous 路由、Memory 与 Redis 所有权、条件续租释放及本地时间预算检查已实现 | Cluster 拓扑与故障切换适配、交接及请求级锁后端 |
| 事件 | [tok-events](../crates/tok-events/src/lib.rs) 已有 DeliveryPolicy 和 EventTarget | 同步本地分发、NATS 后端、订阅、去重及交接处理 |
| 可观测 | 日志动态过滤、Prometheus、OTLP、强追踪规则、RPC metadata 传播及调用指标已实现 | 事件 headers 传播及 session、timer 指标 |
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
已实现基础后端不代表第三批完成：timer、下行序列、恢复及跨网关交接仍待推进，请求级锁是另一套契约。

系统接受有限的竞态窗口：调用期限与本地已知租约期限做尽力判断，续租失败或剩余预算不足时停止新驱动，不自动重试，也不强制逐次远程校验或存储 fencing。
RPC 超时后远端仍可能执行，存储层落地时统一加入写入前期限检查；业务按明确需求增加幂等、条件更新或事务。Redis 状态回退不作为绝对互斥保证。
验证入口为 just ownership-test 与 just ownership-smoke，常规测试不依赖 Redis；CI 独立 Redis 任务包含这些检查。

2026-10-19 Windows 验证：Rust 1.99.0 与 just 1.58.0，55 项常规测试、3 项文档测试通过；独立 Redis 7.4 容器通过 7 项集成测试（发现后端 2 项、发现双进程 1 项、所有权后端 3 项、独立网关所有权 1 项）。格式、clippy、中文 rustdoc、原有三类 smoke、两份 Redis 开发配置及两份生产配置校验、release 构建均通过。Linux 以 CI 实际结果为准。

## 统一网关运行框架重构

设计与实现已统一为 Gateway 加接入组件：运行时不判断 Player 或 Scene，GatewayTransport 提供连接，GatewayIngress 处理业务连接，GatewayContext 提交有界后台任务。PlayerIngress 保留原登录、串行 session、所有权与通知行为，PlayerGateway 保留兼容包装。

服务配置使用可选 factory 指定初始化工厂，省略时按 service_name 查找，普通 player 和 guild 无需重复填写；示例网关显式选择 player-ingress。Application::register_factory 按名称注册并复用工厂，不再提供 gateway 或 component 配置字段。具体协议与端点共享规则由组装层检查，scene-ingress 仍明确报尚未实现。现有 player_gateway 参数保留，PlayerConfig 派生通用运行时容量。连接普通错误仅关闭连接，panic 与后台失败由网关统一监督；退出取消入口并收束任务，组件负责清理。

新增测试覆盖非 TCP 自定义组件、连接与后台任务容量、普通错误、panic、后台失败、正常及强制退出，以及组件配置与自定义服务名。既有 TCP 和 Redis 多进程测试用于验证行为兼容。此重构不代表第三批 timer、恢复或交接已完成。

2026-10-19 Windows 验证：Rust 1.99.0、just 1.58.0，60 项常规测试、3 项文档测试与 7 项真实 Redis 集成测试通过；格式、clippy、中文 rustdoc、生命周期及 RPC、TCP 三类 smoke、四份部署配置校验和 release 构建通过。临时 Redis 数据库无遗留数据，容器已清理；Linux 结果以 CI 为准。

同日组件选择校准后，61 项常规测试与 3 项文档测试通过，新增同一工厂创建两个不同身份服务的生命周期验证；格式、clippy 和中文 rustdoc 通过。重新执行 Redis 双进程发现与多网关所有权两项集成测试、三类 smoke、四份配置校验及 release 构建，全部通过。临时 Redis 数据库无遗留数据，容器已清理。

工厂选择最终简化为可选 factory：默认按服务名查找，显式填写时优先使用指定工厂；示例普通业务配置省去重复字段。61 项常规测试、3 项文档测试、格式、clippy、rustdoc、三类 smoke、四份配置校验及 release 构建通过。Redis 所有权联调首次发生一次查询超时，未修改参数的连续两次重跑通过；发现双进程联调通过。该次超时暂未复现，临时 Redis 容器均已清理。

## 设计约束与实施依赖

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

第三批第一步已实现在线 timer：HandlerReply 保留可选计时更新，PlayerReply 交给 session 应用；ReplaceAll、Upsert、Cancel 原子校验，旧版本的排队触发失效；同一有界队列串行执行客户端与计时任务，繁忙时合并周期，失败或超时不重试。CallContext.timer 区分内部来源，强类型 timer 分发表拒绝客户端仅凭消息 ID 触发。数量、ID 和正文总字节有限，非法更新关闭 session。示例 START_PRODUCTION 只发起业务操作，服务端决定 timer 与产出，内部 FINISH_PRODUCTION 结算状态，client-smoke 覆盖完整链路。

本步未接通断线宽限期、下行序列、ack、补发、恢复或交接，断线仍停止 timer；也未增加隐式 player 登录调用，当前由业务响应安装计时集合。下一步先统一下行序列与缓存，再接同网关恢复。

2026-10-19 Windows 验收：Rust 1.99.0、just 1.58.0，73 项常规测试、4 项文档测试与 7 项真实 Redis 集成测试通过；新增 10 项调度及会话测试、2 项 RPC 测试和 1 项文档测试，客户端既有用例增加计时状态变更与来源隔离。格式、clippy、中文 rustdoc、三类 smoke、四份部署配置校验及 release 构建通过。Redis 数据库无遗留测试数据，临时容器已清理。PlayerExecutor 结果改为 PlayerReply，CallContext 增加可选 timer 来源，迁移方式见 Rust 用户指南；Linux 结果以 CI 为准。

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

当前已完成 Redis 所有权基础后端、双网关竞争拒绝和失效入口收束；本阶段的 Cluster 客户端、完整交接和请求级锁仍未完成。

- 实现 Redis 所有权记录、原子获取与 epoch 更新、续租、条件释放和旧所有者失效；明确 Redis Cluster 键布局与原子操作范围。
- session 开始业务执行前检查所有权有效性。Redis 不可达、续租失败或租约不确定时停止新的有效驱动，避免继续假定自己拥有会话。
- 设计并实现交接状态机与内部 RPC：旧网关停止接收新业务并收束在途调用，导出下行缓存、确认位置、timer 状态和必要订阅信息，新网关取得所有权后接续驱动。
- 明确交接期间新通知和未完成请求的处理边界；原 session_id 和消息序列仅在完整恢复时保留，交接失败使用 ResumeUnavailable 和快速重登。
- 实现 Redis 请求级锁与注入后端的 Tower Layer；按服务命名空间和 bid 加锁，确定获取期限、租约、续租与条件释放规则。
- 锁租约丢失和 RPC 超时都不能视为业务回滚；业务写入需要更强保证时使用业务版本或其他校验，具体持久化方案另行设计。

**验收：** 覆盖双网关竞争、重复登录、旧 epoch、Redis 故障、旧网关宕机及交接中断；检测到租约失效或预算不足后停止新驱动，已发出或检查后暂停的调用遵循约定的竞态边界。正常交接保留序列与 timer，不丢交接边界内的下行。请求级锁验证同 bid 的正常路径互斥、不同 bid 并发和旧持有者不能释放新锁，不宣称异常路径下绝对安全。

## 阶段六 本地事件与 NATS 事件

**目标：** 按既定投递语义接通服务间通知。

- 同步本地分发器在当前 player 调用内执行，可提前随阶段三落地。
- 定义稳定 event_id、事件类型、目标、DeliveryPolicy、离线加载选项及追踪上下文；明确频道加入、退出和交接规则。
- Core NATS 提供即时投递；广播进入下行序列，转发进入 session 执行队列。离线玩家默认不拉起、不积压。
- JetStream 按事件类型配置存储与消费确认，重复发布沿用 event_id；框架提供有界去重辅助，业务幂等承担跨进程和资产变更的重复处理责任。
- 定义持久化转发事件的消费确认时机和失败处理，区分进入网关队列与完成业务执行；同时明确默认离线不消费与持久化保留策略如何配合。
- 允许离线加载的事件也必须获取所有权并串行驱动，明确临时 session 的释放条件。

**验收：** 指定玩家、频道和全体目标正确分发；广播不调用 player；转发与请求、timer 串行；验证重复、重新投递、离线跳过和临时加载。事件消费确认与客户端 ack_seq 各自独立。

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
| 事件确认、频道迁移及离线加载释放 | 阶段六 | 消费生命周期和各类事件策略 |
| Kubernetes 资源映射与生产安全组合 | 阶段七 | 发现配置、部署清单与安全配置 |
| 场景定位和恢复 | 阶段八 | 局内协议及最小示例 |

队列、包长、缓存和期限必须可配置并具有有限默认值；具体数值通过实际负载验证确定，不在当前大纲中预设吞吐承诺。

## 交付与推进方式

第一批已接通基础类型与协调契约、内存开发后端、RPC service、共享监听、连接复用与调用验证，同时加入 Rust 原生用户文档生成。第二批已实现注入认证、串行 session、TCP 接入及示例客户端闭环，并完成本地验收。第三批已接通在线 timer 与 Redis 所有权基础，接下来推进下行序列、缓存、同网关恢复和跨网关交接；锁 Layer、NATS、生产安全和 Kubernetes 仍按后续阶段推进。

每个阶段按“契约与协议 → 组件实现 → 示例接通 → 故障与边界验证 → 文档更新”拆分变更。优先开始阶段零至三，得到可运行的局外链路后，再推进恢复与集群能力。

每次增加真实组件，同步更新 README 的当前能力说明和设计文档中的实现状态；新增业务 smoke 不能替换已有生命周期异常测试。Redis、NATS 和 Kubernetes 的集成验证显式声明依赖与运行方式，常规单元测试保持可独立运行。

最小交付顺序如下：

- 第一批：工具链基线、基础契约、RPC service 定义、共享端点及连接调用。
- 第二批：认证扩展、串行 session、TCP 接入、示例业务及真实客户端 smoke。
- 第三批：timer、下行序列、同网关恢复、Redis 所有权及跨网关交接。
- 第四批：跨节点事件、生产发现与部署、安全适配及故障验证。
- 第五批：局内场景接入；存储方案和无缝切换另开设计与实施计划。
