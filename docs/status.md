# 当前实现状态

核对日期：2026-10-21。P6 已提交为 `06d9dd2`，P7 应用组装与接口清理已提交为 `017a9ea`。框架的“服务 + 处理链”方向校正已完成[七批计划](implementation-plan.md)本轮验收，历史结果见[实施历史](implementation-history.md)；本页只维护当前能力和边界。

## crate 边界校正

新增 [tok-player](../crates/tok-player/src/lib.rs)，只依赖 tok-core，集中会话身份、逻辑消息、玩家计时需求、所有权及恢复凭证契约。所有权接口为 ownership::Store / Owner / Record，专用错误归 player::Error，通用错误以 Core 保留。RPC 的 CallContext 移至 [tok-rpc](../crates/tok-rpc/src/context.rs)。tok 门面增加 player 导出，不保留旧 core 路径别名。

tok-core 保留服务身份、通用执行上下文、计时触发来源、发现、路由与请求锁；tok-service 的 execution / lifecycle / task 和 tok-events 均不依赖 tok-player。gateway、RPC 与 infra 的玩家部分显式引用新契约。只调整 Rust API 归属，不更改客户端或内部 protobuf 线协议、Redis 键、租约与调用语义。本轮校正已完成验收，随本次提交交付；249 项工作区测试、Clippy、rustdoc、七个 smoke 和七项真实 Redis 专项通过。按用户要求未做 release 构建，验证范围见实施计划。

## 当前能力

| 范围 | 已实现与代码依据 | 边界 |
| --- | --- | --- |
| 应用组装 | [Application](../crates/tok/src/application.rs) 复用共同 Factory / Builder，配置驱动共享监听、发现、就绪与退出；[RPC 组装](../crates/tok/src/application/rpc.rs) 自动注入初始化绑定 | 自定义 protobuf 可逐服务绑定，或显式注册完整端点；同一端点不混用两种方式 |
| 无端点服务 | ServiceConfig.endpoint 可省略，纯事件/后台服务使用同一生命周期 | 不发布网络地址，不注入 application::Binding；需要发现查询时由组装层注入 |
| 执行与中间件 | [execution::Context](../crates/tok-core/src/execution/context.rs)、[Request / Tower Layer](../crates/tok-service/src/lib.rs) 用于 RPC、事件、timer 与网关消息链 | 保留具体协议、输入/结果类型、可信入口校验和调度边界，无通用路由器/熔断器 |
| 服务生命周期 | Factory / Init 初始化，Builder → Prepared → Running 消费式编排；task::Supervisor 驱动有界工作 | 主任务、后台故障及退出期限由所有者监督，不创建隐藏的任务运行时 |
| RPC | [tok-rpc](../crates/tok-rpc/src/lib.rs) 共享端点、类型化分发、追踪、请求级锁和无自动重试客户端 | RPC 工厂初始化必须安装处理链，未安装不发布；同一端点不能覆盖同名 protobuf 服务 |
| 事件 | [Registry](../crates/tok-events/src/registry.rs) 本地借用及远程解码共用处理表，有序多处理者、原始错误及 Arc 保留 | 本地 dispatch 显式等待；远程发布不等待业务结果，不提供事务或隐式去重 |
| topic / group | Event::topic 默认全局，可固定或由正文字段派生；[Pool](../crates/tok-events/src/interest.rs) 按组件/主题聚合固定与会话引用 | group 属于订阅组件；无组独立接收，同组分担不等于玩家归属路由 |
| NATS | [Bus](../crates/tok-events/src/nats.rs) 按 namespace、topic、类型安全路由，有限 SID、建立操作、缓冲与处理预算 | Core NATS 即时事件；发布只表示接受，订阅建立有限回环就绪；没有 JetStream |
| gateway / session | [Gateway](../crates/tok-gateway/src/server.rs) 作为共同设施的内置扩展，提供快照、状态工厂、生命周期观察及准入/执行/新下行 Layer | 通信、FIFO、timer、ACK、恢复与交接保留各自状态机；不强制普通服务具备 session |
| 动态成员 | 异步 join/leave 等待所选组件就绪，可信策略在登录前准备初始主题；目标交接提交前准备兴趣 | 宽限期与冻结期间保留兴趣；离线/恢复/冻结不投递即时事件，冻结窗口不补收 |
| 发现与协调 | Memory / Redis 发现、会话所有权、请求级锁；发现缓存和路由分离 | Memory 仅单进程；租约与超时是尽力保护，不保证严格 fencing 或恰好一次 |
| 工程与文档 | 十个框架 crate、demo、构建期 protobuf/消息 ID 生成、中文 rustdoc、workspace lint、LF、Windows/Linux CI | CI 配置不等于本地已执行 Linux 验证；真实外部服务与 release 结果分别记录 |

## 使用与失败边界

普通服务的构造仍是一套 Factory / Init，RPC、事件都通过 Request 和 Tower 执行。Application::register_rpc_factory 自动按配置创建入口，将 rpc::Binding 注入工厂；在工厂内初始化状态、路由和 Layer 后安装。初始化装配用的 rpc::Binding 不保存回自身执行扩展，避免引用环。Application::bind_rpc 用于自定义 protobuf 服务，监听继续由 Application 合并管理。

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
