# Kubernetes 部署边界

Kubernetes Service / EndpointSlice 发现是生产目标，尚未实现；当前目录没有可运行部署清单。Kubernetes 配置通过校验不表示可以启动该后端，不能据此宣称已支持生产部署。

先按[七批实施计划](../../docs/implementation-plan.md)完成“服务 + 处理链”、共同运行设施及 gateway 扩展校正，再继续生产发现、安全与部署工作。后续不为 Kubernetes 再建立独立服务模型，gateway 和普通服务共用构建、就绪、停止与清理契约。

部署仍使用同一平台上的同一个游戏二进制，以配置选择服务与端点。一个 Pod 可承载多个服务，需要独立扩缩容时分开部署；不为每个 bid 或玩家创建 Pod。

后续清单需要覆盖可访问的发布地址、唯一节点与启动代次、共享端点、就绪撤销、优雅退出、资源限制、发现权限及选定的生产认证/安全配置。具体交付另行计划，本轮不生成占位 Deployment 或未实现探针。

开发多进程当前使用 Redis 发现与按需共享所有权，见 [Rust 用户指南](../../crates/tok/docs/guide.md) 和[实现状态](../../docs/status.md)。历史验证不能替代真实 Kubernetes 或 Linux 运行结果。
