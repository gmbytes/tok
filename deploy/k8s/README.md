# Kubernetes 部署

生产环境使用 Kubernetes Service 与 EndpointSlice 发现节点。

当前已实现共享内部 gRPC 监听，客户端网关和 Kubernetes 发现尚未实现，因此暂不提供可部署清单。生产接入完成后，在此添加示例部署配置；Pod 使用相同的游戏二进制，通过不同配置启用服务。
