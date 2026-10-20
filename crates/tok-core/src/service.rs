//! 部署节点上的业务服务身份与端点描述。

use std::net::SocketAddr;

/// 服务发现信息，地址并不意味着网络监听已经启动。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[expect(
    clippy::module_name_repetitions,
    reason = "从 crate 根导出的服务身份描述，保留服务角色以区分其他描述对象。"
)]
pub struct ServiceDescriptor {
    /// 部署节点身份；服务由 (node_id, service_name) 定位。
    pub node_id: String,
    /// 节点内唯一的业务服务名，不是 player id、guild id 等业务 bid。
    pub service_name: String,
    /// 绑定端点的协议名；多个服务可以共享同一地址。
    pub protocol: String,
    /// 绑定端点的地址；远程调用要求使用可连接的地址。
    pub address: SocketAddr,
}
