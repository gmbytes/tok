use std::net::SocketAddr;

/// 服务发现信息，地址并不意味着网络监听已经启动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDescriptor {
    /// 部署节点身份；服务由 (node_id, service_name) 定位。
    pub node_id: String,
    /// 节点内唯一的业务服务名，不是 player id、guild id 等业务 bid。
    pub service_name: String,
    pub address: SocketAddr,
}
