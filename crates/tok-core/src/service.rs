use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// 一个进程可以按配置启用多个服务角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceKind {
    OutsideGateway,
    InsideGateway,
    Player,
    GlobalManager,
    GlobalInstance,
    Coordinator,
    Scene,
}

/// 服务发现信息，地址并不意味着网络监听已经启动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDescriptor {
    /// 部署节点身份；服务由 (node_id, service_name) 定位。
    pub node_id: String,
    /// 节点内唯一的业务服务名，不是 player id、guild id 等业务 bid。
    pub service_name: String,
    pub kind: ServiceKind,
    pub address: SocketAddr,
}
