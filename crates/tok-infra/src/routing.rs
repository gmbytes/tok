//! 与发现后端独立的业务 ID 路由。

use std::sync::Arc;
use tok_core::routing::{Error, Result};
use tok_core::{ServiceDescriptor, ServiceDirectory};

/// 使用注入目录快照的稳定 rendezvous 路由器。
pub struct RendezvousRouter {
    directory: Arc<dyn ServiceDirectory>,
}

impl RendezvousRouter {
    /// 创建路由器，每次选点读取当前本地快照。
    pub fn new(directory: Arc<dyn ServiceDirectory>) -> Self {
        Self { directory }
    }
}

impl tok_core::ServiceRouter for RendezvousRouter {
    fn route(&self, service_name: &str, bid: &str) -> Result<ServiceDescriptor> {
        route(self.directory.as_ref(), service_name, bid)
    }
}

/// 使用稳定的 rendezvous hash 选择目标，输入快照顺序不影响结果。
///
/// FNV-1a 64 位用于亲和性计算，不用于安全目的。节点身份使用长度前缀编码；
/// 节点删除时仅原本选中该节点的 bid 迁移。路由不自动重试业务调用。
pub fn route(
    directory: &dyn ServiceDirectory,
    service_name: &str,
    bid: &str,
) -> Result<ServiceDescriptor> {
    if service_name.trim().is_empty() || bid.trim().is_empty() {
        return Err(Error::InvalidInput("路由需要服务名与业务 ID".into()));
    }
    directory
        .snapshot(service_name)?
        .into_iter()
        .filter(|service| service.service_name == service_name)
        .max_by_key(|service| (score(service, bid), service.node_id.clone()))
        .ok_or_else(|| Error::NoAvailableService(service_name.into()))
}

fn score(service: &ServiceDescriptor, bid: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for part in [
        service.service_name.as_bytes(),
        bid.as_bytes(),
        service.node_id.as_bytes(),
    ] {
        for byte in (part.len() as u64).to_le_bytes().iter().chain(part) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}
