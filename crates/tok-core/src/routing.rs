//! 将服务发现与业务 ID 选点交给组装层注入的路由契约。

use crate::{Result, ServiceDescriptor};

/// 根据业务服务名和实体 ID 选择当前可调用的节点。
pub trait ServiceRouter: Send + Sync {
    /// 无可用节点时返回错误，不自动重试业务请求。
    fn route(&self, service_name: &str, bid: &str) -> Result<ServiceDescriptor>;
}
