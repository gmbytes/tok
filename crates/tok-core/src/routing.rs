//! 将服务发现与业务 ID 选点交给组装层注入的路由契约。

/// 路由参数、选点与依赖目录查询的错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 路由服务名或业务 ID 无效。
    #[error("路由参数无效：{0}")]
    InvalidInput(String),
    /// 指定业务服务没有可调用节点，附带服务名。
    #[error("服务没有可用节点：{0}")]
    NoAvailableService(String),
    /// 服务目录查询失败，保留原始发现错误及错误链。
    #[error("路由查询服务目录失败：{0}")]
    Discovery(#[from] crate::discovery::Error),
}
/// 路由领域的操作结果。
pub type Result<T> = std::result::Result<T, Error>;

use crate::ServiceDescriptor;

/// 根据业务服务名和实体 ID 选择当前可调用的节点。
pub trait ServiceRouter: Send + Sync {
    /// 无可用节点时返回错误，不自动重试业务请求。
    fn route(&self, service_name: &str, bid: &str) -> Result<ServiceDescriptor>;
}
