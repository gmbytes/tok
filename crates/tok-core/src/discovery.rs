//! 服务发现快照与可合并的变更订阅契约。

/// 服务发现、目录查询与注册操作的错误，不包含锁或路由错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 发现参数不符合契约，附带原因。
    #[error("服务发现参数无效：{0}")]
    InvalidInput(String),
    /// 发现后端配置无效，不包含连接凭据。
    #[error("服务发现配置错误：{0}")]
    InvalidConfig(String),
    /// 注册与已有持有者冲突，附带不含凭据的原因。
    #[error("服务注册冲突：{0}")]
    RegistrationConflict(String),
    /// 服务注册资源容量耗尽。
    #[error("服务注册资源容量已耗尽")]
    CapacityExceeded,
    /// 本地目录状态不可用，例如同步锁损坏。
    #[error("服务目录本地状态不可用")]
    StateUnavailable,
    /// 后端、注册租约或快照同步不可用，也包括本地缓存过期。
    #[error("服务发现不可用")]
    Unavailable,
    /// 当前发现后端不支持指定可选能力。
    #[error("服务发现不支持此能力：{0}")]
    Unsupported(&'static str),
}
/// 服务发现领域的操作结果。
pub type Result<T> = std::result::Result<T, Error>;

use crate::BoxFuture;
use crate::ServiceDescriptor;

/// 服务就绪后的发布与退出时的条件撤销，与只读快照分开。
pub trait ServiceRegistry: Send + Sync {
    /// 发布已就绪服务；身份冲突返回 RegistrationConflict，容量耗尽返回 CapacityExceeded。
    fn register(&self, service: ServiceDescriptor) -> BoxFuture<'_, Result<()>>;
    /// 撤销本实例发布的服务，不能移除其他实例的记录。
    fn unregister<'a>(
        &'a self,
        node_id: &'a str,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<bool>>;
}

/// 订阅创建后再读取快照，避免快照与订阅之间漏掉变更。
/// 通知可以合并，调用方每次被唤醒后重新读取所需服务的快照。
pub trait DirectoryWatch: Send {
    /// 等待下一次变更；被唤醒后应重新读取快照，目录关闭时返回错误。
    fn changed(&mut self) -> BoxFuture<'_, Result<()>>;
}

/// 从本地缓存读取节点快照，不在业务请求路径查询注册后端。
/// 按游戏定义的服务名查询各节点上的服务。
///
pub trait ServiceDirectory: Send + Sync {
    /// 返回指定业务服务的当前节点快照，不在请求路径访问注册后端。
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>>;

    /// 后端暂不支持订阅时明确返回 Unsupported，不在请求路径查询外部后端。
    fn subscribe(&self) -> Result<Box<dyn DirectoryWatch>> {
        Err(Error::Unsupported("服务目录变更订阅"))
    }
}
