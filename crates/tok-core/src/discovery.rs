//! 服务发现快照与可合并的变更订阅契约。

use crate::BoxFuture;
use crate::{Result, ServiceDescriptor};

/// 服务就绪后的发布与退出时的条件撤销，与只读快照分开。
pub trait ServiceRegistry: Send + Sync {
    /// 发布已就绪服务；身份冲突返回 RegistrationConflict，容量耗尽返回 RegistrationCapacityExceeded。
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
        Err(crate::Error::Unsupported("服务目录变更订阅"))
    }
}
