//! 服务发现快照与可合并的变更订阅契约。

use crate::{Result, ServiceDescriptor};
use std::{future::Future, pin::Pin};

/// 服务注册操作的异步结果，网络后端应设置有限期限。
pub type RegistrationTask<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// 服务就绪后的发布与退出时的条件撤销，与只读快照分开。
pub trait ServiceRegistry: Send + Sync {
    /// 发布已就绪服务；相同身份被其他实例占用时返回错误。
    fn register(&self, service: ServiceDescriptor) -> RegistrationTask<'_, ()>;
    /// 撤销本实例发布的服务，不能移除其他实例的记录。
    fn unregister<'a>(
        &'a self,
        node_id: &'a str,
        service_name: &'a str,
    ) -> RegistrationTask<'a, bool>;
}

/// 订阅创建后再读取快照，避免快照与订阅之间漏掉变更。
/// 通知可以合并，调用方每次被唤醒后重新读取所需服务的快照。
pub trait DirectoryWatch: Send {
    /// 等待下一次变更；被唤醒后应重新读取快照，目录关闭时返回错误。
    fn changed(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

/// 从本地缓存读取节点快照，不在业务请求路径查询注册后端。
/// 按游戏定义的服务名查询各节点上的服务。
///
pub trait ServiceDirectory: Send + Sync {
    /// 返回指定业务服务的当前节点快照，不在请求路径访问注册后端。
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>>;

    /// 后端暂不支持订阅时明确返回 NotImplemented，不在请求路径查询外部后端。
    fn subscribe(&self) -> Result<Box<dyn DirectoryWatch>> {
        Err(crate::Error::NotImplemented("服务目录变更订阅"))
    }
}
