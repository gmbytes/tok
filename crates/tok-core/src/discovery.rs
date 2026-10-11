use crate::{Result, ServiceDescriptor};

/// 从本地缓存读取节点快照，不在业务请求路径查询注册后端。
/// 按游戏定义的服务名查询各节点上的服务。
///
/// 后端监听及变更通知将在发现组件实现时扩展。
pub trait ServiceDirectory: Send + Sync {
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>>;
}
