use crate::{Result, ServiceDescriptor, ServiceKind};

/// 从本地缓存读取节点快照，不在业务请求路径查询注册后端。
///
/// 后端监听及变更通知将在发现组件实现时扩展。
pub trait ServiceDirectory: Send + Sync {
    fn snapshot(&self, kind: ServiceKind) -> Result<Vec<ServiceDescriptor>>;
}
