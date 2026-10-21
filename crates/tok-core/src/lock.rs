//! 通用请求级锁契约，不绑定玩家会话或 Redis。

use crate::BoxFuture;
use std::time::Duration;

use crate::Result;

/// 不同业务服务通过命名空间隔离同名业务 ID 的锁。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    /// 业务服务的锁命名空间。
    pub namespace: String,
    /// 需要互斥访问的业务实体 ID。
    pub bid: String,
}

/// 显式释放的请求级租约；Drop 不保证完成异步释放。
pub trait Lease: Send {
    /// 后端发放的持有者标识，不能作为已完成业务写入的保证。
    fn token(&self) -> &str;
    /// 消费租约并异步条件释放，不能释放其他持有者的新锁。
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

/// 锁的获取期限与租期由实现及组装策略决定；此契约不提供续租。
///
/// 获取结果不确定时不能重试写操作或假定已解锁；取消获取与丢弃租约依靠 TTL 兜底。
/// 调用方从获取开始按单调时钟保守扣减有效预算，不将 token 作为写入完成或 fencing 保证。
pub trait Backend: Send + Sync {
    /// 在 wait 竞争期限内获取实体锁，成功后返回有限 ttl 租约。
    /// wait 为零仍允许一次有界尝试；连接和后端操作还受实现自身的操作期限约束。
    /// 内置后端以 LockWaitTimeout 表示竞争超时，以 LockCapacityExceeded 表示本地容量不足。
    fn acquire(
        &self,
        key: Key,
        wait: Duration,
        ttl: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn Lease>>>;
}
