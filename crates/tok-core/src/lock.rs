//! 通用请求级锁契约，不绑定玩家会话或 Redis。

/// 请求锁配置、获取与释放错误，不包含发现或玩家所有权错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 锁键或时间参数无效。
    #[error("请求锁参数无效：{0}")]
    InvalidInput(String),
    /// 请求锁后端配置无效。
    #[error("请求锁配置错误：{0}")]
    InvalidConfig(String),
    /// 竞争等待超时，不表示后端 I/O 超时。
    #[error("等待请求锁超时")]
    WaitTimeout,
    /// 锁记录或并发获取容量已耗尽。
    #[error("请求锁资源容量已耗尽")]
    CapacityExceeded,
    /// 后端操作失败或超时，获取结果可能不确定，不包含凭据。
    #[error("请求锁后端不可用：{0}")]
    Unavailable(String),
}
/// 请求锁领域的操作结果。
pub type Result<T> = std::result::Result<T, Error>;

use crate::BoxFuture;
use std::time::Duration;

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
    /// 内置后端以 WaitTimeout 表示竞争超时，以 CapacityExceeded 表示本地容量不足。
    fn acquire(
        &self,
        key: Key,
        wait: Duration,
        ttl: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn Lease>>>;
}
