//! 可注入的 session 所有权和请求级锁契约，不绑定 Redis。

use std::{future::Future, pin::Pin, time::Duration};

use crate::{Result, SessionIdentity};

/// 后端异步操作；具体实现必须明确取消和租约语义。
pub type CoordinationTask<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// 所有权持有者，以部署节点和服务名定位。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOwner {
    /// 持有会话所有权的部署节点。
    pub node_id: String,
    /// 节点内持有会话的网关服务名。
    pub service_name: String,
}

/// 当前可驱动 player 的会话及其所在网关。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOwnership {
    /// 玩家、逻辑会话及当前所有权代次。
    pub identity: SessionIdentity,
    /// 当前持有所有权的网关位置。
    pub owner: SessionOwner,
}

/// 原子获取、续租与条件释放。读取所有权不等同于持有租约。
///
/// 这是 session 驱动权租约，不是任意业务写入的严格互斥保证。续租失败后停止新调用；
/// 已发出的 RPC 在超时或所有权变化后仍可能运行，不默认要求每次存储写入校验 fencing token。
/// 后续存储封装按调用期限和本地已知租约期限做尽力检查，检查与实际写入间的竞态由系统约定接受。
pub trait OwnershipStore: Send + Sync {
    /// 读取未过期的所有权；查询结果不能代替获取或续租。
    fn lookup(&self, player_id: &str) -> CoordinationTask<'_, Option<SessionOwnership>>;

    /// 没有当前所有者时 expected_epoch 应为 None；接管时必须匹配当前 epoch。
    /// 成功时产生新的 epoch，调用方决定恢复原 session_id 或建立新会话。
    fn acquire(
        &self,
        player_id: String,
        session_id: String,
        owner: SessionOwner,
        expected_epoch: Option<u64>,
        ttl: Duration,
    ) -> CoordinationTask<'_, SessionOwnership>;

    /// 仅在所有权仍匹配且未过期时续租，ttl 必须大于零。
    fn renew(&self, ownership: SessionOwnership, ttl: Duration) -> CoordinationTask<'_, ()>;
    /// 条件释放匹配的所有权；不匹配或不存在时返回 false。
    fn release(&self, ownership: SessionOwnership) -> CoordinationTask<'_, bool>;
}

/// 不同业务服务通过命名空间隔离同名业务 ID 的锁。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockKey {
    /// 业务服务的锁命名空间。
    pub namespace: String,
    /// 需要互斥访问的业务实体 ID。
    pub bid: String,
}

/// 显式释放的请求级租约；Drop 不保证完成异步释放。
pub trait LockLease: Send {
    /// 后端发放的持有者标识，不能作为已完成业务写入的保证。
    fn token(&self) -> &str;
    /// 消费租约并异步条件释放，不能释放其他持有者的新锁。
    fn release(self: Box<Self>) -> CoordinationTask<'static, ()>;
}

/// 锁的获取期限、租期和续租能力由实现及组装策略决定。
pub trait LockBackend: Send + Sync {
    /// 在 wait 期限内获取指定实体的锁，成功后返回租期为 ttl 的租约。
    fn acquire(
        &self,
        key: LockKey,
        wait: Duration,
        ttl: Duration,
    ) -> CoordinationTask<'_, Box<dyn LockLease>>;
}
