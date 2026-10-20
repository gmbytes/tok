//! 可注入的 session 所有权和请求级锁契约，不绑定 Redis。

use std::{future::Future, pin::Pin, time::Duration};

use crate::{Result, SessionIdentity};

/// 后端异步操作；具体实现必须明确取消和租约语义。
#[expect(
    clippy::module_name_repetitions,
    reason = "跨 crate 使用的协调任务类型，名称区分业务执行等其他异步任务。"
)]
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

/// 原子获取、条件转移、续租与释放。读取所有权不等同于持有租约。
///
/// 这是 session 驱动权租约，不是任意业务写入的严格互斥保证。续租失败后停止新调用；
/// 已发出的 RPC 在超时或所有权变化后仍可能运行，不默认要求每次存储写入校验 fencing token。
/// 后续存储封装按调用期限和本地已知租约期限做尽力检查，检查与实际写入间的竞态由系统约定接受。
pub trait OwnershipStore: Send + Sync {
    /// 快速重登：原子检查玩家最新登录的 session_id 后建立新会话。
    /// 新登录更新此标记，交接和租约释放保留；旧凭证不能借租约过期再次进入。
    /// 自定义后端需实现此原子操作才能支持快速重登，默认不支持。
    fn acquire_for_relogin(
        &self,
        _previous: crate::credentials::ResumeClaims,
        _session_id: String,
        _owner: SessionOwner,
        _expected_epoch: Option<u64>,
        _ttl: Duration,
    ) -> CoordinationTask<'_, SessionOwnership> {
        Box::pin(async { Err(crate::Error::NotImplemented("快速重登会话代次校验")) })
    }
    /// 仅在完整旧身份匹配且租约有效时转移；保留 session_id 并增加 epoch。
    /// 默认不支持，不能用先查询再 acquire 模拟完整身份比较。
    fn transfer(
        &self,
        _expected: SessionOwnership,
        _target: SessionOwner,
        _ttl: Duration,
    ) -> CoordinationTask<'_, SessionOwnership> {
        Box::pin(async { Err(crate::Error::NotImplemented("所有权条件转移")) })
    }
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

/// 锁的获取期限与租期由实现及组装策略决定；此契约不提供续租。
///
/// 获取结果不确定时不能重试写操作或假定已解锁；取消获取与丢弃租约依靠 TTL 兜底。
/// 调用方从获取开始按单调时钟保守扣减有效预算，不将 token 作为写入完成或 fencing 保证。
pub trait LockBackend: Send + Sync {
    /// 在 wait 竞争期限内获取实体锁，成功后返回有限 ttl 租约。
    /// wait 为零仍允许一次有界尝试；连接和后端操作还受实现自身的操作期限约束。
    /// 内置后端以 LockTimeout 表示竞争超时，以 LockCapacity 表示本地容量不足。
    fn acquire(
        &self,
        key: LockKey,
        wait: Duration,
        ttl: Duration,
    ) -> CoordinationTask<'_, Box<dyn LockLease>>;
}
