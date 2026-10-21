//! 玩家会话驱动权的租约契约，不绑定具体存储后端。
use crate::{Result, SessionIdentity};
use std::{future::Future, pin::Pin, time::Duration};

/// 所有权后端异步操作；实现必须明确取消和租约语义。
pub type Task<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// 所有权持有者，以部署节点和服务名定位。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    /// 持有会话所有权的部署节点。
    pub node_id: String,
    /// 节点内持有会话的网关服务名。
    pub service_name: String,
}

/// 当前可驱动 player 的会话及其所在网关。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// 玩家、逻辑会话及当前所有权代次。
    pub identity: SessionIdentity,
    /// 当前持有所有权的网关位置。
    pub owner: Owner,
}

/// 原子获取、条件转移、续租与释放。读取所有权不等同于持有租约。
///
/// 这是 session 驱动权租约，不是任意业务写入的严格互斥保证。续租失败后停止新调用；
/// 已发出的 RPC 在超时或所有权变化后仍可能运行，不默认要求每次存储写入校验 fencing token。
/// 后续存储封装按调用期限和本地已知租约期限做尽力检查，检查与实际写入间的竞态由系统约定接受。
pub trait Store: Send + Sync {
    /// 快速重登：原子检查玩家最新登录的 session_id 后建立新会话。
    /// 新登录更新此标记，交接和租约释放保留；旧凭证不能借租约过期再次进入。
    /// 自定义后端需实现此原子操作才能支持快速重登，默认不支持。
    fn acquire_for_relogin(
        &self,
        _previous: crate::credentials::ResumeClaims,
        _session_id: String,
        _owner: Owner,
        _expected_epoch: Option<u64>,
        _ttl: Duration,
    ) -> Task<'_, Record> {
        Box::pin(async {
            Err(tok_core::Error::NotImplemented("快速重登会话代次校验").into())
        })
    }
    /// 仅在完整旧身份匹配且租约有效时转移；保留 session_id 并增加 epoch。
    /// 默认不支持，不能用先查询再 acquire 模拟完整身份比较。
    fn transfer(&self, _expected: Record, _target: Owner, _ttl: Duration) -> Task<'_, Record> {
        Box::pin(async { Err(tok_core::Error::NotImplemented("所有权条件转移").into()) })
    }
    /// 读取未过期的所有权；查询结果不能代替获取或续租。
    fn lookup(&self, player_id: &str) -> Task<'_, Option<Record>>;

    /// 没有当前所有者时 expected_epoch 应为 None；接管时必须匹配当前 epoch。
    /// 成功时产生新的 epoch，调用方决定恢复原 session_id 或建立新会话。
    fn acquire(
        &self,
        player_id: String,
        session_id: String,
        owner: Owner,
        expected_epoch: Option<u64>,
        ttl: Duration,
    ) -> Task<'_, Record>;

    /// 仅在所有权仍匹配且未过期时续租，ttl 必须大于零。
    fn renew(&self, ownership: Record, ttl: Duration) -> Task<'_, ()>;
    /// 条件释放匹配的所有权；不匹配或不存在时返回 false。
    fn release(&self, ownership: Record) -> Task<'_, bool>;
}
