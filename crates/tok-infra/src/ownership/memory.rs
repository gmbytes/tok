//! 单进程开发用的有界内存 session 所有权后端。

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tok_player::{Error, Result};
use tok_player::{
    SessionIdentity,
    ownership::{
        Owner as SessionOwner, Record as SessionOwnership, Store as OwnershipStore,
        Task as OwnershipTask,
    },
};

struct Record {
    ownership: SessionOwnership,
    expires: Instant,
}

#[derive(Default)]
struct State {
    epoch: u64,
    latest_sessions: BTreeMap<String, String>,
    records: BTreeMap<String, Record>,
}

/// 有界内存所有权目录。epoch 在此后端生命周期内单调递增。
///
/// 不提供跨进程协调或重启恢复；调用方在租约失效时必须停止新业务驱动。
/// 最新登录标记也受容量限制；回收离线玩家标记后，其快速重登被拒绝，需重新普通鉴权。
#[expect(
    clippy::module_name_repetitions,
    reason = "公开类型保留后端或所有权角色名称，便于调用方区分 Memory 与 Redis 实现。"
)]
pub struct MemoryOwnership {
    capacity: usize,
    state: Mutex<State>,
}

impl MemoryOwnership {
    /// 创建具有有限记录容量的开发后端，capacity 必须大于零。
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Core(tok_core::Error::InvalidInput(
                "所有权容量必须大于零".into(),
            )));
        }
        Ok(Self {
            capacity,
            state: Mutex::new(State::default()),
        })
    }

    fn acquire_checked(
        &self,
        player_id: String,
        session_id: String,
        owner: SessionOwner,
        expected_epoch: Option<u64>,
        expected_session_id: Option<String>,
        ttl: Duration,
    ) -> OwnershipTask<'_, SessionOwnership> {
        Box::pin(async move {
            if [
                player_id.as_str(),
                session_id.as_str(),
                owner.node_id.as_str(),
                owner.service_name.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            {
                return Err(Error::Core(tok_core::Error::InvalidInput(
                    "玩家、会话和所有者身份不能为空".into(),
                )));
            }
            let mut state = self.state()?;
            let expires = expiration(ttl)?;
            state
                .records
                .retain(|_, record| record.expires > Instant::now());
            let current = state
                .records
                .get(&player_id)
                .map(|record| record.ownership.identity.epoch);
            if expected_session_id
                .as_ref()
                .is_some_and(|expected| state.latest_sessions.get(&player_id) != Some(expected))
            {
                return Err(Error::OwnershipConflict);
            }
            if current != expected_epoch {
                return Err(Error::OwnershipConflict);
            }
            if current.is_none() && state.records.len() >= self.capacity {
                return Err(Error::Core(tok_core::Error::CoordinationUnavailable(
                    "所有权容量已满".into(),
                )));
            }
            let next_epoch = state.epoch.checked_add(1).ok_or_else(|| {
                Error::Core(tok_core::Error::CoordinationUnavailable(
                    "epoch 已耗尽".into(),
                ))
            })?;
            // 只缓存有限玩家的最新登录标记；回收历史后拒绝其快速重登，需普通鉴权。
            if !state.latest_sessions.contains_key(&player_id)
                && state.latest_sessions.len() >= self.capacity
            {
                let inactive = state
                    .latest_sessions
                    .keys()
                    .find(|player| !state.records.contains_key(*player))
                    .cloned()
                    .ok_or_else(|| {
                        Error::Core(tok_core::Error::CoordinationUnavailable(
                            "登录标记容量已满".into(),
                        ))
                    })?;
                state.latest_sessions.remove(&inactive);
            }
            state
                .latest_sessions
                .insert(player_id.clone(), session_id.clone());
            state.epoch = next_epoch;
            let ownership = SessionOwnership {
                identity: SessionIdentity {
                    player_id: player_id.clone(),
                    session_id,
                    epoch: state.epoch,
                },
                owner,
            };
            state.records.insert(
                player_id,
                Record {
                    ownership: ownership.clone(),
                    expires,
                },
            );
            Ok(ownership)
        })
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| {
            Error::Core(tok_core::Error::CoordinationUnavailable(
                "内存所有权锁不可用".into(),
            ))
        })
    }
}

fn expiration(ttl: Duration) -> Result<Instant> {
    if ttl.is_zero() {
        return Err(Error::Core(tok_core::Error::InvalidInput(
            "租约期限必须大于零".into(),
        )));
    }
    Instant::now()
        .checked_add(ttl)
        .ok_or_else(|| Error::Core(tok_core::Error::InvalidInput("租约期限过大".into())))
}

impl OwnershipStore for MemoryOwnership {
    fn transfer(
        &self,
        expected: SessionOwnership,
        target: SessionOwner,
        ttl: Duration,
    ) -> OwnershipTask<'_, SessionOwnership> {
        Box::pin(async move {
            let expires = expiration(ttl)?;
            if target.node_id.trim().is_empty() || target.service_name.trim().is_empty() {
                return Err(Error::Core(tok_core::Error::InvalidInput(
                    "目标网关身份不能为空".into(),
                )));
            }
            let mut state = self.state()?;
            let key = &expected.identity.player_id;
            if !state.records.get(key).is_some_and(|record| {
                record.ownership == expected && record.expires > Instant::now()
            }) {
                return Err(Error::OwnershipConflict);
            }
            state.epoch = state.epoch.checked_add(1).ok_or_else(|| {
                Error::Core(tok_core::Error::CoordinationUnavailable(
                    "epoch 已耗尽".into(),
                ))
            })?;
            let ownership = SessionOwnership {
                identity: SessionIdentity {
                    epoch: state.epoch,
                    ..expected.identity
                },
                owner: target,
            };
            state.records.insert(
                ownership.identity.player_id.clone(),
                Record {
                    ownership: ownership.clone(),
                    expires,
                },
            );
            Ok(ownership)
        })
    }
    fn lookup(&self, player_id: &str) -> OwnershipTask<'_, Option<SessionOwnership>> {
        let player_id = player_id.to_owned();
        Box::pin(async move {
            let mut state = self.state()?;
            state
                .records
                .retain(|_, record| record.expires > Instant::now());
            Ok(state
                .records
                .get(&player_id)
                .map(|record| record.ownership.clone()))
        })
    }

    fn acquire(
        &self,
        player_id: String,
        session_id: String,
        owner: SessionOwner,
        expected_epoch: Option<u64>,
        ttl: Duration,
    ) -> OwnershipTask<'_, SessionOwnership> {
        self.acquire_checked(player_id, session_id, owner, expected_epoch, None, ttl)
    }
    fn acquire_for_relogin(
        &self,
        previous: tok_player::credentials::ResumeClaims,
        session_id: String,
        owner: SessionOwner,
        expected_epoch: Option<u64>,
        ttl: Duration,
    ) -> OwnershipTask<'_, SessionOwnership> {
        self.acquire_checked(
            previous.player_id,
            session_id,
            owner,
            expected_epoch,
            Some(previous.session_id),
            ttl,
        )
    }

    fn renew(&self, ownership: SessionOwnership, ttl: Duration) -> OwnershipTask<'_, ()> {
        Box::pin(async move {
            let mut state = self.state()?;
            let expires = expiration(ttl)?;
            let record = state
                .records
                .get_mut(&ownership.identity.player_id)
                .ok_or(Error::OwnershipConflict)?;
            if record.ownership != ownership || record.expires <= Instant::now() {
                return Err(Error::OwnershipConflict);
            }
            record.expires = expires;
            Ok(())
        })
    }

    fn release(&self, ownership: SessionOwnership) -> OwnershipTask<'_, bool> {
        Box::pin(async move {
            let mut state = self.state()?;
            let key = &ownership.identity.player_id;
            if state
                .records
                .get(key)
                .is_some_and(|record| record.ownership == ownership)
            {
                state.records.remove(key);
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
}
