//! 单进程开发用的有界内存 session 所有权后端。

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tok_core::{
    Error, Result, SessionIdentity,
    coordination::{CoordinationTask, OwnershipStore, SessionOwner, SessionOwnership},
};

struct Record {
    ownership: SessionOwnership,
    expires: Instant,
}

#[derive(Default)]
struct State {
    epoch: u64,
    records: BTreeMap<String, Record>,
}

/// 有界内存所有权目录。epoch 在此后端生命周期内单调递增。
///
/// 不提供跨进程协调或重启恢复；调用方在租约失效时必须停止新业务驱动。
pub struct MemoryOwnership {
    capacity: usize,
    state: Mutex<State>,
}

impl MemoryOwnership {
    /// 创建具有有限记录容量的开发后端，capacity 必须大于零。
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::InvalidInput("所有权容量必须大于零".into()));
        }
        Ok(Self {
            capacity,
            state: Mutex::new(State::default()),
        })
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::CoordinationUnavailable("内存所有权锁不可用".into()))
    }
}

fn expiration(ttl: Duration) -> Result<Instant> {
    if ttl.is_zero() {
        return Err(Error::InvalidInput("租约期限必须大于零".into()));
    }
    Instant::now()
        .checked_add(ttl)
        .ok_or_else(|| Error::InvalidInput("租约期限过大".into()))
}

impl OwnershipStore for MemoryOwnership {
    fn lookup(&self, player_id: &str) -> CoordinationTask<'_, Option<SessionOwnership>> {
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
    ) -> CoordinationTask<'_, SessionOwnership> {
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
                return Err(Error::InvalidInput("玩家、会话和所有者身份不能为空".into()));
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
            if current != expected_epoch {
                return Err(Error::OwnershipConflict);
            }
            if current.is_none() && state.records.len() >= self.capacity {
                return Err(Error::CoordinationUnavailable("所有权容量已满".into()));
            }
            state.epoch = state
                .epoch
                .checked_add(1)
                .ok_or_else(|| Error::CoordinationUnavailable("epoch 已耗尽".into()))?;
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

    fn renew(&self, ownership: SessionOwnership, ttl: Duration) -> CoordinationTask<'_, ()> {
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

    fn release(&self, ownership: SessionOwnership) -> CoordinationTask<'_, bool> {
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
