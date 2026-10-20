//! 当前网关的有界不透明恢复凭证；身份有效期独立于 session 恢复窗口。

use super::PlayerConfig;
use crate::{ErrorCode, Result};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tok_core::SessionIdentity;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct Claims {
    pub(super) player_id: String,
    pub(super) session_id: String,
}

struct Record {
    claims: Claims,
    expires_at: Instant,
}

pub(super) struct ResumeTokens {
    records: Mutex<BTreeMap<String, Record>>,
    ttl: Duration,
    capacity: usize,
}

impl ResumeTokens {
    pub(super) fn new(config: &PlayerConfig) -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            ttl: Duration::from_millis(config.resume_token_ttl_ms),
            capacity: config.max_resume_tokens,
        }
    }

    pub(super) fn issue(&self, identity: &SessionIdentity) -> Result<String> {
        let mut records = self.records.lock().map_err(|_| ErrorCode::Internal)?;
        let now = Instant::now();
        records.retain(|_, record| record.expires_at > now);
        let replacing = records
            .values()
            .any(|record| record.claims.player_id == identity.player_id);
        if !replacing && records.len() >= self.capacity {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        // 两个系统随机 UUID v4 提供不透明凭证，不从身份、时间或递增编号推导。
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        records.retain(|_, record| record.claims.player_id != identity.player_id);
        records.insert(
            token.clone(),
            Record {
                claims: Claims {
                    player_id: identity.player_id.clone(),
                    session_id: identity.session_id.clone(),
                },
                expires_at: now + self.ttl,
            },
        );
        Ok(token)
    }

    pub(super) fn validate(&self, token: &str) -> Result<Claims> {
        if token.len() != 64 {
            return Err(ErrorCode::AuthenticationFailed.into());
        }
        let mut records = self.records.lock().map_err(|_| ErrorCode::Internal)?;
        let now = Instant::now();
        records.retain(|_, record| record.expires_at > now);
        records
            .get(token)
            .map(|record| record.claims.clone())
            .ok_or(ErrorCode::AuthenticationFailed.into())
    }
}

#[cfg(test)]
mod tests;
