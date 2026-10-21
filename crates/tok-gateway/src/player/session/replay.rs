//! 每个逻辑会话的有界下行字节缓存与连续范围查询，由连接恢复流程按原字节补发。

use crate::player::Config;
use crate::{ErrorCode, Result};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::time::Instant;

/// 某次查询时连续完整的下行快照，仅含原始 protobuf Frame 正文。
///
/// 单独查询不会确认消息或触发发送，连接恢复流程使用此快照保持补发顺序。
#[derive(Debug)]
pub struct ReplayBatch {
    /// 本快照包含的最大发布序号；frames 为空时也保留查询时的位置。
    pub through_seq: u64,
    /// 确认位置之后按序排列的编码正文，不含长度前缀或连接加密封装。
    pub frames: Vec<Arc<[u8]>>,
}

struct Entry {
    seq: u64,
    published_at: Instant,
    body: Arc<[u8]>,
}

pub(super) struct ReplayCache {
    entries: VecDeque<Entry>,
    cached_bytes: usize,
    // 已发布的最大下行序号；淘汰缓存不会回退。
    published_seq: u64,
    // 客户端累计确认位置。
    acknowledged_seq: u64,
    // 可查询补发的最小确认位置；更早的位置已有消息缺失。
    replay_floor_seq: u64,
    ttl: Duration,
    max_entries: usize,
    max_bytes: usize,
}

impl ReplayCache {
    pub(super) fn snapshot(&mut self) -> tok_game_rpc::protocol::internal::v1::ChannelSnapshot {
        self.evict_expired();
        tok_game_rpc::protocol::internal::v1::ChannelSnapshot {
            published_seq: self.published_seq,
            acknowledged_seq: self.acknowledged_seq,
            replay_floor_seq: self.replay_floor_seq,
            entries: self
                .entries
                .iter()
                .map(|entry| tok_game_rpc::protocol::internal::v1::ReplayEntry {
                    seq: entry.seq,
                    body: entry.body.to_vec(),
                    remaining_ms: self
                        .ttl
                        .saturating_sub(entry.published_at.elapsed())
                        .as_millis() as u64,
                })
                .collect(),
            ..Default::default()
        }
    }
    pub(super) fn restore(
        config: &Config,
        snapshot: &tok_game_rpc::protocol::internal::v1::ChannelSnapshot,
        elapsed: Duration,
    ) -> Result<Self> {
        let mut cache = Self::new(config);
        if snapshot.acknowledged_seq > snapshot.replay_floor_seq
            || snapshot.replay_floor_seq > snapshot.published_seq
            || snapshot.entries.len() > config.replay_capacity
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        cache.published_seq = snapshot.published_seq;
        cache.acknowledged_seq = snapshot.acknowledged_seq;
        cache.replay_floor_seq = snapshot.replay_floor_seq;
        let mut previous = snapshot.replay_floor_seq;
        for entry in &snapshot.entries {
            if previous.checked_add(1) != Some(entry.seq)
                || entry.remaining_ms > config.replay_ttl_ms
                || entry.body.len() > config.max_frame_size
            {
                return Err(ErrorCode::ResumeUnavailable.into());
            }
            previous = entry.seq;
            let remaining = Duration::from_millis(entry.remaining_ms).saturating_sub(elapsed);
            cache.cached_bytes = cache
                .cached_bytes
                .checked_add(entry.body.len())
                .ok_or(ErrorCode::CapacityExceeded)?;
            if cache.cached_bytes > config.replay_max_bytes {
                return Err(ErrorCode::CapacityExceeded.into());
            }
            cache.entries.push_back(Entry {
                seq: entry.seq,
                body: entry.body.clone().into(),
                published_at: Instant::now() - cache.ttl.saturating_sub(remaining),
            });
        }
        if previous != snapshot.published_seq {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        cache.evict_expired();
        Ok(cache)
    }
    pub(super) fn new(config: &Config) -> Self {
        Self {
            entries: VecDeque::new(),
            cached_bytes: 0,
            published_seq: 0,
            acknowledged_seq: 0,
            replay_floor_seq: 0,
            ttl: Duration::from_millis(config.replay_ttl_ms),
            max_entries: config.replay_capacity,
            max_bytes: config.replay_max_bytes,
        }
    }

    // 只读取候选序号；消息通过 push 发布后才推进计数。
    pub(super) fn next_seq(&self) -> Result<u64> {
        self.published_seq
            .checked_add(1)
            .ok_or(ErrorCode::SessionClosed.into())
    }

    pub(super) fn last_published_seq(&self) -> u64 {
        self.published_seq
    }

    fn evict_oldest(&mut self) {
        if let Some(entry) = self.entries.pop_front() {
            self.cached_bytes -= entry.body.len();
            self.replay_floor_seq = entry.seq;
        }
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        while self
            .entries
            .front()
            .is_some_and(|entry| now.duration_since(entry.published_at) >= self.ttl)
        {
            self.evict_oldest();
        }
    }

    pub(super) fn push(&mut self, seq: u64, body: Arc<[u8]>) {
        self.evict_expired();
        self.published_seq = seq;
        self.cached_bytes += body.len();
        self.entries.push_back(Entry {
            seq,
            published_at: Instant::now(),
            body,
        });
        while self.entries.len() > self.max_entries || self.cached_bytes > self.max_bytes {
            self.evict_oldest();
        }
    }

    pub(super) fn acknowledge(&mut self, seq: u64) -> Result<()> {
        self.evict_expired();
        if seq > self.published_seq {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if seq > self.acknowledged_seq {
            self.acknowledged_seq = seq;
            while self.entries.front().is_some_and(|entry| entry.seq <= seq) {
                self.evict_oldest();
            }
        }
        Ok(())
    }

    pub(super) fn replay_after(&mut self, seq: u64) -> Option<ReplayBatch> {
        self.evict_expired();
        if seq < self.replay_floor_seq || seq > self.published_seq {
            return None;
        }
        Some(ReplayBatch {
            through_seq: self.published_seq,
            frames: self
                .entries
                .iter()
                .filter(|entry| entry.seq > seq)
                .map(|entry| Arc::clone(&entry.body))
                .collect(),
        })
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.cached_bytes = 0;
        self.replay_floor_seq = self.published_seq;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push(cache: &mut ReplayCache, bytes: usize) {
        let seq = cache.next_seq().unwrap();
        cache.push(seq, vec![seq as u8; bytes].into());
    }

    #[tokio::test(start_paused = true)]
    async fn handoff_budget_preserves_expiration_and_rejects_noncontiguous_snapshots() {
        let config = Config {
            replay_ttl_ms: 100,
            ..Default::default()
        };
        let mut source = ReplayCache::new(&config);
        push(&mut source, 4);
        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        let snapshot = source.snapshot();
        assert_eq!(snapshot.entries[0].remaining_ms, 40);
        let mut target =
            ReplayCache::restore(&config, &snapshot, std::time::Duration::from_millis(30)).unwrap();
        assert_eq!(target.replay_after(0).unwrap().frames.len(), 1);
        tokio::time::advance(std::time::Duration::from_millis(11)).await;
        assert!(target.replay_after(0).is_none());
        assert_eq!(target.last_published_seq(), 1);
        let mut broken = snapshot;
        broken.entries[0].seq = 2;
        assert!(ReplayCache::restore(&config, &broken, std::time::Duration::ZERO).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn count_eviction_ack_progress_and_missing_ranges() {
        let mut cache = ReplayCache::new(&Config {
            replay_capacity: 2,
            ..Default::default()
        });
        for _ in 0..3 {
            push(&mut cache, 4);
        }
        assert!(cache.replay_after(0).is_none());
        let batch = cache.replay_after(1).unwrap();
        assert_eq!(batch.through_seq, 3);
        assert_eq!(
            batch.frames.iter().map(|body| body[0]).collect::<Vec<_>>(),
            [2, 3]
        );
        assert!(cache.replay_after(4).is_none());
        cache.acknowledge(0).unwrap();
        cache.acknowledge(2).unwrap();
        cache.acknowledge(1).unwrap();
        cache.acknowledge(2).unwrap();
        assert!(cache.replay_after(1).is_none());
        assert_eq!(cache.replay_after(2).unwrap().frames.len(), 1);
        assert_eq!(cache.cached_bytes, 4);
        assert_eq!(
            cache.acknowledge(4).unwrap_err().code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(cache.acknowledged_seq, 2);
        cache.acknowledge(3).unwrap();
        assert!(cache.replay_after(3).unwrap().frames.is_empty());
        assert_eq!(cache.cached_bytes, 0);
        assert_eq!(cache.next_seq().unwrap(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn byte_limit_counts_actual_body_and_keeps_exact_boundary() {
        let mut cache = ReplayCache::new(&Config {
            replay_max_bytes: 8,
            ..Default::default()
        });
        push(&mut cache, 4);
        push(&mut cache, 4);
        assert_eq!(cache.replay_after(0).unwrap().frames.len(), 2);
        push(&mut cache, 5);
        assert!(cache.replay_after(1).is_none());
        assert_eq!(cache.replay_after(2).unwrap().frames[0].len(), 5);
        assert_eq!(cache.cached_bytes, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_uses_original_publication_and_queries_do_not_extend_it() {
        let mut cache = ReplayCache::new(&Config {
            replay_ttl_ms: 100,
            ..Default::default()
        });
        push(&mut cache, 3);
        tokio::time::advance(Duration::from_millis(50)).await;
        push(&mut cache, 3);
        let snapshot = cache.replay_after(0).unwrap();
        assert_eq!(snapshot.frames.len(), 2);
        tokio::time::advance(Duration::from_millis(49)).await;
        assert_eq!(cache.replay_after(0).unwrap().frames.len(), 2);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(cache.replay_after(0).is_none());
        assert_eq!(cache.replay_after(1).unwrap().frames.len(), 1);
        // 原快照可读取，但不会延长缓存保留时间。
        assert_eq!(snapshot.frames.len(), 2);
        tokio::time::advance(Duration::from_millis(50)).await;
        cache.acknowledge(0).unwrap();
        assert!(cache.replay_after(1).is_none());
        assert!(cache.replay_after(2).unwrap().frames.is_empty());
        assert_eq!(cache.cached_bytes, 0);
    }

    #[test]
    fn sequence_exhaustion_does_not_wrap_and_clear_preserves_high_watermark() {
        let mut cache = ReplayCache::new(&Config::default());
        cache.published_seq = u64::MAX;
        assert_eq!(cache.next_seq().unwrap_err().code, ErrorCode::SessionClosed);
        cache.clear();
        assert_eq!(cache.published_seq, u64::MAX);
        assert!(cache.replay_after(u64::MAX).unwrap().frames.is_empty());
    }

    #[test]
    fn replay_configuration_requires_bounded_values_and_room_for_one_frame() {
        for config in [
            Config {
                disconnect_grace_ms: 86_400_001,
                ..Default::default()
            },
            Config {
                resume_token_ttl_ms: 0,
                ..Default::default()
            },
            Config {
                resume_token_ttl_ms: 86_400_001,
                ..Default::default()
            },
            Config {
                replay_capacity: 0,
                ..Default::default()
            },
            Config {
                replay_capacity: 65_537,
                ..Default::default()
            },
            Config {
                replay_ttl_ms: 0,
                ..Default::default()
            },
            Config {
                replay_ttl_ms: 86_400_001,
                ..Default::default()
            },
            Config {
                replay_max_bytes: 64 * 1024 * 1024 + 1,
                ..Default::default()
            },
            Config {
                replay_max_bytes: 1_048_575,
                ..Default::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
        Config::default().validate().unwrap();
    }
}
