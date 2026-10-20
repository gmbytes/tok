//! 每个逻辑会话的有界下行字节缓存与连续范围查询，由连接恢复流程按原字节补发。

use super::PlayerConfig;
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
    pub(super) fn new(config: &PlayerConfig) -> Self {
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
                .map(|entry| entry.body.clone())
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
mod tests;
