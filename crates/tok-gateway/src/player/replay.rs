//! 每个逻辑会话的有界下行字节缓存与连续范围查询；尚不提供断线恢复。

use super::PlayerConfig;
use crate::{ErrorCode, Result};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::time::Instant;

/// 某次查询时连续完整的下行快照，仅含原始 protobuf Frame 正文。
///
/// 查询不会确认消息或触发发送；连接重绑定与恢复流程尚未实现。
#[derive(Debug)]
pub struct ReplayBatch {
    /// 本快照包含的最大发布序号；frames 为空时也保留查询时的位置。
    pub through_seq: u64,
    /// 确认位置之后按序排列的编码正文，不含长度前缀或连接加密封装。
    pub frames: Vec<Arc<[u8]>>,
}

struct Entry {
    seq: u64,
    published: Instant,
    body: Arc<[u8]>,
}

pub(super) struct ReplayCache {
    entries: VecDeque<Entry>,
    bytes: usize,
    high: u64,
    ack: u64,
    floor: u64,
    ttl: Duration,
    capacity: usize,
    max_bytes: usize,
}

impl ReplayCache {
    pub(super) fn new(config: &PlayerConfig) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            high: 0,
            ack: 0,
            floor: 0,
            ttl: Duration::from_millis(config.replay_ttl_ms),
            capacity: config.replay_capacity,
            max_bytes: config.replay_max_bytes,
        }
    }

    pub(super) fn next(&self) -> Result<u64> {
        self.high
            .checked_add(1)
            .ok_or(ErrorCode::SessionClosed.into())
    }

    fn pop(&mut self) {
        if let Some(entry) = self.entries.pop_front() {
            self.bytes -= entry.body.len();
            self.floor = entry.seq;
        }
    }

    fn expire(&mut self) {
        let now = Instant::now();
        while self
            .entries
            .front()
            .is_some_and(|entry| now.duration_since(entry.published) >= self.ttl)
        {
            self.pop();
        }
    }

    pub(super) fn push(&mut self, seq: u64, body: Arc<[u8]>) {
        self.expire();
        self.high = seq;
        self.bytes += body.len();
        self.entries.push_back(Entry {
            seq,
            published: Instant::now(),
            body,
        });
        while self.entries.len() > self.capacity || self.bytes > self.max_bytes {
            self.pop();
        }
    }

    pub(super) fn acknowledge(&mut self, seq: u64) -> Result<()> {
        self.expire();
        if seq > self.high {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if seq > self.ack {
            self.ack = seq;
            while self.entries.front().is_some_and(|entry| entry.seq <= seq) {
                self.pop();
            }
        }
        Ok(())
    }

    pub(super) fn after(&mut self, seq: u64) -> Option<ReplayBatch> {
        self.expire();
        if seq < self.floor || seq > self.high {
            return None;
        }
        Some(ReplayBatch {
            through_seq: self.high,
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
        self.bytes = 0;
        self.floor = self.high;
    }
}

#[cfg(test)]
mod tests;
