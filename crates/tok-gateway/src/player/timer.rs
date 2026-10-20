//! session 内的有界计时集合、调度版本和周期合并；不为每个 timer 创建任务。

use super::session::Job;
use crate::{ErrorCode, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok_core::{TimerSpec, TimerTrigger, TimerUpdate};
use tokio::{
    sync::{Notify, mpsc},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct TimerTicket {
    timer_id: String,
    version: u64,
    scheduled_at_ms: i64,
}
#[derive(Clone)]
struct Entry {
    spec: TimerSpec,
    version: u64,
    // 本次触发已排队或正在执行，期间不再产生同一 timer 的触发。
    tick_pending: bool,
    scheduled_at_ms: i64,
    deadline: Instant,
}
#[derive(Default)]
struct State {
    version: u64,
    entries: BTreeMap<String, Entry>,
}

// 会话建立时把 UTC 与单调时钟对齐；等待和实际时刻均沿此基准推进，避免墙钟跳变重放。
struct Clock {
    base_utc_ms: i64,
    base_instant: Instant,
}
impl Clock {
    fn new() -> Self {
        Self {
            base_utc_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64,
            base_instant: Instant::now(),
        }
    }
    fn now_ms(&self) -> i64 {
        self.base_utc_ms.saturating_add(
            self.base_instant
                .elapsed()
                .as_millis()
                .min(i64::MAX as u128) as i64,
        )
    }
    fn deadline(&self, target_utc_ms: i64) -> Option<Instant> {
        self.base_instant.checked_add(Duration::from_millis(
            target_utc_ms.saturating_sub(self.base_utc_ms).max(0) as u64,
        ))
    }
}

pub(super) struct TimerScheduler {
    state: Mutex<State>,
    changed: Notify,
    clock: Clock,
    max_timers: usize,
    max_timer_bytes: usize,
}
impl TimerScheduler {
    pub(super) fn new(max_timers: usize, max_timer_bytes: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            clock: Clock::new(),
            max_timers,
            max_timer_bytes,
        }
    }
    pub(super) fn apply(&self, update: TimerUpdate) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        let version = state.version.checked_add(1).ok_or(ErrorCode::Internal)?;
        let (mut entries, upsert, cancel) = match update {
            TimerUpdate::ReplaceAll(specs) => (BTreeMap::new(), specs, Vec::new()),
            TimerUpdate::Patch { upsert, cancel } => (state.entries.clone(), upsert, cancel),
        };
        if upsert.len() > self.max_timers || cancel.len() > self.max_timers {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        let mut ids = BTreeSet::new();
        for spec in upsert {
            if !valid_id(&spec.timer_id)
                || !ids.insert(spec.timer_id.clone())
                || spec.message_id == 0
                || spec.due_at_ms < 0
                || spec
                    .interval_ms
                    .is_some_and(|ms| ms == 0 || ms > i64::MAX as u64)
            {
                return Err(ErrorCode::InvalidRequest.into());
            }
            let deadline = self
                .clock
                .deadline(spec.due_at_ms)
                .ok_or(ErrorCode::InvalidRequest)?;
            entries.insert(
                spec.timer_id.clone(),
                Entry {
                    scheduled_at_ms: spec.due_at_ms,
                    spec,
                    version,
                    tick_pending: false,
                    deadline,
                },
            );
        }
        for id in cancel {
            if !valid_id(&id) || !ids.insert(id.clone()) {
                return Err(ErrorCode::InvalidRequest.into());
            }
            entries.remove(&id);
        }
        let bytes = entries.values().try_fold(0usize, |sum, entry| {
            sum.checked_add(entry.spec.timer_id.len())?
                .checked_add(entry.spec.body.len())
        });
        if entries.len() > self.max_timers || bytes.is_none_or(|bytes| bytes > self.max_timer_bytes)
        {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        state.entries = entries;
        state.version = version;
        drop(state);
        self.changed.notify_one();
        Ok(())
    }
    pub(super) fn is_current(&self, ticket: &TimerTicket) -> bool {
        self.lock_state()
            .entries
            .get(&ticket.timer_id)
            .is_some_and(|entry| {
                entry.version == ticket.version
                    && entry.tick_pending
                    && entry.scheduled_at_ms == ticket.scheduled_at_ms
            })
    }
    pub(super) fn prepare(&self, ticket: &TimerTicket) -> Option<(TimerSpec, TimerTrigger)> {
        let state = self.lock_state();
        let entry = state.entries.get(&ticket.timer_id)?;
        if entry.version != ticket.version
            || !entry.tick_pending
            || entry.scheduled_at_ms != ticket.scheduled_at_ms
        {
            return None;
        }
        Some((
            entry.spec.clone(),
            TimerTrigger {
                timer_id: ticket.timer_id.clone(),
                scheduled_at_ms: ticket.scheduled_at_ms,
                fired_at_ms: self.clock.now_ms().max(ticket.scheduled_at_ms),
            },
        ))
    }
    pub(super) fn complete_tick(&self, ticket: &TimerTicket) {
        let mut state = self.lock_state();
        let Some(entry) = state.entries.get_mut(&ticket.timer_id) else {
            return;
        };
        if entry.version != ticket.version {
            return;
        }
        let next = entry.spec.interval_ms.and_then(|period| {
            let now = self.clock.now_ms();
            let steps = (i128::from(now) - i128::from(entry.scheduled_at_ms)).max(0)
                / i128::from(period)
                + 1;
            let next =
                i64::try_from(i128::from(entry.scheduled_at_ms) + steps * i128::from(period))
                    .ok()?;
            Some((next, self.clock.deadline(next)?))
        });
        if let Some((next, deadline)) = next {
            entry.scheduled_at_ms = next;
            entry.deadline = deadline;
            entry.tick_pending = false;
        } else {
            state.entries.remove(&ticket.timer_id);
        }
        drop(state);
        self.changed.notify_one();
    }
    pub(super) async fn run(&self, queue: mpsc::Sender<Job>, cancel: CancellationToken) {
        loop {
            let changed = self.changed.notified();
            let deadline = self
                .lock_state()
                .entries
                .values()
                .filter(|entry| !entry.tick_pending)
                .map(|entry| entry.deadline)
                .min();
            let waiting = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! { biased; _ = cancel.cancelled() => return, _ = changed => continue, _ = waiting => {} }
            // 满队列只等待一个可取消的容量许可，不丢触发，不自旋，也不创建无限后台任务。
            let permit = tokio::select! { biased; _ = cancel.cancelled() => return, _ = self.changed.notified() => continue, permit = queue.reserve() => match permit { Ok(permit) => permit, Err(_) => return } };
            let mut state = self.lock_state();
            let next = state
                .entries
                .values_mut()
                .filter(|entry| !entry.tick_pending && entry.deadline <= Instant::now())
                .min_by_key(|entry| entry.deadline);
            if let Some(entry) = next {
                entry.tick_pending = true;
                permit.send(Job::Timer(TimerTicket {
                    timer_id: entry.spec.timer_id.clone(),
                    version: entry.version,
                    scheduled_at_ms: entry.scheduled_at_ms,
                }));
            }
        }
    }

    // 锁中毒说明持锁路径已 panic，timer 的版本及 tick_pending 状态可能只更新了一部分。
    // 不能把它当作“无有效 timer”继续运行；主动 panic 交给 Gateway 的 JoinSet 监督，
    // 由既有后台任务崩溃流程失效入口、停止服务并收束所有权，而不恢复未知状态。
    #[expect(
        clippy::expect_used,
        reason = "timer 锁中毒后状态完整性未知，必须让受监督任务失败并触发网关清理"
    )]
    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("timer 锁已中毒，停止受监督的调度任务")
    }
}
fn valid_id(id: &str) -> bool {
    !id.trim().is_empty() && id.len() <= 256
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn poisoned_state_stops_the_scheduler_instead_of_treating_it_as_empty() {
        let timers = Arc::new(TimerScheduler::new(4, 1024));
        let poisoned = timers.clone();
        let poisoner = tokio::spawn(async move {
            let _guard = poisoned.state.lock().unwrap();
            panic!("模拟持锁路径崩溃");
        });
        assert!(poisoner.await.unwrap_err().is_panic());
        let (sender, _receiver) = mpsc::channel(1);
        let worker = tokio::spawn(async move {
            timers.run(sender, CancellationToken::new()).await;
        });
        assert!(worker.await.unwrap_err().is_panic());
    }

    fn spec(id: &str, due: i64, period: Option<u64>) -> TimerSpec {
        TimerSpec {
            timer_id: id.into(),
            message_id: 1,
            body: vec![1],
            due_at_ms: due,
            interval_ms: period,
        }
    }
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    async fn ticket(queue: &mut mpsc::Receiver<Job>) -> TimerTicket {
        match queue.recv().await.unwrap() {
            Job::Timer(ticket) => ticket,
            _ => panic!("预期计时任务"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn replacing_and_cancelling_invalidates_queued_versions() {
        let timers = Arc::new(TimerScheduler::new(4, 1024));
        let (sender, mut queue) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let worker = {
            let timers = timers.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { timers.run(sender, cancel).await })
        };
        timers
            .apply(TimerUpdate::ReplaceAll(vec![spec("a", 0, None)]))
            .unwrap();
        let old = ticket(&mut queue).await;
        timers
            .apply(TimerUpdate::Patch {
                upsert: vec![spec("a", 0, Some(1000))],
                cancel: vec![],
            })
            .unwrap();
        let current = ticket(&mut queue).await;
        assert!(timers.prepare(&old).is_none());
        timers.complete_tick(&old);
        assert!(timers.prepare(&current).is_some());
        timers
            .apply(TimerUpdate::Patch {
                upsert: vec![],
                cancel: vec!["a".into(), "missing".into()],
            })
            .unwrap();
        assert!(timers.prepare(&current).is_none());
        timers
            .apply(TimerUpdate::ReplaceAll(vec![spec("b", 0, None)]))
            .unwrap();
        let replaced = ticket(&mut queue).await;
        timers.apply(TimerUpdate::ReplaceAll(vec![])).unwrap();
        assert!(timers.prepare(&replaced).is_none());
        cancel.cancel();
        worker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn busy_periods_merge_and_keep_original_time_base() {
        let timers = Arc::new(TimerScheduler::new(4, 1024));
        let epoch = timers.clock.now_ms();
        let (sender, mut queue) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let worker = {
            let timers = timers.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { timers.run(sender, cancel).await })
        };
        timers
            .apply(TimerUpdate::ReplaceAll(vec![spec(
                "periodic",
                epoch + 1000,
                Some(1000),
            )]))
            .unwrap();
        tokio::time::advance(Duration::from_millis(1000)).await;
        let first = ticket(&mut queue).await;
        tokio::time::advance(Duration::from_millis(3500)).await;
        settle().await;
        assert!(queue.try_recv().is_err());
        let (_, trigger) = timers.prepare(&first).unwrap();
        assert_eq!(trigger.scheduled_at_ms, epoch + 1000);
        assert_eq!(trigger.fired_at_ms, epoch + 4500);
        timers.complete_tick(&first);
        settle().await;
        tokio::time::advance(Duration::from_millis(499)).await;
        settle().await;
        assert!(queue.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        let next = ticket(&mut queue).await;
        assert_eq!(next.scheduled_at_ms, epoch + 5000);
        cancel.cancel();
        worker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn queue_pressure_does_not_lose_or_duplicate_due_timers() {
        let timers = Arc::new(TimerScheduler::new(4, 1024));
        let (sender, mut queue) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        timers
            .apply(TimerUpdate::ReplaceAll(vec![
                spec("first", 0, None),
                spec("second", 0, None),
            ]))
            .unwrap();
        let worker = {
            let timers = timers.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { timers.run(sender, cancel).await })
        };
        settle().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let first = ticket(&mut queue).await;
        let second = ticket(&mut queue).await;
        assert_ne!(first.timer_id, second.timer_id);
        timers.complete_tick(&first);
        timers.complete_tick(&second);
        settle().await;
        assert!(queue.try_recv().is_err());
        assert!(timers.state.lock().unwrap().entries.is_empty());
        cancel.cancel();
        worker.await.unwrap();
    }

    #[test]
    fn invalid_or_oversized_updates_leave_the_entire_set_unchanged() {
        let timers = TimerScheduler::new(2, 12);
        timers
            .apply(TimerUpdate::ReplaceAll(vec![spec("old", 0, None)]))
            .unwrap();
        let version = timers.state.lock().unwrap().version;
        for update in [
            TimerUpdate::ReplaceAll(vec![spec("zero", 0, Some(0))]),
            TimerUpdate::ReplaceAll(vec![spec("dup", 0, None), spec("dup", 0, None)]),
            TimerUpdate::Patch {
                upsert: vec![spec("both", 0, None)],
                cancel: vec!["both".into()],
            },
            TimerUpdate::ReplaceAll(vec![
                spec("a", 0, None),
                spec("b", 0, None),
                spec("c", 0, None),
            ]),
            TimerUpdate::ReplaceAll(vec![spec("longer-than-limit", 0, None)]),
            TimerUpdate::ReplaceAll(vec![spec("past", -1, None)]),
        ] {
            assert!(timers.apply(update).is_err());
            let state = timers.state.lock().unwrap();
            assert_eq!(state.version, version);
            assert_eq!(
                state.entries.keys().map(String::as_str).collect::<Vec<_>>(),
                ["old"]
            );
        }
    }
}
