//! 逻辑会话的共享数据和执行完成信号。
use super::{channel, timer, worker::Job};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tok_player::ownership::Record as SessionOwnership;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

pub(super) struct Completion {
    pub(super) done: AtomicBool,
    pub(super) notify: Notify,
}
impl Completion {
    pub(super) async fn wait(&self) {
        loop {
            let waiting = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            waiting.await;
        }
    }
}
pub(super) struct SessionState {
    pub(super) extension: Arc<dyn super::extension::Factory>,
    pub(super) closing_notified: AtomicBool,
    pub(super) manager: std::sync::Weak<super::manager::ManagerState>,
    pub(super) extensions: tok_core::Extensions,
    pub(super) pipeline: Arc<crate::player::Pipeline>,
    pub(super) execution_budget: std::time::Duration,
    pub(super) topic_gate: tokio::sync::Mutex<()>,
    pub(super) downlink_gate: tokio::sync::Mutex<()>,
    pub(super) timers: Arc<timer::Scheduler>,
    pub(super) initial_execution:
        Option<tok_message::rpc::protocol::internal::v1::ExecutionSnapshot>,
    pub(super) ownership: SessionOwnership,
    pub(super) cancel: CancellationToken,
    pub(super) completion: Completion,
    pub(super) job_sender: mpsc::Sender<Job>,
    pub(super) channel: Arc<channel::Channel>,
    pub(super) resume_token: String,
}

#[derive(Default)]
pub(super) struct Activities {
    count: std::sync::atomic::AtomicUsize,
    changed: Notify,
}
pub(super) struct ActivityGuard(Arc<Activities>);
impl Activities {
    pub(super) fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
    pub(super) fn guard(self: &Arc<Self>) -> ActivityGuard {
        self.count.fetch_add(1, Ordering::SeqCst);
        ActivityGuard(Arc::clone(self))
    }
    pub(super) async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            if self.count() == 0 {
                return;
            }
            changed.await;
        }
    }
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}
