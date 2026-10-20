//! 逻辑会话的共享数据和执行完成信号。
use super::{channel, timer, worker::Job};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tok_core::coordination::SessionOwnership;
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
    pub(super) timers: Arc<timer::Scheduler>,
    pub(super) initial_execution: Option<tok_rpc::protocol::internal::v1::ExecutionSnapshot>,
    pub(super) ownership: SessionOwnership,
    pub(super) cancel: CancellationToken,
    pub(super) completion: Completion,
    pub(super) job_sender: mpsc::Sender<Job>,
    pub(super) channel: Arc<channel::Channel>,
    pub(super) resume_token: String,
}
