use super::{
    channel::{self, Binding, Outbound},
    handle::{PendingResponse, SessionHandle},
    state::{Completion, SessionState},
    timer, worker,
};
use crate::{
    ErrorCode,
    player::{Config, Executor, Reply, Task},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok_core::Identity as SessionOwner;
use tok_infra::ownership::MemoryOwnership;
use tok_player::TimerTrigger;
use tok_player::{
    LogicalMessage, MessageKind, TimerSpec, TimerUpdate, ownership::Store as OwnershipStore,
};
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
enum Event {
    Request(u32),
    Timer(TimerTrigger),
}
struct TestExecutor {
    updates: Mutex<BTreeMap<u32, TimerUpdate>>,
    timer_update: Mutex<Option<TimerUpdate>>,
    events: mpsc::UnboundedSender<Event>,
    permits: Semaphore,
    failures: AtomicUsize,
    block_timer: AtomicBool,
}
impl Executor for TestExecutor {
    fn execute_request(
        &self,
        input: tok_execution::Request<crate::player::RequestInput>,
    ) -> Task<'_> {
        let crate::player::RequestInput {
            identity: _identity,
            message: request,
        } = input.input;
        Box::pin(async move {
            self.events
                .send(Event::Request(request.message_id))
                .unwrap();
            if request.message_id == 2 {
                self.permits.acquire().await.unwrap().forget();
            }
            Ok(tok_execution::Response::new(Reply {
                response: Some(LogicalMessage {
                    kind: MessageKind::Response,
                    message_id: request.message_id + 100,
                    request_id: request.request_id,
                    server_seq: None,
                    error_code: 0,
                    body: vec![],
                }),
                timers: self.updates.lock().unwrap().remove(&request.message_id),
            }))
        })
    }
    fn execute_timer(&self, input: tok_execution::Request<crate::player::TimerInput>) -> Task<'_> {
        let crate::player::TimerInput {
            identity,
            request_id: _request_id,
            spec: _spec,
            trigger,
        } = input.input;
        Box::pin(async move {
            assert_eq!(identity.player_id, "42");
            self.events.send(Event::Timer(trigger)).unwrap();
            if self.block_timer.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self
                .failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(ErrorCode::Unavailable.into());
            }
            Ok(tok_execution::Response::new(Reply {
                response: None,
                timers: self.timer_update.lock().unwrap().take(),
            }))
        })
    }
}
struct Fixture {
    handle: SessionHandle,
    executor: Arc<TestExecutor>,
    events: mpsc::UnboundedReceiver<Event>,
    channel: mpsc::Receiver<Outbound>,
    ownership: Arc<MemoryOwnership>,
    task: Option<tokio::task::JoinHandle<()>>,
    due: i64,
    connection: Binding,
}
impl Fixture {
    async fn new() -> Self {
        let ownership = Arc::new(MemoryOwnership::new(8).unwrap());
        let lease = ownership
            .acquire(
                "42".into(),
                "session".into(),
                SessionOwner {
                    node_id: "node".into(),
                    service_name: "gateway".into(),
                },
                None,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        let (queue, receiver) = mpsc::channel(2);
        let (channel, mut outgoing) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let active = Arc::new(SessionState {
            extension: Arc::new(super::extension::Empty),
            closing_notified: std::sync::atomic::AtomicBool::new(false),
            manager: std::sync::Weak::new(),
            extensions: Default::default(),
            pipeline: crate::player::pipeline::test_pipeline(),
            execution_budget: std::time::Duration::from_millis(Config::default().rpc_timeout_ms),
            downlink_gate: tokio::sync::Mutex::new(()),
            topic_gate: tokio::sync::Mutex::new(()),
            timers: Arc::new(timer::Scheduler::new(128, 1048576)),
            initial_execution: None,
            ownership: lease,
            cancel: cancel.clone(),
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            job_sender: queue,
            channel: Arc::new(channel::Channel::new(channel, &Config::default(), cancel)),
            resume_token: String::new(),
        });
        let connection = active
            .channel
            .complete_login(crate::protocol::Frame {
                version: crate::codec::VERSION,
                content: Some(crate::protocol::frame::Content::LoggedIn(
                    crate::protocol::LoginResponse::default(),
                )),
            })
            .unwrap();
        outgoing.try_recv().unwrap();
        let due = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 1000;
        let (events, receiver_events) = mpsc::unbounded_channel();
        let initial = TimerUpdate::ReplaceAll(vec![TimerSpec {
            timer_id: "income".into(),
            message_id: 20,
            body: vec![],
            due_at_ms: due,
            interval_ms: Some(1000),
        }]);
        let executor = Arc::new(TestExecutor {
            updates: Mutex::new(BTreeMap::from([(1, initial)])),
            timer_update: Mutex::new(None),
            events,
            permits: Semaphore::new(0),
            failures: AtomicUsize::new(0),
            block_timer: AtomicBool::new(false),
        });
        let task = tokio::spawn(worker::task(
            Arc::clone(&active),
            receiver,
            Arc::new(Config::default()),
            Arc::<MemoryOwnership>::clone(&ownership),
            Arc::<TestExecutor>::clone(&executor),
        ));
        Self {
            handle: SessionHandle(active),
            executor,
            events: receiver_events,
            channel: outgoing,
            ownership,
            task: Some(task),
            due,
            connection,
        }
    }
    fn request(&self, id: u64, message: u32) -> PendingResponse {
        self.handle
            .try_request(LogicalMessage {
                kind: MessageKind::Request,
                request_id: id,
                message_id: message,
                server_seq: None,
                error_code: 0,
                body: vec![],
            })
            .unwrap()
    }
    async fn install(&mut self) {
        assert_eq!(self.request(1, 1).await.output.error_code, 0);
        assert!(matches!(self.events.recv().await, Some(Event::Request(1))));
    }
    async fn trigger(&mut self) -> TimerTrigger {
        match self.events.recv().await.unwrap() {
            Event::Timer(trigger) => trigger,
            other => panic!("预期计时触发，实际 {other:?}"),
        }
    }
    async fn close(&mut self) {
        self.handle.close();
        self.task.take().unwrap().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn disconnected_session_keeps_timers_and_ownership_during_grace() {
    let mut f = Fixture::new().await;
    f.install().await;
    f.handle.0.channel.detach(&f.connection);
    tokio::time::advance(Duration::from_secs(1)).await;
    let trigger = f.trigger().await;
    assert_eq!(trigger.scheduled_at_ms, f.due);
    assert!(!f.handle.cancellation().is_cancelled());
    assert!(f.ownership.lookup("42").await.unwrap().is_some());
    f.close().await;
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
async fn settle() {
    for _ in 0..12 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn timers_share_fifo_and_do_not_consume_client_request_ids() {
    let mut f = Fixture::new().await;
    f.install().await;
    let blocked = f.request(2, 2);
    assert!(matches!(f.events.recv().await, Some(Event::Request(2))));
    let queued = f.request(3, 3);
    tokio::time::advance(Duration::from_millis(3500)).await;
    settle().await;
    assert!(f.events.try_recv().is_err());
    f.executor.permits.add_permits(1);
    assert_eq!(blocked.await.output.error_code, 0);
    assert_eq!(queued.await.output.error_code, 0);
    assert!(matches!(f.events.recv().await, Some(Event::Request(3))));
    let trigger = f.trigger().await;
    assert_eq!(trigger.scheduled_at_ms, f.due);
    assert!(trigger.fired_at_ms >= f.due + 2000);
    settle().await;
    assert!(f.events.try_recv().is_err());
    assert!(f.channel.try_recv().is_err());
    assert_eq!(f.request(4, 4).await.output.error_code, 0);
    f.close().await;
}

#[tokio::test(start_paused = true)]
async fn response_update_invalidates_timer_already_queued_during_rpc() {
    let mut f = Fixture::new().await;
    f.install().await;
    f.executor.updates.lock().unwrap().insert(
        2,
        TimerUpdate::Patch {
            upsert: vec![],
            cancel: vec!["income".into()],
        },
    );
    let blocked = f.request(2, 2);
    assert!(matches!(f.events.recv().await, Some(Event::Request(2))));
    tokio::time::advance(Duration::from_millis(1500)).await;
    settle().await;
    f.executor.permits.add_permits(1);
    assert_eq!(blocked.await.output.error_code, 0);
    tokio::time::advance(Duration::from_secs(2)).await;
    settle().await;
    assert!(f.events.try_recv().is_err());
    assert!(!f.handle.cancellation().is_cancelled());
    f.close().await;
}

#[tokio::test(start_paused = true)]
async fn failed_tick_is_not_retried_and_next_tick_can_cancel_itself() {
    let mut f = Fixture::new().await;
    f.install().await;
    f.executor.failures.store(1, Ordering::SeqCst);
    *f.executor.timer_update.lock().unwrap() = Some(TimerUpdate::Patch {
        upsert: vec![],
        cancel: vec!["income".into()],
    });
    tokio::time::advance(Duration::from_millis(1000)).await;
    assert_eq!(f.trigger().await.scheduled_at_ms, f.due);
    settle().await;
    assert!(f.events.try_recv().is_err());
    tokio::time::advance(Duration::from_millis(1000)).await;
    assert_eq!(f.trigger().await.scheduled_at_ms, f.due + 1000);
    tokio::time::advance(Duration::from_secs(3)).await;
    settle().await;
    assert!(f.events.try_recv().is_err());
    assert!(f.channel.try_recv().is_err());
    f.close().await;
}

#[tokio::test(start_paused = true)]
async fn timed_out_tick_skips_missed_periods_and_uses_next_future_tick() {
    let mut f = Fixture::new().await;
    f.install().await;
    f.executor.block_timer.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(1000)).await;
    f.trigger().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(f.events.try_recv().is_err());
    f.executor.block_timer.store(false, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(1)).await;
    let trigger = f.trigger().await;
    assert_eq!(trigger.scheduled_at_ms, f.due + 6000);
    f.close().await;
}

#[tokio::test(start_paused = true)]
async fn invalid_timer_metadata_closes_session_before_new_work() {
    let mut f = Fixture::new().await;
    f.install().await;
    f.executor.updates.lock().unwrap().insert(
        3,
        TimerUpdate::ReplaceAll(vec![TimerSpec {
            timer_id: "invalid".into(),
            message_id: 0,
            body: vec![],
            due_at_ms: 0,
            interval_ms: None,
        }]),
    );
    assert_eq!(
        f.request(2, 3).await.output.error_code,
        ErrorCode::Internal as u32
    );
    assert!(f.handle.cancellation().is_cancelled());
    assert!(
        f.handle
            .try_request(LogicalMessage {
                kind: MessageKind::Request,
                message_id: 4,
                request_id: 3,
                server_seq: None,
                error_code: 0,
                body: vec![]
            })
            .is_err()
    );
    f.close().await;
}

#[tokio::test(start_paused = true)]
async fn lease_loss_prevents_timer_execution_and_preserves_new_owner() {
    let mut f = Fixture::new().await;
    f.install().await;
    let owner = f
        .ownership
        .acquire(
            "42".into(),
            "new".into(),
            SessionOwner {
                node_id: "other".into(),
                service_name: "gateway".into(),
            },
            Some(f.handle.identity().epoch),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert!(f.events.try_recv().is_err());
    assert!(f.handle.cancellation().is_cancelled());
    f.close().await;
    assert_eq!(f.ownership.lookup("42").await.unwrap(), Some(owner));
}
