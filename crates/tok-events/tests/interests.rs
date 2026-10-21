//! 兴趣共享、取消、预算与监督边界；使用可控传输，不依赖外部服务。
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_events::{
    Error, Handler, Message, Outcome, Subscriber, Task, Topic, TopicFilter,
    interest::{self, Pool, Transport},
};
use tok_lifecycle::task::Supervisor;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
struct Ignore;
impl Handler for Ignore {
    fn event_types(&self) -> Vec<&str> {
        vec!["test.changed"]
    }
    fn handle(&self, _: tok_execution::Request<Arc<Message>>) -> Task<'_> {
        Box::pin(async { Ok(tok_execution::Response::new(Outcome::Ignored)) })
    }
}
fn component() -> Subscriber {
    Subscriber::new(
        tok_core::service::Identity {
            node_id: "test".into(),
            service_name: "events".into(),
        },
        Arc::new(Ignore),
        TopicFilter::All,
        Some("test.workers"),
    )
    .unwrap()
}
struct Fake {
    starts: AtomicUsize,
    active: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    opened: Notify,
    permits: Arc<Semaphore>,
    failure: AtomicBool,
    block_cleanup: AtomicBool,
    cleanup: Arc<Semaphore>,
    crash: CancellationToken,
}
impl Fake {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(0)),
            starts: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            stops: Arc::new(AtomicUsize::new(0)),
            opened: Notify::new(),
            failure: AtomicBool::new(false),
            block_cleanup: AtomicBool::new(false),
            cleanup: Arc::new(Semaphore::new(0)),
            crash: CancellationToken::new(),
        }
    }
    async fn started(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.opened.notified();
                if self.starts.load(Ordering::SeqCst) >= n {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
    }
}
struct Active {
    active: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.stops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Transport for Fake {
    fn subscribe(
        &self,
        subscriber: Subscriber,
        stop: CancellationToken,
    ) -> Task<'_, Task<'static, ()>> {
        Box::pin(async move {
            assert_eq!(subscriber.group(), Some("test.workers"));
            assert!(matches!(subscriber.topics(), TopicFilter::Exact(_)));
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.opened.notify_waiters();
            let permit = Arc::clone(&self.permits).acquire_owned().await.unwrap();
            permit.forget();
            if self.failure.swap(false, Ordering::SeqCst) {
                return Err(Error::Unavailable("injected"));
            }
            self.active.fetch_add(1, Ordering::SeqCst);
            let active = Active {
                active: Arc::clone(&self.active),
                stops: Arc::clone(&self.stops),
            };
            let block_cleanup = self.block_cleanup.load(Ordering::SeqCst);
            let cleanup = Arc::clone(&self.cleanup);
            let crash = self.crash.clone();
            Ok(Box::pin(async move {
                let _active = active;
                tokio::select! { biased; () = stop.cancelled() => {}, () = crash.cancelled() => return Err(Error::Unavailable("injected consumer failure")) }
                if block_cleanup {
                    cleanup.acquire().await.unwrap().forget();
                }
                Ok(())
            }) as Task<'static, ()>)
        })
    }
}
struct Fixture {
    pool: Pool,
    fake: Arc<Fake>,
    stop: CancellationToken,
    driver: tokio::task::JoinHandle<()>,
}
impl Fixture {
    fn new(config: interest::Config) -> Self {
        let stop = CancellationToken::new();
        let mut supervisor = Supervisor::new(16, stop.clone()).unwrap();
        let fake = Arc::new(Fake::new());
        let pool = Pool::new(
            config,
            Arc::<Fake>::clone(&fake),
            supervisor.handle(),
            [("events".into(), component())],
        )
        .unwrap();
        let cancel = stop.clone();
        let driver = tokio::spawn(async move {
            loop {
                tokio::select! { biased; () = cancel.cancelled() => break,
                result = supervisor.next() => result.result.unwrap(), }
            }
            supervisor
                .shutdown(Some(Duration::from_secs(2)))
                .await
                .unwrap();
        });
        Self {
            pool,
            fake,
            stop,
            driver,
        }
    }
    async fn finish(self) {
        self.stop.cancel();
        self.driver.await.unwrap();
        assert_eq!(self.fake.active.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn shared_opening_survives_one_waiter_cancellation_and_fixed_reference_survives_members() {
    let f = Fixture::new(Default::default());
    let one = tokio::spawn({
        let pool = f.pool.clone();
        async move { pool.acquire("events", Topic::named("room").unwrap()).await }
    });
    f.fake.started(1).await;
    let two = tokio::spawn({
        let pool = f.pool.clone();
        async move { pool.acquire("events", Topic::named("room").unwrap()).await }
    });
    while f.pool.snapshot().unwrap().references < 2 {
        tokio::task::yield_now().await;
    }
    one.abort();
    assert!(matches!(one.await, Err(error) if error.is_cancelled()));
    f.fake.permits.add_permits(1);
    let fixed = two.await.unwrap().unwrap();
    let member = f
        .pool
        .acquire("events", Topic::named("room").unwrap())
        .await
        .unwrap();
    assert_eq!(f.fake.starts.load(Ordering::SeqCst), 1);
    assert_eq!(f.pool.snapshot().unwrap().references, 2);
    member.release().await.unwrap();
    assert_eq!(f.fake.active.load(Ordering::SeqCst), 1);
    fixed.release().await.unwrap();
    assert_eq!(f.pool.snapshot().unwrap().interests, 0);
    f.finish().await;
}
#[tokio::test]
async fn failed_setup_rolls_back_and_later_explicit_acquire_can_succeed() {
    let f = Fixture::new(Default::default());
    f.fake.failure.store(true, Ordering::SeqCst);
    f.fake.permits.add_permits(1);
    assert!(f.pool.acquire("events", Topic::default()).await.is_err());
    assert_eq!(f.pool.snapshot().unwrap().references, 0);
    assert_eq!(f.pool.snapshot().unwrap().interests, 0);
    f.fake.permits.add_permits(1);
    f.pool
        .acquire("events", Topic::default())
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
    assert_eq!(f.fake.starts.load(Ordering::SeqCst), 2);
    f.finish().await;
}
#[tokio::test]
async fn interest_and_pending_budgets_bound_in_progress_work() {
    let f = Fixture::new(interest::Config {
        max_interests: 1,
        max_pending: 1,
        ..Default::default()
    });
    let opening = tokio::spawn({
        let pool = f.pool.clone();
        async move { pool.acquire("events", Topic::default()).await }
    });
    f.fake.started(1).await;
    assert!(matches!(
        f.pool.acquire("events", Topic::default()).await,
        Err(Error::CapacityExceeded)
    ));
    f.fake.permits.add_permits(1);
    let lease = opening.await.unwrap().unwrap();
    assert!(matches!(
        f.pool
            .acquire("events", Topic::named("other").unwrap())
            .await,
        Err(Error::CapacityExceeded)
    ));
    assert!(f.pool.acquire("unknown", Topic::default()).await.is_err());
    lease.release().await.unwrap();
    f.finish().await;
}
#[tokio::test]
async fn readiness_timeout_releases_last_reference_and_shutdown_cleans_retained_handles() {
    let f = Fixture::new(interest::Config {
        timeout: Duration::from_millis(30),
        ..Default::default()
    });
    assert!(f.pool.acquire("events", Topic::default()).await.is_err());
    while f.pool.snapshot().unwrap().interests != 0 {
        tokio::task::yield_now().await;
    }
    f.fake.permits.add_permits(1);
    let lease = f.pool.acquire("events", Topic::default()).await.unwrap();
    let pool = f.pool.clone();
    f.finish().await;
    assert_eq!(pool.snapshot().unwrap().interests, 0);
    assert!(pool.acquire("events", Topic::default()).await.is_err());
    drop(lease);
}

#[tokio::test]
async fn reacquire_waits_until_prior_subscription_cleanup_finishes() {
    let f = Fixture::new(Default::default());
    f.fake.block_cleanup.store(true, Ordering::SeqCst);
    f.fake.permits.add_permits(1);
    let first = f.pool.acquire("events", Topic::default()).await.unwrap();
    drop(first);
    let next = tokio::spawn({
        let pool = f.pool.clone();
        async move { pool.acquire("events", Topic::default()).await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.pool.snapshot().unwrap().pending == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.fake.starts.load(Ordering::SeqCst), 1);
    f.fake.block_cleanup.store(false, Ordering::SeqCst);
    f.fake.cleanup.add_permits(1);
    f.fake.started(2).await;
    assert_eq!(f.fake.active.load(Ordering::SeqCst), 0);
    f.fake.permits.add_permits(1);
    next.await.unwrap().unwrap().release().await.unwrap();
    f.finish().await;
}
#[tokio::test]
async fn consumer_failure_is_reported_to_supervisor_without_hidden_restart() {
    let stop = CancellationToken::new();
    let mut supervisor = Supervisor::new(4, stop).unwrap();
    let fake = Arc::new(Fake::new());
    fake.permits.add_permits(1);
    let pool = Pool::new(
        Default::default(),
        Arc::<Fake>::clone(&fake),
        supervisor.handle(),
        [("events".into(), component())],
    )
    .unwrap();
    let acquire = pool.acquire("events", Topic::default());
    tokio::pin!(acquire);
    let lease = tokio::select! { result = &mut acquire => result.unwrap(), _ = supervisor.next() => panic!("订阅不应提前结束") };
    fake.crash.cancel();
    let completion = tokio::time::timeout(Duration::from_secs(1), supervisor.next())
        .await
        .unwrap();
    assert!(completion.result.is_err());
    assert_eq!(fake.starts.load(Ordering::SeqCst), 1);
    assert_eq!(pool.snapshot().unwrap().interests, 0);
    drop(lease);
    supervisor
        .shutdown(Some(Duration::from_secs(1)))
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_lease_does_not_keep_pool_or_its_subscriber_state_alive() {
    let Fixture {
        pool,
        fake,
        stop,
        driver,
    } = Fixture::new(Default::default());
    fake.permits.add_permits(1);
    let lease = pool.acquire("events", Topic::default()).await.unwrap();
    let weak = pool.downgrade();
    drop(pool);
    assert!(weak.upgrade().is_none());
    assert!(!lease.is_ready());
    lease.release().await.unwrap();
    assert_eq!(fake.active.load(Ordering::SeqCst), 0);
    stop.cancel();
    driver.await.unwrap();
}
