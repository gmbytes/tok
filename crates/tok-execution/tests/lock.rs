//! 纯类型化输入、回应与业务错误的请求锁，不依赖 RPC 或生成协议。
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{
    BoxFuture, Identity,
    lock::{self as contract, Key},
};
use tok_execution::{
    Context, Request, StopReason,
    lock::{Config, Error, Layer},
};
use tokio_util::sync::CancellationToken;
use tower::{Layer as _, ServiceExt};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum DomainError {
    #[error("实体无效")]
    InvalidEntity,
    #[error("业务拒绝")]
    Denied,
}
struct Input {
    entity: u64,
}
#[derive(Debug)]
struct Reply {
    accepted: bool,
    value: u64,
}
#[derive(Default)]
struct Observed {
    keys: Mutex<Vec<Key>>,
    releases: AtomicUsize,
}
struct Backend {
    observed: Arc<Observed>,
    delay: Duration,
    fail_acquire: bool,
    fail_release: bool,
    block_release: bool,
}
struct Lease {
    observed: Arc<Observed>,
    fail: bool,
    block: bool,
}
impl contract::Lease for Lease {
    fn token(&self) -> &str {
        "test"
    }
    fn release(self: Box<Self>) -> BoxFuture<'static, contract::Result<()>> {
        Box::pin(async move {
            self.observed.releases.fetch_add(1, Ordering::SeqCst);
            if self.block {
                std::thread::sleep(Duration::from_millis(20));
            }
            if self.fail {
                Err(contract::Error::Unavailable("测试释放失败".into()))
            } else {
                Ok(())
            }
        })
    }
}
impl contract::Backend for Backend {
    fn acquire(
        &self,
        key: Key,
        _: Duration,
        _: Duration,
    ) -> BoxFuture<'_, contract::Result<Box<dyn contract::Lease>>> {
        Box::pin(async move {
            self.observed.keys.lock().unwrap().push(key);
            tokio::time::sleep(self.delay).await;
            if self.fail_acquire {
                return Err(contract::Error::CapacityExceeded);
            }
            Ok(Box::new(Lease {
                observed: Arc::clone(&self.observed),
                fail: self.fail_release,
                block: self.block_release,
            }) as Box<dyn contract::Lease>)
        })
    }
}
fn backend(observed: &Arc<Observed>, fail_release: bool) -> Backend {
    Backend {
        observed: Arc::clone(observed),
        delay: Duration::ZERO,
        fail_acquire: false,
        fail_release,
        block_release: false,
    }
}
fn request(entity: u64, budget: Duration) -> Request<Input> {
    Request::new(
        Context::new(
            Identity {
                node_id: "node".into(),
                service_name: "inventory".into(),
            },
            "inventory/update",
            budget,
            CancellationToken::new(),
        )
        .unwrap(),
        Input { entity },
    )
}
fn key(request: &Request<Input>) -> Result<Key, DomainError> {
    if request.input.entity == 0 {
        return Err(DomainError::InvalidEntity);
    }
    Ok(Key {
        namespace: "inventory".into(),
        bid: request.input.entity.to_string(),
    })
}
fn success(reply: &Reply) -> bool {
    reply.accepted
}

#[tokio::test]
async fn typed_input_and_response_preserve_context_and_key_errors_skip_backend() {
    let observed = Arc::new(Observed::default());
    let layer = Layer::new(
        Arc::new(backend(&observed, false)),
        Config::default(),
        key,
        success,
    )
    .unwrap();
    let service = layer.layer(tower::service_fn(|request: Request<Input>| async move {
        assert_eq!(request.context.operation(), "inventory/update");
        assert_eq!(request.context.extensions().get::<u64>(), Some(&42));
        Ok::<_, DomainError>(Reply {
            accepted: true,
            value: request.input.entity + 1,
        })
    }));
    let mut input = request(7, Duration::from_secs(30));
    input.context.extensions_mut().insert(42u64);
    assert_eq!(service.clone().oneshot(input).await.unwrap().value, 8);
    assert_eq!(
        *observed.keys.lock().unwrap(),
        vec![Key {
            namespace: "inventory".into(),
            bid: "7".into()
        }]
    );
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert!(matches!(
        service.oneshot(request(0, Duration::from_secs(30))).await,
        Err(Error::Key(DomainError::InvalidEntity))
    ));
    assert_eq!(observed.keys.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn release_failure_preserves_domain_error_or_encoded_failure_and_overrides_success() {
    for mode in 0..3 {
        let observed = Arc::new(Observed::default());
        let layer = Layer::new(
            Arc::new(backend(&observed, true)),
            Config::default(),
            key,
            success,
        )
        .unwrap();
        let service = layer.layer(tower::service_fn(move |_: Request<Input>| async move {
            if mode == 0 {
                Err(DomainError::Denied)
            } else {
                Ok(Reply {
                    accepted: mode == 2,
                    value: 42,
                })
            }
        }));
        let result = service.oneshot(request(7, Duration::from_secs(30))).await;
        match mode {
            0 => assert!(matches!(result, Err(Error::Inner(DomainError::Denied)))),
            1 => assert!(!result.unwrap().accepted),
            _ => assert!(matches!(
                result,
                Err(Error::Release(contract::Error::Unavailable(_)))
            )),
        }
        assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn original_budget_and_cancellation_cover_acquire_without_executing_business() {
    let observed = Arc::new(Observed::default());
    let mut backend = backend(&observed, false);
    backend.delay = Duration::from_secs(10);
    let layer = Layer::new(Arc::new(backend), Config::default(), key, success).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let executed = Arc::clone(&calls);
    let service = layer.layer(tower::service_fn(move |_: Request<Input>| {
        let executed = Arc::clone(&executed);
        async move {
            executed.fetch_add(1, Ordering::SeqCst);
            Ok::<_, DomainError>(Reply {
                accepted: true,
                value: 0,
            })
        }
    }));
    let cancelled = request(7, Duration::from_secs(30));
    cancelled.context.cancellation().cancel();
    assert!(matches!(
        service.clone().oneshot(cancelled).await,
        Err(Error::Stopped(StopReason::Cancelled))
    ));
    assert!(observed.keys.lock().unwrap().is_empty());
    assert!(matches!(
        service.oneshot(request(7, Duration::from_millis(20))).await,
        Err(Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert_eq!(observed.keys.lock().unwrap().len(), 1);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn backend_error_keeps_its_category_and_does_not_release_unacquired_lease() {
    let observed = Arc::new(Observed::default());
    let mut backend = backend(&observed, false);
    backend.fail_acquire = true;
    let layer = Layer::new(Arc::new(backend), Config::default(), key, success).unwrap();
    let service = layer.layer(tower::service_fn(|_: Request<Input>| async {
        Err::<Reply, _>(DomainError::Denied)
    }));
    assert!(matches!(
        service.oneshot(request(7, Duration::from_secs(30))).await,
        Err(Error::Acquire(contract::Error::CapacityExceeded))
    ));
    assert_eq!(observed.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn late_non_cooperative_release_is_not_accepted_as_timely_success() {
    let observed = Arc::new(Observed::default());
    let mut backend = backend(&observed, false);
    backend.block_release = true;
    let config = Config {
        release_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let layer = Layer::new(Arc::new(backend), config, key, success).unwrap();
    let service = layer.layer(tower::service_fn(|_: Request<Input>| async {
        Ok::<_, DomainError>(Reply {
            accepted: true,
            value: 42,
        })
    }));
    assert!(matches!(
        service.oneshot(request(7, Duration::from_secs(30))).await,
        Err(Error::ReleaseTimeout)
    ));
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
}
