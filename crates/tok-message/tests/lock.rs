//! 请求锁 Layer 的预算、错误及取消清理语义。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::BoxFuture;
use tok_core::lock::{Backend as LockBackend, Key as LockKey, Lease as LockLease};
use tok_execution::lock::Config;
use tok_grpc::{Client, Listener};
use tok_message::rpc::CallContext;
use tok_message::rpc::{
    lock::Layer,
    tower::{self, Layer as _, ServiceExt},
};
use tok_proto::internal::v1::{CallResponse, Payload};
use tonic::{Code, Status};

fn context() -> CallContext {
    CallContext {
        node_id: "node-a".into(),
        service_name: "guild".into(),
        bid: Some("7".into()),
        request_id: 1,
        session: None,
        timer: None,
    }
}
fn handler(
    calls: Arc<AtomicUsize>,
    delay: Duration,
    fail: bool,
) -> impl tower::Service<
    tok_execution::Request<(CallContext, Payload)>,
    Response = tok_execution::Response<CallResponse>,
    Error = Status,
    Future: Send,
> + Clone
+ Send
+ Sync {
    tower::service_fn(move |_| {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
            if fail {
                Err(Status::permission_denied("业务拒绝"))
            } else {
                Ok(tok_execution::Response::new(CallResponse::default()))
            }
        }
    })
}
fn execution<T>(input: T) -> tok_execution::Request<T> {
    let context = tok_execution::Context::new(
        tok_core::service::Identity {
            node_id: "node-a".into(),
            service_name: "guild".into(),
        },
        "test",
        Duration::from_secs(30),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    tok_execution::Request::new(context, input)
}
fn request() -> tok_execution::Request<(CallContext, Payload)> {
    execution((context(), Payload::default()))
}

fn layer(backend: Arc<dyn LockBackend>) -> Layer {
    Layer::new(backend, "guild", Config::default()).unwrap()
}
struct Backend {
    acquire_delay: Duration,
    release_delay: Duration,
    release_fails: bool,
    released: Arc<AtomicUsize>,
}
struct Lease {
    delay: Duration,
    fail: bool,
    released: Arc<AtomicUsize>,
}
impl LockLease for Lease {
    fn token(&self) -> &str {
        "test"
    }
    fn release(self: Box<Self>) -> BoxFuture<'static, tok_core::lock::Result<()>> {
        Box::pin(async move {
            self.released.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            if self.fail {
                Err(tok_core::lock::Error::Unavailable("test".into()))
            } else {
                Ok(())
            }
        })
    }
}
impl LockBackend for Backend {
    fn acquire(
        &self,
        _: LockKey,
        _: Duration,
        _: Duration,
    ) -> BoxFuture<'_, tok_core::lock::Result<Box<dyn LockLease>>> {
        Box::pin(async move {
            tokio::time::sleep(self.acquire_delay).await;
            Ok(Box::new(Lease {
                delay: self.release_delay,
                fail: self.release_fails,
                released: Arc::clone(&self.released),
            }) as Box<dyn LockLease>)
        })
    }
}
fn backend(
    acquire_delay: Duration,
    release_delay: Duration,
    release_fails: bool,
    released: &Arc<AtomicUsize>,
) -> Arc<dyn LockBackend> {
    Arc::new(Backend {
        acquire_delay,
        release_delay,
        release_fails,
        released: Arc::clone(released),
    })
}

#[tokio::test(start_paused = true)]
async fn release_is_awaited_on_success_business_error_and_execution_timeout() {
    for (delay, fail, code) in [
        (Duration::ZERO, false, None),
        (Duration::ZERO, true, Some(Code::PermissionDenied)),
        (Duration::from_secs(2), false, Some(Code::DeadlineExceeded)),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let service = layer(backend(Duration::ZERO, Duration::ZERO, false, &released))
            .layer(handler(Arc::clone(&calls), delay, fail));
        let result = service.oneshot(request()).await;
        assert_eq!(result.err().map(|error| error.code()), code);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn release_failure_or_timeout_keeps_business_error_and_fails_success() {
    for release_delay in [Duration::ZERO, Duration::from_secs(1)] {
        for fail in [false, true] {
            let released = Arc::new(AtomicUsize::new(0));
            let service = layer(backend(Duration::ZERO, release_delay, true, &released))
                .layer(handler(Arc::new(AtomicUsize::new(0)), Duration::ZERO, fail));
            let error = service.oneshot(request()).await.unwrap_err();
            assert_eq!(
                error.code(),
                if fail {
                    Code::PermissionDenied
                } else {
                    Code::Unavailable
                }
            );
            assert_eq!(released.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn acquire_latency_is_subtracted_and_insufficient_budget_skips_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(AtomicUsize::new(0));
    let config = Config {
        acquire_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let layer = Layer::new(
        backend(Duration::from_secs(2), Duration::ZERO, false, &released),
        "guild",
        config,
    )
    .unwrap();
    let result = layer
        .layer(handler(Arc::clone(&calls), Duration::ZERO, false))
        .oneshot(request())
        .await;
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn unresponsive_acquire_is_bounded_and_invalid_context_does_not_execute() {
    let calls = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(AtomicUsize::new(0));
    let service = layer(backend(
        Duration::from_secs(10),
        Duration::ZERO,
        false,
        &released,
    ))
    .layer(handler(Arc::clone(&calls), Duration::ZERO, false));
    assert_eq!(
        service.clone().oneshot(request()).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    for bid in [None, Some("".into()), Some(" ".into())] {
        let mut context = context();
        context.bid = bid;
        assert_eq!(
            service
                .clone()
                .oneshot(execution((context, Payload::default())))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    let mut context = context();
    context.service_name = "other".into();
    assert_eq!(
        service
            .oneshot(execution((context, Payload::default())))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(released.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn outer_cancellation_leaves_lock_until_ttl_and_capacity_errors_are_distinct() {
    let memory = Arc::new(tok_infra::locks::Memory::new(1).unwrap());
    let backend: Arc<dyn LockBackend> = memory;
    let calls = Arc::new(AtomicUsize::new(0));
    let service = layer(backend).layer(handler(Arc::clone(&calls), Duration::from_secs(10), false));
    let task = tokio::spawn(service.clone().oneshot(request()));
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let mut other = context();
    other.bid = Some("8".into());
    assert_eq!(
        service
            .clone()
            .oneshot(execution((other, Payload::default())))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        service.clone().oneshot(request()).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    tokio::time::advance(Duration::from_secs(3)).await;
    // 新调用确实进入业务，之后因业务超时而失败；之前取消的调用没有触发异步释放。
    assert_eq!(
        service.oneshot(request()).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn invalid_budgets_are_rejected_before_backend_access() {
    let backend: Arc<dyn LockBackend> = Arc::new(tok_infra::locks::Memory::new(1).unwrap());
    let mut config = Config::default();
    config.ttl = config.execution_timeout;
    assert!(Layer::new(Arc::clone(&backend), "guild", config).is_err());
    assert!(Layer::new(Arc::clone(&backend), " ", Config::default()).is_err());
    let tiny = Config {
        ttl: Duration::from_micros(500),
        execution_timeout: Duration::from_micros(100),
        release_timeout: Duration::from_micros(100),
        ..Default::default()
    };
    assert!(Layer::new(backend, "guild", tiny).is_err());
}

#[tokio::test]
async fn business_rpc_on_two_nodes_shares_locks_after_target_validation() {
    use std::{collections::BTreeMap, sync::Mutex};
    use tok_core::ServiceDescriptor;
    use tok_message::rpc::{Service as RpcService, TowerHandler};
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    let backend: Arc<dyn LockBackend> = Arc::new(tok_infra::locks::Memory::new(16).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
    let total = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut nodes = Vec::new();
    let stop = CancellationToken::new();
    let mut servers = Vec::new();
    for node in ["node-a", "node-b"] {
        let (calls, active, total, peak) = (
            Arc::clone(&calls),
            Arc::clone(&active),
            Arc::clone(&total),
            Arc::clone(&peak),
        );
        let handler = tower::service_fn(
            move |request: tok_execution::Request<(CallContext, Payload)>| {
                let (context, _payload) = request.input;
                let (calls, active, total, peak) = (
                    Arc::clone(&calls),
                    Arc::clone(&active),
                    Arc::clone(&total),
                    Arc::clone(&peak),
                );
                async move {
                    let bid = context.bid.unwrap();
                    {
                        let mut active = active.lock().unwrap();
                        let count = active.entry(bid.clone()).or_default();
                        *count += 1;
                        assert_eq!(*count, 1, "相同 bid 不得同时进入 handler");
                    }
                    calls.fetch_add(1, Ordering::SeqCst);
                    let count = total.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    *active.lock().unwrap().get_mut(&bid).unwrap() -= 1;
                    total.fetch_sub(1, Ordering::SeqCst);
                    Ok(tok_execution::Response::new(CallResponse::default()))
                }
            },
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = ServiceDescriptor {
            node_id: node.into(),
            service_name: "guild".into(),
            protocol: "grpc".into(),
            address: listener.local_addr().unwrap(),
        };
        let endpoint = Listener::new()
            .add_service(
                "guild",
                RpcService::new(
                    node,
                    "guild",
                    Arc::new(TowerHandler(layer(Arc::clone(&backend)).layer(handler))),
                )
                .unwrap()
                .into_business(),
            )
            .unwrap();
        endpoint.readiness().set("guild", true);
        servers.push(tokio::spawn(
            endpoint.serve(listener, stop.clone()).unwrap(),
        ));
        nodes.push(target);
    }
    let clients = Arc::new(Client::new(Duration::from_secs(2), 4).unwrap());
    let business_client = tok_message::rpc::business::Client::new(Arc::clone(&clients));
    let context_for = |node: &ServiceDescriptor, bid: &str| CallContext {
        node_id: node.node_id.clone(),
        bid: Some(bid.into()),
        ..context()
    };
    let payload = || Payload {
        message_id: 1,
        body: vec![],
    };
    let mut raw = tok_proto::internal::v1::business_rpc_client::BusinessRpcClient::connect(
        format!("http://{}", nodes[0].address),
    )
    .await
    .unwrap();
    let invalid = tok_proto::internal::v1::CallRequest {
        context: Some(tok_proto::internal::v1::CallContext {
            node_id: "wrong-node".into(),
            service_name: "guild".into(),
            bid: Some("7".into()),
            request_id: 1,
            session: None,
        }),
        payload: Some(payload()),
        timer: None,
    };
    assert_eq!(
        raw.call(invalid).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut missing = context_for(&nodes[0], "7");
    missing.bid = None;
    assert_eq!(
        business_client
            .call(
                &nodes[0],
                root((missing, payload()), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let (a, b, c, d) = tokio::join!(
        business_client.call(
            &nodes[0],
            root(
                (context_for(&nodes[0], "7"), payload()),
                tracing::Span::current()
            )
        ),
        business_client.call(
            &nodes[1],
            root(
                (context_for(&nodes[1], "7"), payload()),
                tracing::Span::current()
            )
        ),
        business_client.call(
            &nodes[0],
            root(
                (context_for(&nodes[0], "8"), payload()),
                tracing::Span::current()
            )
        ),
        business_client.call(
            &nodes[1],
            root(
                (context_for(&nodes[1], "9"), payload()),
                tracing::Span::current()
            )
        ),
    );
    for result in [a, b, c, d] {
        assert_eq!(result.unwrap().output.request_id, 1);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert!(peak.load(Ordering::SeqCst) > 1, "不同 bid 应可并行");
    stop.cancel();
    for server in servers {
        server.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn encoded_business_failure_is_preserved_when_release_fails() {
    let released = Arc::new(AtomicUsize::new(0));
    let service = tower::service_fn(|_| async {
        Ok(tok_execution::Response::new(CallResponse {
            error: Some(tok_proto::internal::v1::RpcError {
                code: 7,
                message: "业务拒绝".into(),
            }),
            ..CallResponse::default()
        }))
    });
    let response = layer(backend(Duration::ZERO, Duration::ZERO, true, &released))
        .layer(service)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(response.output.error.unwrap().code, 7);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct NeverReady;
impl tower::Service<tok_execution::Request<(CallContext, Payload)>> for NeverReady {
    type Response = tok_execution::Response<CallResponse>;
    type Error = Status;
    type Future = tok_message::rpc::CallTask<'static>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Status>> {
        std::task::Poll::Pending
    }
    fn call(&mut self, _: tok_execution::Request<(CallContext, Payload)>) -> Self::Future {
        panic!("未就绪时不得执行业务")
    }
}
#[tokio::test(start_paused = true)]
async fn inner_readiness_is_bounded_and_lease_is_released() {
    let released = Arc::new(AtomicUsize::new(0));
    let service =
        layer(backend(Duration::ZERO, Duration::ZERO, false, &released)).layer(NeverReady);
    assert_eq!(
        service.oneshot(request()).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

struct LateBackend(Arc<AtomicUsize>);
impl LockBackend for LateBackend {
    fn acquire(
        &self,
        _: LockKey,
        _: Duration,
        _: Duration,
    ) -> BoxFuture<'_, tok_core::lock::Result<Box<dyn LockLease>>> {
        Box::pin(async move {
            // 模拟后端 future 在一次 poll 内阻塞，直到期限后才返回成功。
            std::thread::sleep(Duration::from_millis(20));
            Ok(Box::new(Lease {
                delay: Duration::ZERO,
                fail: false,
                released: Arc::clone(&self.0),
            }) as Box<dyn LockLease>)
        })
    }
}
#[tokio::test]
async fn late_ready_futures_cannot_bypass_acquire_or_execution_deadlines() {
    let released = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let config = Config {
        wait: Duration::ZERO,
        acquire_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let layer = Layer::new(
        Arc::new(LateBackend(Arc::clone(&released))),
        "guild",
        config,
    )
    .unwrap();
    let result = layer
        .layer(handler(Arc::clone(&calls), Duration::ZERO, false))
        .oneshot(request())
        .await;
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(released.load(Ordering::SeqCst), 1);

    let config = Config {
        execution_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let layer = Layer::new(
        backend(Duration::ZERO, Duration::ZERO, false, &released),
        "guild",
        config,
    )
    .unwrap();
    let service = tower::service_fn(|_| async {
        std::thread::sleep(Duration::from_millis(20));
        Ok(tok_execution::Response::new(CallResponse::default()))
    });
    assert_eq!(
        layer
            .layer(service)
            .oneshot(request())
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    assert_eq!(released.load(Ordering::SeqCst), 2);
}

fn root<T>(input: T, span: tracing::Span) -> tok_execution::Request<T> {
    tok_execution::Request::new(
        tok_execution::Context::new(
            tok_core::Identity {
                node_id: "local".into(),
                service_name: "test-caller".into(),
            },
            "test-call",
            std::time::Duration::from_secs(30),
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap()
        .with_span(span),
        input,
    )
}
