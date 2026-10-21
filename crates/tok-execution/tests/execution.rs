//! 同一 Layer 处理不同来源与结果，保留局部借用，覆盖就绪/执行预算与取消。
use std::{
    future::{Future, Pending, pending, ready},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tok_core::service::Identity;
use tok_execution::{Context, Error, Request, StopReason};
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service, ServiceExt, service_fn};

#[derive(Debug, Clone)]
struct Tick {
    fired_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("业务拒绝：{0}")]
struct Rejected(u32);
fn context(operation: &str, budget: Duration, cancellation: CancellationToken) -> Context {
    Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        operation,
        budget,
        cancellation,
    )
    .unwrap()
}
fn call_context(operation: &str) -> Context {
    context(operation, Duration::from_secs(2), CancellationToken::new())
}
#[derive(Clone)]
struct AuditLayer(Arc<Mutex<Vec<String>>>);
#[derive(Clone)]
struct Audit<S> {
    inner: S,
    log: Arc<Mutex<Vec<String>>>,
}
impl<S> Layer<S> for AuditLayer {
    type Service = Audit<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Audit {
            inner,
            log: Arc::clone(&self.0),
        }
    }
}
struct Audited<F> {
    inner: Pin<Box<F>>,
    operation: String,
    log: Arc<Mutex<Vec<String>>>,
}
impl<F, T, E> Future for Audited<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll(cx);
        if result.is_ready() {
            this.log
                .lock()
                .unwrap()
                .push(format!("after:{}", this.operation));
        }
        result
    }
}
impl<S, T> Service<Request<T>> for Audit<S>
where
    S: Service<Request<T>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Audited<S::Future>;
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let operation = request.context.operation().to_owned();
        self.log.lock().unwrap().push(format!("before:{operation}"));
        Audited {
            inner: Box::pin(self.inner.call(request)),
            operation,
            log: Arc::clone(&self.log),
        }
    }
}
#[derive(Default)]
struct State {
    value: i64,
}
struct Added {
    delta: i64,
} // 普通 Rust 事件，无 protobuf 或序列化实现。
fn add<'a>(
    request: Request<(&'a mut State, &'a Added)>,
) -> Pin<Box<dyn Future<Output = Result<(), Rejected>> + Send + 'a>> {
    Box::pin(async move {
        tokio::task::yield_now().await;
        let (state, event) = request.input;
        if event.delta < 0 {
            return Err(Rejected(17));
        }
        state.value += event.delta;
        Ok(())
    })
}
#[tokio::test]
async fn one_layer_handles_rpc_client_event_and_timer_with_typed_results_and_borrowed_state() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let audit = AuditLayer(Arc::clone(&log));
    let budget = tok_execution::Layer;
    let rpc = budget.layer(audit.layer(service_fn(|request: Request<u32>| {
        ready(Ok::<_, Rejected>(request.input + 1))
    })));
    assert_eq!(
        rpc.oneshot(Request::new(call_context("rpc"), 41))
            .await
            .unwrap(),
        42
    );
    let parent = call_context("outer");
    let child = parent.child("event").unwrap();
    assert_eq!(child.deadline(), parent.deadline());
    let mut state = State::default();
    let event = Added { delta: 5 };
    let event_handler = budget.layer(audit.layer(service_fn(add)));
    event_handler
        .clone()
        .oneshot(Request::new(child, (&mut state, &event)))
        .await
        .unwrap();
    let invalid = Added { delta: -1 };
    let error = event_handler
        .oneshot(Request::new(
            parent.child("rejected").unwrap(),
            (&mut state, &invalid),
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Inner(Rejected(17))));
    // 同步处理者只需 ready 适配，不改变借用或结果类型。
    let sync = budget.layer(audit.layer(service_fn(|request: Request<&mut State>| {
        request.input.value += 2;
        ready(Ok::<_, Rejected>(request.input.value))
    })));
    let client = parent.child("client").unwrap();
    assert_eq!(
        sync.oneshot(Request::new(client, &mut state))
            .await
            .unwrap(),
        7
    );
    let trigger = Tick { fired_at_ms: 2 };
    let timer = budget.layer(audit.layer(service_fn(|request: Request<()>| {
        ready(Ok::<_, Rejected>(
            request
                .extensions()
                .get::<Tick>()
                .map(|trigger| trigger.fired_at_ms),
        ))
    })));
    let mut timer_context = Request::new(parent.child("timer").unwrap(), ());
    timer_context.extensions_mut().insert(trigger);
    assert_eq!(timer.oneshot(timer_context).await.unwrap(), Some(2));
    assert_eq!(state.value, 7);
    assert_eq!(
        *log.lock().unwrap(),
        [
            "before:rpc",
            "after:rpc",
            "before:event",
            "after:event",
            "before:rejected",
            "after:rejected",
            "before:client",
            "after:client",
            "before:timer",
            "after:timer"
        ]
    );
}

#[derive(Clone)]
struct NotReady {
    polls: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    failure: bool,
}
impl Service<Request<()>> for NotReady {
    type Response = ();
    type Error = Rejected;
    type Future = Pending<Result<(), Rejected>>;
    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.failure {
            Poll::Ready(Err(Rejected(23)))
        } else {
            Poll::Pending
        }
    }
    fn call(&mut self, _request: Request<()>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        pending()
    }
}
#[tokio::test]
async fn readiness_is_inside_the_budget_and_never_starts_business_after_expiry() {
    let polls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let service = tok_execution::Layer.layer(NotReady {
        polls: Arc::clone(&polls),
        calls: Arc::clone(&calls),
        failure: false,
    });
    let context = context(
        "waiting",
        Duration::from_millis(50),
        CancellationToken::new(),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        service.oneshot(Request::new(context, ())),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert!(polls.load(Ordering::SeqCst) > 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn readiness_errors_keep_the_original_error_type() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = tok_execution::Layer.layer(NotReady {
        polls: Arc::new(AtomicUsize::new(0)),
        calls: Arc::clone(&calls),
        failure: true,
    });
    assert!(matches!(
        service
            .oneshot(Request::new(call_context("waiting"), ()))
            .await,
        Err(Error::Inner(Rejected(23)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn parent_cancellation_wakes_and_drops_inflight_work() {
    let dropped = Arc::new(AtomicBool::new(false));
    let started = Arc::new(tokio::sync::Notify::new());
    let (signal, observed) = (Arc::clone(&dropped), Arc::clone(&started));
    let inner = service_fn(move |_request: Request<()>| {
        let guard = Dropped(Arc::clone(&signal));
        let started = Arc::clone(&observed);
        async move {
            let _guard = guard;
            started.notify_one();
            pending::<Result<(), Rejected>>().await
        }
    });
    let parent = call_context("parent");
    let cancellation = parent.cancellation();
    let child = parent.child("child").unwrap();
    // owned 调用自动满足 Send，可交给运行时；借用调用不要求这么做。
    let task = tokio::spawn(
        tok_execution::Layer
            .layer(inner)
            .oneshot(Request::new(child, ())),
    );
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Stopped(StopReason::Cancelled))
    ));
    assert!(dropped.load(Ordering::SeqCst));
}
#[tokio::test]
async fn dropping_call_releases_borrowed_handler_without_cancelling_parent() {
    let dropped = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&dropped);
    let parent = call_context("parent");
    let cancellation = parent.cancellation();
    let mut state = State::default();
    let inner = service_fn(move |request: Request<&mut State>| {
        request.input.value += 1;
        let guard = Dropped(Arc::clone(&signal));
        async move {
            let _guard = guard;
            pending::<Result<(), Rejected>>().await
        }
    });
    let mut future = Box::pin(
        tok_execution::Layer
            .layer(inner)
            .oneshot(Request::new(parent, &mut state)),
    );
    assert!(
        future
            .as_mut()
            .poll(&mut TaskContext::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(future);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(state.value, 1);
    assert!(!cancellation.is_cancelled());
}
#[tokio::test]
async fn cancelled_or_expired_calls_do_not_poll_readiness_or_execute_handlers() {
    for cancelled in [false, true] {
        let token = CancellationToken::new();
        let context = context(
            "stopped",
            if cancelled {
                Duration::from_secs(1)
            } else {
                Duration::ZERO
            },
            token.clone(),
        );
        if cancelled {
            token.cancel();
        }
        let polls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let service = tok_execution::Layer.layer(NotReady {
            polls: Arc::clone(&polls),
            calls: Arc::clone(&calls),
            failure: false,
        });
        let result = service.oneshot(Request::new(context, ())).await;
        assert!(matches!(result, Err(Error::Stopped(_))));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn a_non_cooperative_handler_cannot_return_success_after_the_original_deadline() {
    let inner = service_fn(|_request: Request<()>| {
        std::thread::sleep(Duration::from_millis(40));
        ready(Ok::<_, Rejected>(42))
    });
    let context = context("late", Duration::from_millis(10), CancellationToken::new());
    assert!(matches!(
        tok_execution::Layer
            .layer(inner)
            .oneshot(Request::new(context, ()))
            .await,
        Err(Error::Stopped(StopReason::DeadlineExceeded))
    ));
}

#[tokio::test]
async fn captured_span_is_active_before_and_after_an_async_yield() {
    use tracing::instrument::WithSubscriber;
    async {
        let span = tracing::info_span!("execution_test");
        let expected = span.id();
        assert!(expected.is_some());
        let context = {
            let _entered = span.enter();
            call_context("trace")
        };
        let parent = context.span().id();
        let child = context.child("trace.event").unwrap();
        assert_eq!(child.span().id(), parent);
        let handler = service_fn(move |_request: Request<()>| {
            let expected = expected.clone();
            async move {
                assert_eq!(tracing::Span::current().id(), expected);
                tokio::task::yield_now().await;
                assert_eq!(tracing::Span::current().id(), expected);
                Ok::<_, Rejected>(())
            }
        });
        tok_execution::Layer
            .layer(handler)
            .oneshot(Request::new(child, ()))
            .await
            .unwrap();
    }
    .with_subscriber(tracing_subscriber::registry())
    .await;
}

#[tokio::test]
async fn ordered_borrowed_event_handlers_stop_on_the_first_business_error() {
    type EventFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Rejected>> + Send + 'a>>;
    type EventHandler = for<'a> fn(Request<(&'a mut State, &'a Added)>) -> EventFuture<'a>;
    fn reject<'a>(_request: Request<(&'a mut State, &'a Added)>) -> EventFuture<'a> {
        Box::pin(ready(Err(Rejected(31))))
    }
    let handlers: [EventHandler; 3] = [add, reject, add];
    let parent = call_context("parent");
    let child = parent.child("ordered").unwrap();
    let mut state = State::default();
    let event = Added { delta: 3 };
    let result = async {
        for handler in handlers {
            tok_execution::Layer
                .layer(service_fn(handler))
                .oneshot(Request::new(child.clone(), (&mut state, &event)))
                .await?;
        }
        Ok::<_, Error<Rejected>>(())
    }
    .await;
    assert!(matches!(result, Err(Error::Inner(Rejected(31)))));
    assert_eq!(state.value, 3); // 前一处理者已完成，不回滚；后一处理者未执行。
}

#[test]
fn execution_error_exposes_the_original_error_in_its_source_chain() {
    let error = Error::Inner(Rejected(41));
    let source = std::error::Error::source(&error).unwrap();
    assert_eq!(source.downcast_ref::<Rejected>(), Some(&Rejected(41)));
}

#[derive(Clone)]
struct LateReady {
    token: CancellationToken,
    calls: Arc<AtomicUsize>,
    expire: bool,
}
impl Service<Request<()>> for LateReady {
    type Response = ();
    type Error = Rejected;
    type Future = std::future::Ready<Result<(), Rejected>>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Rejected>> {
        if self.expire {
            std::thread::sleep(Duration::from_millis(30));
        } else {
            self.token.cancel();
        }
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Request<()>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(()))
    }
}
#[tokio::test]
async fn scope_stopped_during_readiness_never_starts_business() {
    for expire in [false, true] {
        let token = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let context = context(
            "late-ready",
            if expire {
                Duration::from_millis(5)
            } else {
                Duration::from_secs(1)
            },
            token.clone(),
        );
        let result = tok_execution::Layer
            .layer(LateReady {
                token,
                calls: Arc::clone(&calls),
                expire,
            })
            .oneshot(Request::new(context, ()))
            .await;
        let expected = if expire {
            StopReason::DeadlineExceeded
        } else {
            StopReason::Cancelled
        };
        assert!(matches!(result, Err(Error::Stopped(reason)) if reason == expected));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
