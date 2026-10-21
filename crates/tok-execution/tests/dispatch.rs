//! 自定义键、普通 Rust 输入、原始业务错误与同一执行预算。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::Identity;
use tok_execution::{
    Context, Request, Response, StopReason,
    registry::{DispatchError, Error, Registry, Table},
};
use tokio_util::sync::CancellationToken;

// 不实现 Display，证明键不必具备数字或文本协议的格式化契约。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Action {
    Increment,
    Missing,
}
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("业务拒绝：{0}")]
struct Rejected(u32);

fn context(budget: Duration, stop: CancellationToken) -> Context {
    Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "custom".into(),
        },
        "local.increment",
        budget,
        stop,
    )
    .unwrap()
}

#[tokio::test]
async fn enum_keys_plain_rust_values_and_original_errors_share_the_execution_chain() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut registry = Table::<Action, u32, String, Rejected>::new();
    registry
        .register_service(
            Action::Increment,
            tower::service_fn(move |request: Request<u32>| {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.context.operation(), "local.increment");
                    assert_eq!(request.extensions().get::<u64>(), Some(&42));
                    if request.input == 0 {
                        return Err(Rejected(0));
                    }
                    Ok(Response::new((request.input + 1).to_string()))
                }
            }),
        )
        .unwrap();
    let mut ctx = Request::new(
        context(Duration::from_secs(1), CancellationToken::new()),
        (),
    );
    ctx.extensions_mut().insert(42u64);
    assert_eq!(
        registry
            .clone()
            .dispatch(&Action::Increment, ctx.clone().map(|()| 7))
            .await
            .unwrap()
            .output,
        "8"
    );
    assert!(matches!(
        registry
            .dispatch(&Action::Increment, ctx.clone().map(|()| 0))
            .await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            Rejected(0)
        )))
    ));
    assert!(matches!(
        registry.dispatch(&Action::Missing, ctx.map(|()| 7)).await,
        Err(tok_execution::Error::Inner(DispatchError::Unknown(
            Action::Missing
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn key_validity_is_owned_by_the_caller_and_duplicate_registration_preserves_the_chain() {
    let mut numeric = Table::<u64, u32, u32>::new();
    numeric
        .register(
            0,
            |request| async move { Ok(Response::new(request.input + 1)) },
        )
        .unwrap();
    assert_eq!(
        numeric.register(
            0,
            |request| async move { Ok(Response::new(request.input + 99)) }
        ),
        Err(Error::DuplicateKey(0))
    );
    let ctx = context(Duration::from_secs(1), CancellationToken::new());
    assert_eq!(
        numeric
            .dispatch(&0, Request::new(ctx.clone(), 1))
            .await
            .unwrap()
            .output,
        2
    );
    let mut textual = Table::<String, u32, u32>::new();
    textual
        .register(String::new(), |request| async move {
            Ok(Response::new(request.input))
        })
        .unwrap();
    assert_eq!(
        textual
            .dispatch(&String::new(), Request::new(ctx, 42))
            .await
            .unwrap()
            .output,
        42
    );
}

#[derive(Clone)]
struct NeverReady(Arc<AtomicUsize>);
impl tower::Service<Request<()>> for NeverReady {
    type Response = Response<()>;
    type Error = Rejected;
    type Future = std::future::Ready<Result<Response<()>, Rejected>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Rejected>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Pending
    }
    fn call(&mut self, _: Request<()>) -> Self::Future {
        panic!("unready service must not run")
    }
}
#[tokio::test]
async fn readiness_wait_uses_original_deadline_and_cancelled_requests_do_not_start() {
    let polls = Arc::new(AtomicUsize::new(0));
    let mut registry = Table::new();
    registry
        .register_service(Action::Increment, NeverReady(Arc::clone(&polls)))
        .unwrap();
    let stop = CancellationToken::new();
    stop.cancel();
    let error = registry
        .dispatch(
            &Action::Increment,
            Request::new(context(Duration::from_secs(1), stop), ()),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        tok_execution::Error::Stopped(StopReason::Cancelled)
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let error = registry
        .dispatch(
            &Action::Increment,
            Request::new(
                context(Duration::from_millis(20), CancellationToken::new()),
                (),
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        tok_execution::Error::Stopped(StopReason::DeadlineExceeded)
    ));
    assert!(polls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn registry_service_composes_common_and_route_layers_without_losing_context_or_errors() {
    use tower::ServiceExt as _;
    let mut registry = Table::<Action, (Action, u32), u32, Rejected>::new();
    let route = tower::ServiceBuilder::new()
        .map_request(|mut request: Request<(Action, u32)>| {
            assert_eq!(request.extensions().get::<u64>(), Some(&41));
            request.extensions_mut().insert(42u64);
            request
        })
        .service(tower::service_fn(
            |request: Request<(Action, u32)>| async move {
                assert_eq!(request.extensions().get::<u64>(), Some(&42));
                assert_eq!(request.context.operation(), "local.increment");
                if request.input.1 == 0 {
                    return Err(Rejected(0));
                }
                Ok(Response::new(request.input.1 + 1))
            },
        ));
    registry.register_service(Action::Increment, route).unwrap();
    let service = tower::ServiceBuilder::new()
        .map_request(|mut request: Request<(Action, u32)>| {
            request.extensions_mut().insert(41u64);
            request
        })
        .service(registry.into_service(|request| request.input.0.clone()));
    let context = context(Duration::from_secs(1), CancellationToken::new());
    assert_eq!(
        service
            .clone()
            .oneshot(Request::new(context.clone(), (Action::Increment, 7)))
            .await
            .unwrap()
            .output,
        8
    );
    assert!(matches!(
        service
            .clone()
            .oneshot(Request::new(context.clone(), (Action::Increment, 0)))
            .await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            Rejected(0)
        )))
    ));
    assert!(matches!(
        service
            .oneshot(Request::new(context, (Action::Missing, 7)))
            .await,
        Err(tok_execution::Error::Inner(DispatchError::Unknown(
            Action::Missing
        )))
    ));
}

#[tokio::test]
async fn service_clones_share_non_clone_selector_and_stop_before_selection_or_unready_dispatch() {
    use tower::ServiceExt as _;
    let polls = Arc::new(AtomicUsize::new(0));
    let selections = Arc::new(std::sync::Mutex::new(0));
    let observed = Arc::clone(&selections);
    let non_clone = std::sync::Mutex::new(Action::Increment);
    let mut registry = Table::new();
    registry
        .register_service(Action::Increment, NeverReady(Arc::clone(&polls)))
        .unwrap();
    let service = registry.into_service(move |_| {
        *observed.lock().unwrap() += 1;
        non_clone.lock().unwrap().clone()
    });
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(matches!(
        service
            .clone()
            .oneshot(Request::new(context(Duration::from_secs(1), stop), ()))
            .await,
        Err(tok_execution::Error::Stopped(StopReason::Cancelled))
    ));
    assert_eq!(*selections.lock().unwrap(), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service
            .oneshot(Request::new(
                context(Duration::from_millis(20), CancellationToken::new()),
                ()
            ))
            .await,
        Err(tok_execution::Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert_eq!(*selections.lock().unwrap(), 1);
    assert!(polls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn slow_key_selection_cannot_dispatch_or_return_a_late_unknown_result() {
    use tower::ServiceExt as _;
    let calls = Arc::new(AtomicUsize::new(0));
    for selected in [Action::Increment, Action::Missing] {
        let mut registry = Table::<Action, (), (), Rejected>::new();
        let calls = Arc::clone(&calls);
        registry
            .register(Action::Increment, move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(Response::new(())) }
            })
            .unwrap();
        let service = registry.into_service(move |_| {
            std::thread::sleep(Duration::from_millis(30));
            selected.clone()
        });
        assert!(matches!(
            service
                .oneshot(Request::new(
                    context(Duration::from_millis(5), CancellationToken::new()),
                    ()
                ))
                .await,
            Err(tok_execution::Error::Stopped(StopReason::DeadlineExceeded))
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
