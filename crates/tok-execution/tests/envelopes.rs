//! 输入能力与输出元信息在完整本地处理链中各自流动。
use std::{sync::Arc, time::Duration};
use tok_core::Identity;
use tok_execution::{Context, Request, Response, StopReason};
use tokio_util::sync::CancellationToken;
use tower::{Layer as _, ServiceBuilder, ServiceExt as _};

#[derive(Debug)]
struct InputMark(Arc<String>);
#[derive(Debug, PartialEq, Eq)]
struct OutputMark(u32);
#[derive(Debug, PartialEq, Eq)]
struct Denied;
fn context(stop: CancellationToken) -> Context {
    Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "envelopes".into(),
        },
        "convert",
        Duration::from_secs(1),
        stop,
    )
    .unwrap()
}

#[tokio::test]
async fn borrowed_conversion_preserves_scope_and_response_metadata_reaches_outer_policy() {
    let stop = CancellationToken::new();
    let mut value = 41;
    let mut request = Request::new(context(stop.clone()), &mut value);
    let deadline = request.context.deadline();
    let mark = Arc::new(String::from("trusted"));
    request
        .extensions_mut()
        .insert(InputMark(Arc::clone(&mark)));
    let request = request
        .map(|value| (value, 1))
        .try_map(Ok::<_, Denied>)
        .unwrap();
    let chain = ServiceBuilder::new()
        .layer(tok_execution::Layer)
        .map_response(|mut response: Response<u32>| {
            assert_eq!(
                response.extensions().get::<OutputMark>(),
                Some(&OutputMark(42))
            );
            assert!(response.extensions().get::<InputMark>().is_none());
            response
                .extensions_mut()
                .insert(String::from("outer-policy"));
            response.map(|value| value.to_string())
        })
        .service(tok_execution::sync_handler(
            move |request: Request<(&mut u32, u32)>| {
                assert_eq!(request.context.deadline(), deadline);
                assert!(Arc::ptr_eq(
                    &request.extensions().get::<InputMark>().unwrap().0,
                    &mark
                ));
                let (value, amount) = request.input;
                *value += amount;
                let mut response = Response::new(*value);
                response.extensions_mut().insert(OutputMark(*value));
                Ok::<_, Denied>(response)
            },
        ));
    let response = chain.oneshot(request).await.unwrap();
    assert_eq!(response.output, "42");
    assert_eq!(
        response.extensions().get::<String>().unwrap(),
        "outer-policy"
    );
    assert_eq!(value, 42);
    stop.cancel();
}

#[test]
fn explicit_child_scope_and_parts_keep_request_metadata_without_container_backflow() {
    let stop = CancellationToken::new();
    let mut parent = Request::new(context(stop.clone()), 1);
    parent.extensions_mut().insert(String::from("parent"));
    let mut child = Request::from_parts(
        parent.context.child("nested").unwrap(),
        2,
        parent.extensions().clone(),
    );
    assert_eq!(child.context.deadline(), parent.context.deadline());
    child.extensions_mut().insert(String::from("child"));
    assert_eq!(parent.extensions().get::<String>().unwrap(), "parent");
    let (context, input, extensions) = child.into_parts();
    let child = Request::from_parts(context, input, extensions);
    child.context.cancellation().cancel();
    assert!(parent.context.check().is_ok());
    assert_eq!(child.context.check(), Err(StopReason::Cancelled));
    let cloned = parent.clone();
    stop.cancel();
    assert_eq!(cloned.context.check(), Err(StopReason::Cancelled));
}

#[tokio::test]
async fn fallible_output_conversion_preserves_metadata_and_error_branch_stays_original() {
    let mut response = Response::new("42");
    response.extensions_mut().insert(OutputMark(7));
    let response = response.try_map(str::parse::<u32>).unwrap();
    let (value, extensions) = response.into_parts();
    assert_eq!(value, 42);
    assert_eq!(extensions.get::<OutputMark>(), Some(&OutputMark(7)));
    let original = Arc::new(String::from("denied"));
    let failure = Arc::clone(&original);
    let service = tok_execution::Layer.layer(tower::service_fn(move |_: Request<()>| {
        let failure = Arc::clone(&failure);
        async move { Err::<Response<()>, _>(failure) }
    }));
    let error = service
        .oneshot(Request::new(context(CancellationToken::new()), ()))
        .await
        .unwrap_err();
    assert!(matches!(error, tok_execution::Error::Inner(error) if Arc::ptr_eq(&error, &original)));
}

// 对已采用共同错误契约的链增加外层预算保护，错误结构仍保持一层。
#[tokio::test]
async fn repeated_execution_protection_preserves_errors_and_inner_or_outer_stops() {
    use tok_execution::Error;
    let original = Arc::new(String::from("original"));
    let source = Arc::clone(&original);
    let inner = tok_execution::Layer.layer(tower::service_fn(move |_: Request<()>| {
        let source = Arc::clone(&source);
        async move { Err::<Response<()>, _>(source) }
    }));
    let service = tok_execution::Layer.layer(inner).map_err(Error::flatten);
    let error = service
        .oneshot(Request::new(context(CancellationToken::new()), ()))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Inner(error) if Arc::ptr_eq(&error, &original)));

    let inner = tower::service_fn(|_: Request<()>| async {
        Err::<Response<()>, Error<Denied>>(Error::Stopped(StopReason::Cancelled))
    });
    let service = tok_execution::Layer.layer(inner).map_err(Error::flatten);
    assert!(matches!(
        service
            .oneshot(Request::new(context(CancellationToken::new()), ()))
            .await,
        Err(Error::Stopped(StopReason::Cancelled))
    ));

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let inner = tower::service_fn(move |_: Request<()>| {
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { Ok::<_, Error<Denied>>(Response::new(())) }
    });
    let service = tok_execution::Layer.layer(inner).map_err(Error::flatten);
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(matches!(
        service.oneshot(Request::new(context(stop), ())).await,
        Err(Error::Stopped(StopReason::Cancelled))
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn mapping_concrete_error_never_maps_a_stop_reason() {
    use tok_execution::Error;
    let error: Error<Denied> = Error::Stopped(StopReason::DeadlineExceeded);
    let mapped: Error<()> = error.map_inner(|_| panic!("停止不调用具体错误映射"));
    assert!(matches!(
        mapped,
        Error::Stopped(StopReason::DeadlineExceeded)
    ));
    let error = Error::Inner(41u32).map_inner(|value| value + 1);
    assert!(matches!(error, Error::Inner(42)));
}
