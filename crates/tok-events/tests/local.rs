//! 本地与远程共用注册表；直接借用、顺序、预算和错误边界。
mod support;
use std::sync::Arc;
use tok_events::{DispatchError, Envelope, Error, Event, EventId, Input, Registry};
use tok_service::{Request, sync_handler};
struct Changed(i32);
impl Event for Changed {
    const NAME: &'static str = "test.changed.v1";
}
struct Other;
impl Event for Other {
    const NAME: &'static str = "test.other.v1";
}
struct Collision;
impl Event for Collision {
    const NAME: &'static str = Changed::NAME;
}
struct Invalid;
impl Event for Invalid {
    const NAME: &'static str = "test..invalid";
}
fn changed(value: i32) -> Envelope<Changed> {
    Envelope::new(EventId::generate(), Changed(value)).unwrap()
}

#[tokio::test]
async fn async_handlers_borrow_current_state_in_order_and_support_multiple_types() {
    let mut registry = Registry::<Vec<i32>, (), Error>::new(3).unwrap();
    registry
        .register::<Changed, _>(|request| {
            Box::pin(async move {
                request.input.state.push(request.input.event.payload.0);
                tokio::task::yield_now().await;
                request.input.state.push(2);
                Ok(())
            })
        })
        .unwrap();
    registry
        .register_service::<Changed, _>(sync_handler(
            |request: Request<Input<'_, Vec<i32>, Changed>>| {
                request.input.state.push(request.input.state[0] + 1);
                Ok(())
            },
        ))
        .unwrap();
    registry
        .register::<Other, _>(|request| {
            Box::pin(async move {
                request.input.state.push(9);
                Ok(())
            })
        })
        .unwrap();
    let mut state = vec![];
    assert_eq!(
        registry
            .dispatch(&mut state, support::request(&changed(7)))
            .await
            .unwrap()
            .len(),
        2
    );
    let other = Envelope::new(EventId::generate(), Other).unwrap();
    registry
        .dispatch(&mut state, support::request(&other))
        .await
        .unwrap();
    assert_eq!(state, [7, 2, 8, 9]);
}
#[tokio::test]
async fn first_error_preserves_original_identity_and_keeps_prior_changes() {
    let original = Arc::new(String::from("业务失败"));
    let mut registry = Registry::<Vec<i32>, (), Arc<String>>::new(3).unwrap();
    registry
        .register::<Changed, _>(|request| {
            Box::pin(async move {
                request.input.state.push(request.input.event.payload.0);
                Ok(())
            })
        })
        .unwrap();
    let failure = Arc::clone(&original);
    registry
        .register::<Changed, _>(move |_| {
            let failure = Arc::clone(&failure);
            Box::pin(async { Err(failure) })
        })
        .unwrap();
    registry
        .register::<Changed, _>(|_| panic!("首错后不得执行"))
        .unwrap();
    let mut state = vec![];
    let error = registry
        .dispatch(&mut state, support::request(&changed(1)))
        .await
        .unwrap_err();
    assert!(matches!(error,DispatchError::Handler(returned) if Arc::ptr_eq(&returned,&original)));
    assert_eq!(state, [1]);
    assert!(
        registry
            .dispatch(&mut state, support::request(&changed(1)))
            .await
            .is_err()
    );
    assert_eq!(state, [1, 1]);
}
#[tokio::test]
async fn registration_bounds_type_collisions_and_empty_dispatch_are_explicit() {
    assert!(matches!(
        Registry::<(), (), Error>::new(0),
        Err(Error::InvalidCapacity)
    ));
    let mut registry = Registry::<(), (), Error>::new(1).unwrap();
    assert!(
        registry
            .register::<Invalid, _>(|_| Box::pin(async { Ok(()) }))
            .is_err()
    );
    registry
        .register::<Changed, _>(|_| Box::pin(async { Ok(()) }))
        .unwrap();
    assert!(matches!(
        registry.register::<Changed, _>(|_| Box::pin(async { Ok(()) })),
        Err(Error::CapacityExceeded)
    ));
    assert!(matches!(
        registry.register::<Collision, _>(|_| Box::pin(async { Ok(()) })),
        Err(Error::InvalidInput(_))
    ));
    let other = Envelope::new(EventId::generate(), Other).unwrap();
    assert!(
        registry
            .dispatch(&mut (), support::request(&other))
            .await
            .unwrap()
            .is_empty()
    );
    let collision = Envelope::new(EventId::generate(), Collision).unwrap();
    assert!(matches!(
        registry
            .dispatch(&mut (), support::request(&collision))
            .await,
        Err(DispatchError::Invalid(_))
    ));
}
#[tokio::test]
async fn cancellation_and_deadline_are_not_renewed_for_nested_events() {
    let mut registry = Registry::<(), (), Error>::new(1).unwrap();
    registry
        .register::<Changed, _>(|request| {
            Box::pin(async move {
                request.context.check().unwrap();
                std::future::pending().await
            })
        })
        .unwrap();
    let context = support::context();
    let deadline = context.deadline();
    let cancel = context.cancellation();
    cancel.cancel();
    assert!(matches!(
        registry
            .dispatch(&mut (), Request::new(context, &changed(1)))
            .await,
        Err(DispatchError::Stopped(
            tok_core::execution::StopReason::Cancelled
        ))
    ));
    let context = tok_core::execution::Context::new(
        tok_core::service::Identity {
            node_id: "n".into(),
            service_name: "s".into(),
        },
        "parent",
        tok_core::execution::Source::Rpc { request_id: 1 },
        std::time::Duration::ZERO,
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    assert!(context.deadline() <= deadline);
    assert!(matches!(
        registry
            .dispatch(&mut (), Request::new(context, &changed(1)))
            .await,
        Err(DispatchError::Stopped(
            tok_core::execution::StopReason::DeadlineExceeded
        ))
    ));
}
#[tokio::test]
async fn local_execution_stays_on_callers_thread_and_carries_capabilities() {
    let thread = std::thread::current().id();
    let mut registry = Registry::<usize, (), Error>::new(1).unwrap();
    registry
        .register::<Changed, _>(move |request| {
            Box::pin(async move {
                assert_eq!(std::thread::current().id(), thread);
                tokio::task::yield_now().await;
                assert_eq!(std::thread::current().id(), thread);
                assert_eq!(request.context.extensions().get::<u32>(), Some(&42));
                *request.input.state += 1;
                Ok(())
            })
        })
        .unwrap();
    let mut context = support::context();
    context.extensions_mut().insert(42u32);
    let mut state = 0;
    registry
        .dispatch(&mut state, Request::new(context, &changed(1)))
        .await
        .unwrap();
    assert_eq!(state, 1);
}
