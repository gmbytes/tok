//! 通用上下文：派生期限、取消范围与类型化能力隔离。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::service::Identity;
use tok_execution::{Context, ContextError, Request, StopReason};
use tokio_util::sync::CancellationToken;

fn context(cancellation: CancellationToken) -> Context {
    Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "ordinary".into(),
        },
        "ordinary.call",
        Duration::from_secs(1),
        cancellation,
    )
    .unwrap()
}
#[test]
fn child_preserves_budget_and_typed_extensions_without_cancelling_parent() {
    let cancellation = CancellationToken::new();
    let mut parent = Request::new(context(cancellation.clone()), ());
    #[derive(Debug, PartialEq, Eq)]
    struct EntityId(u64);
    parent.extensions_mut().insert(EntityId(7));
    parent
        .extensions_mut()
        .insert(Arc::new(AtomicUsize::new(0)));
    parent.extensions_mut().insert(String::from("parent"));
    let mut child = Request::from_parts(
        parent.context.child("ordinary.changed").unwrap(),
        (),
        parent.extensions().clone(),
    );
    assert_eq!(child.context.deadline(), parent.context.deadline());
    assert_eq!(child.context.service(), parent.context.service());
    assert_eq!(child.extensions().get::<EntityId>(), Some(&EntityId(7)));
    child
        .extensions()
        .get::<Arc<AtomicUsize>>()
        .unwrap()
        .fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        parent
            .extensions()
            .get::<Arc<AtomicUsize>>()
            .unwrap()
            .load(Ordering::SeqCst),
        1
    );
    child.extensions_mut().insert(String::from("child"));
    assert_eq!(parent.extensions().get::<String>().unwrap(), "parent");
    assert_eq!(child.extensions().get::<String>().unwrap(), "child");
    child.context.cancellation().cancel();
    assert_eq!(child.context.check(), Err(StopReason::Cancelled));
    assert!(parent.context.check().is_ok());
    let another = parent.context.child("another").unwrap();
    cancellation.cancel();
    assert_eq!(another.check(), Err(StopReason::Cancelled));
}
#[test]
fn zero_budget_is_already_expired_and_invalid_metadata_is_rejected() {
    let expired = Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "ordinary".into(),
        },
        "expired",
        Duration::ZERO,
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(expired.check(), Err(StopReason::DeadlineExceeded));
    assert_eq!(
        expired.child("").unwrap_err(),
        ContextError::InvalidOperation
    );
    assert_eq!(
        Context::new(
            Identity {
                node_id: String::new(),
                service_name: "ordinary".into()
            },
            "invalid",
            Duration::from_secs(1),
            CancellationToken::new()
        )
        .unwrap_err(),
        ContextError::InvalidService
    );
}

#[test]
fn request_mapping_and_explicit_stop_boundary_preserve_deadline_and_extensions() {
    let stop = CancellationToken::new();
    let mut original = Request::new(context(stop.clone()), 1);
    original.extensions_mut().insert(String::from("marker"));
    let deadline = original.context.deadline();
    let request = original.map(|value| value.to_string());
    let narrowed = request.context.clone().with_budget_limit(Duration::MAX);
    assert_eq!(narrowed.deadline(), deadline);
    let independent_stop = CancellationToken::new();
    let narrowed = narrowed
        .with_deadline_limit(deadline)
        .with_budget_limit(Duration::from_millis(10))
        .with_cancellation(independent_stop.clone());
    assert!(narrowed.deadline() < deadline);
    assert_eq!(request.extensions().get::<String>().unwrap(), "marker");
    stop.cancel();
    assert!(narrowed.check().is_ok());
    independent_stop.cancel();
    assert_eq!(narrowed.check(), Err(StopReason::Cancelled));
    assert_eq!(request.input, "1");
}
