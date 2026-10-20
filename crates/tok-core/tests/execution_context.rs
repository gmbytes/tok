//! 通用上下文：派生期限、取消范围与类型化能力隔离。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{
    execution::{Context, Source, StopReason},
    service::Identity,
};
use tokio_util::sync::CancellationToken;

fn context(cancellation: CancellationToken) -> Context {
    Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "ordinary".into(),
        },
        "ordinary.call",
        Source::Rpc { request_id: 7 },
        Duration::from_secs(1),
        cancellation,
    )
    .unwrap()
}
#[test]
fn child_preserves_budget_and_typed_extensions_without_cancelling_parent() {
    let cancellation = CancellationToken::new();
    let mut parent = context(cancellation.clone());
    #[derive(Debug, PartialEq, Eq)]
    struct EntityId(u64);
    parent.extensions_mut().insert(EntityId(7));
    parent
        .extensions_mut()
        .insert(Arc::new(AtomicUsize::new(0)));
    parent.extensions_mut().insert(String::from("parent"));
    let mut child = parent
        .child(
            "ordinary.changed",
            Source::Event {
                event_id: "event:1".into(),
            },
        )
        .unwrap();
    assert_eq!(child.deadline(), parent.deadline());
    assert_eq!(child.service(), parent.service());
    assert_eq!(child.extensions().get::<EntityId>(), Some(&EntityId(7)));
    assert!(matches!(child.source(), Source::Event { event_id } if event_id == "event:1"));
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
    child.cancellation().cancel();
    assert_eq!(child.check(), Err(StopReason::Cancelled));
    assert!(parent.check().is_ok());
    let another = parent
        .child("another", Source::Rpc { request_id: 8 })
        .unwrap();
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
        Source::Rpc { request_id: 1 },
        Duration::ZERO,
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(expired.check(), Err(StopReason::DeadlineExceeded));
    assert!(expired.child("", Source::Rpc { request_id: 1 }).is_err());
    assert!(
        Context::new(
            Identity {
                node_id: String::new(),
                service_name: "ordinary".into()
            },
            "invalid",
            Source::Rpc { request_id: 1 },
            Duration::from_secs(1),
            CancellationToken::new()
        )
        .is_err()
    );
}
