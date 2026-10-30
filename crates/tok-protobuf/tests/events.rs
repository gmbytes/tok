//! Protobuf 正文与通用事件注册、分发和投递策略的集成。
#![cfg(feature = "events")]

use std::{sync::Arc, time::Duration};
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, Outcome, Registry};
use tok_protobuf::events::RegistryExt as _;

#[derive(Clone, PartialEq, prost::Message)]
struct Changed {
    #[prost(uint64, tag = "1")]
    value: u64,
}
impl Event for Changed {
    const NAME: &'static str = "protobuf.changed";
}
#[derive(Clone, PartialEq, prost::Message)]
struct Persistent {}
impl Event for Persistent {
    const NAME: &'static str = "protobuf.persistent";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
}
#[derive(Clone, PartialEq, prost::Message)]
struct Large {
    #[prost(bytes, tag = "1")]
    body: Vec<u8>,
}
impl Event for Large {
    const NAME: &'static str = "protobuf.large";
}

#[test]
fn protobuf_rejects_oversized_body_before_encoding() {
    let event = Envelope::new(
        EventId::generate(),
        Large {
            body: vec![0; 1_048_576],
        },
    )
    .unwrap();
    assert!(matches!(
        tok_protobuf::events::encode(&event),
        Err(tok_events::Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn protobuf_events_use_common_registry_and_preserve_delivery_policy() {
    let event = Envelope::new(EventId::generate(), Changed { value: 42 }).unwrap();
    let message = tok_protobuf::events::immediate(&event).unwrap();
    let mut registry = Registry::<u64>::new(1).unwrap();
    registry
        .register::<Changed, _>(|request| {
            Box::pin(async move {
                *request.input.state = request.input.event.payload.value;
                Ok(tok_execution::Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    registry.enable_protobuf::<Changed>().unwrap();
    let context = tok_execution::Context::new(
        tok_core::Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        "test",
        Duration::from_secs(1),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    let mut value = 0;
    registry
        .receive(
            &mut value,
            tok_execution::Request::new(context, Arc::new(message)),
        )
        .await
        .unwrap();
    assert_eq!(value, 42);

    let event = Envelope::new(EventId::generate(), Persistent {}).unwrap();
    assert!(matches!(
        tok_protobuf::events::immediate(&event),
        Err(tok_events::Error::Unsupported(_))
    ));
    assert_eq!(
        tok_protobuf::events::publication(&event).unwrap().delivery,
        DeliveryPolicy::Persistent
    );
}
