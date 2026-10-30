//! NATS 与 protobuf 正文适配的真实组合。
#![cfg(feature = "nats")]
use std::{sync::Arc, time::Duration};
use tok_events::{Envelope, Event, EventId, Outcome, Registry, Subscriber, TopicFilter};
use tok_execution::Response;
use tok_protobuf::{events::RegistryExt as _, nats::PublisherExt as _};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    number: u32,
}
impl Event for Value {
    const NAME: &'static str = "test.protobuf.nats";
}

#[tokio::test]
#[ignore = "需要真实 NATS"]
async fn protobuf_publisher_and_registry_share_real_nats() {
    let bus = tok_nats::Bus::connect(tok_nats::Config {
        url: std::env::var("TOK_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        namespace: format!("adapter-{}", EventId::generate()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
    let mut registry = Registry::new(1).unwrap();
    registry
        .register::<Value, _>(move |request| {
            let value = request.input.event.payload.number;
            let seen = seen.clone();
            Box::pin(async move {
                seen.send(value).unwrap();
                Ok(Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    registry.enable_protobuf::<Value>().unwrap();
    let subscriber = Subscriber::new(
        tok_core::Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        Arc::new(registry),
        TopicFilter::All,
        None,
    )
    .unwrap();
    let subscription = bus.subscribe(subscriber).await.unwrap();
    let stop = CancellationToken::new();
    let consumer = tokio::spawn(subscription.run(stop.clone()));
    bus.publish_event(Envelope::new(EventId::generate(), Value { number: 7 }).unwrap())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap(),
        Some(7)
    );
    stop.cancel();
    consumer.await.unwrap().unwrap();
    bus.close().await.unwrap();
}
