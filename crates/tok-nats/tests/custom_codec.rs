//! 关闭 protobuf 正文适配后通过 NATS 使用自定义事件正文。
use std::{sync::Arc, time::Duration};
use tok_core::codec::{Decode, Encode};
use tok_events::{
    Envelope, Event, EventId, Outcome, Publication, Publisher, Registry, Subscriber, TopicFilter,
};
use tok_execution::{Context, Request, Response};
use tokio_util::sync::CancellationToken;

struct Changed(u8);
impl Event for Changed {
    const NAME: &'static str = "test.custom.codec";
}
struct ByteCodec;
impl Encode<Changed> for ByteCodec {
    type Error = std::convert::Infallible;
    fn encode(&self, value: &Changed) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![value.0])
    }
}
impl Decode<Changed> for ByteCodec {
    type Error = tok_events::Error;
    fn decode(&self, body: &[u8]) -> Result<Changed, Self::Error> {
        match body {
            [value] => Ok(Changed(*value)),
            _ => Err(tok_events::Error::InvalidInput("正文无效")),
        }
    }
}

#[tokio::test]
#[ignore = "需要真实 NATS"]
async fn custom_body_codec_works_across_nats_without_protobuf_adapter() {
    let bus = tok_nats::Bus::connect(tok_nats::Config {
        url: std::env::var("TOK_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        namespace: format!("codec-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
    let mut registry = Registry::new(1).unwrap();
    registry
        .register::<Changed, _>(move |request| {
            let value = request.input.event.payload.0;
            let seen = seen.clone();
            Box::pin(async move {
                seen.send(value).unwrap();
                Ok(Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    registry.enable_codec::<Changed, _>(ByteCodec).unwrap();
    let identity = tok_core::Identity {
        node_id: "node".into(),
        service_name: "service".into(),
    };
    let subscriber =
        Subscriber::new(identity.clone(), Arc::new(registry), TopicFilter::All, None).unwrap();
    let subscription = bus.subscribe(subscriber).await.unwrap();
    let stop = CancellationToken::new();
    let consumer = tokio::spawn(subscription.run(stop.clone()));
    let context = Context::new(
        identity,
        "publish",
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .unwrap();
    let event = Envelope::new(EventId::generate(), Changed(7)).unwrap();
    Publisher::publish(
        &bus,
        Request::new(
            context,
            Publication::with_codec(&event, &ByteCodec).unwrap(),
        ),
    )
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
