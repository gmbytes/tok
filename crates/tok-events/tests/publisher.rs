//! 不依赖具体传输的事件发布契约。
use std::time::Duration;
mod codec_support;
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, Publication, Publisher, Task};
use tok_execution::{Context, Request, Response};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq)]
struct Saved {}
impl codec_support::Body for Saved {
    fn encode_body(&self) -> Vec<u8> {
        Vec::new()
    }
    fn decode_body(_: &[u8]) -> Result<Self, std::io::Error> {
        Ok(Self {})
    }
}
impl Event for Saved {
    const NAME: &'static str = "test.saved";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
}
#[derive(Clone)]
struct Fake;
impl Publisher for Fake {
    fn publish(&self, request: Request<Publication>) -> Task<'_, Response<()>> {
        Box::pin(async move {
            assert_eq!(request.input.delivery, DeliveryPolicy::Persistent);
            assert_eq!(request.context.service().service_name, "caller");
            assert_eq!(request.extensions().get::<u32>(), Some(&42));
            Ok(Response::new(()))
        })
    }
}
#[tokio::test]
async fn transport_independent_publication_keeps_delivery_and_context() {
    let event = Envelope::new(EventId::generate(), Saved {}).unwrap();
    let publication = Publication::with_codec(&event, &codec_support::Codec).unwrap();
    let context = Context::new(
        tok_core::Identity {
            node_id: "node".into(),
            service_name: "caller".into(),
        },
        "publish",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let mut request = Request::new(context, publication);
    request.extensions_mut().insert(42_u32);
    use tower::ServiceExt as _;
    tok_events::PublisherService::new(Fake)
        .oneshot(request)
        .await
        .unwrap();
}
