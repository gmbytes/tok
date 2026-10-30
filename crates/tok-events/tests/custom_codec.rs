//! 不依赖 protobuf 的自定义远程事件正文解码。
use std::{sync::Arc, time::Duration};
use tok_events::{Event, EventId, Outcome, Registry};
use tok_execution::{Context, Request, Response};
use tokio_util::sync::CancellationToken;

struct Changed(u8);
impl Event for Changed {
    const NAME: &'static str = "test.custom";
}
struct ByteCodec;
impl tok_core::codec::Encode<Changed> for ByteCodec {
    type Error = std::convert::Infallible;
    fn encode(&self, value: &Changed) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![value.0])
    }
}
impl tok_core::codec::Decode<Changed> for ByteCodec {
    type Error = tok_events::Error;
    fn decode(&self, body: &[u8]) -> Result<Changed, Self::Error> {
        match body {
            [value] => Ok(Changed(*value)),
            _ => Err(tok_events::Error::InvalidInput("正文无效")),
        }
    }
}

#[tokio::test]
async fn custom_decoder_drives_the_same_event_registry() {
    let mut registry = Registry::<()>::new(1).unwrap();
    registry
        .register::<Changed, _>(|request| {
            Box::pin(async move {
                assert_eq!(request.input.event.payload.0, 7);
                Ok(Response::new(Outcome::Handled))
            })
        })
        .unwrap();
    registry.enable_codec::<Changed, _>(ByteCodec).unwrap();
    let envelope = tok_events::Envelope::new(EventId::generate(), Changed(7)).unwrap();
    let message = Arc::new(
        tok_events::Publication::with_codec(&envelope, &ByteCodec)
            .unwrap()
            .message,
    );
    let context = Context::new(
        tok_core::Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        "consume",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let result = registry
        .receive(&mut (), Request::new(context, message))
        .await
        .unwrap();
    assert_eq!(result.output, vec![Outcome::Handled]);

    let bad = Arc::new(tok_events::Message {
        topic: Default::default(),
        event_id: EventId::generate(),
        event_type: Changed::NAME.into(),
        body: vec![],
        trace_context: Default::default(),
    });
    let context = Context::new(
        tok_core::Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        "consume",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let error = registry
        .receive(&mut (), Request::new(context, bad))
        .await
        .unwrap_err();
    match error {
        tok_events::DispatchError::Decode(source) => {
            assert!(source.downcast_ref::<tok_events::Error>().is_some())
        }
        _ => panic!("应保留具体解码错误"),
    }
}
