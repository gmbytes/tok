//! 不依赖 protobuf 的自定义远程事件正文解码。
use std::{sync::Arc, time::Duration};
use tok_events::{Event, EventId, Message, Outcome, Registry};
use tok_execution::{Context, Request, Response};
use tokio_util::sync::CancellationToken;

struct Changed(u8);
impl Event for Changed {
    const NAME: &'static str = "test.custom";
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
    registry
        .enable_decoder::<Changed>(|body| match body {
            [value] => Ok(Changed(*value)),
            _ => Err(tok_events::Error::InvalidInput("正文无效")),
        })
        .unwrap();
    let message = Arc::new(Message {
        topic: Default::default(),
        event_id: EventId::generate(),
        event_type: Changed::NAME.into(),
        body: vec![7],
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
    let result = registry
        .receive(&mut (), Request::new(context, message))
        .await
        .unwrap();
    assert_eq!(result.output, vec![Outcome::Handled]);
}
