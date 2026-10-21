//! 同一事件注册链的本地借用和远程解码适配，不需要启动 NATS。
use std::{sync::Arc, time::Duration};
use tok::{
    core::service::Identity,
    events::{Envelope, Event, EventId, Message, Outcome, Registry},
    service::{Context, Request, Source},
};
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, prost::Message)]
struct Added {
    #[prost(int64, tag = "1")]
    amount: i64,
}
impl Event for Added {
    const NAME: &'static str = "demo.added.v1";
}
fn context() -> Result<Context, tok::core::Error> {
    Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "counter".into(),
        },
        "counter",
        Source::Rpc { request_id: 1 },
        Duration::from_secs(1),
        CancellationToken::new(),
    )
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut events = Registry::<i64, Outcome, std::io::Error>::new(4)?;
    events.register::<Added, _>(|request| {
        Box::pin(async move {
            tokio::task::yield_now().await;
            *request.input.state += request.input.event.payload.amount;
            assert_eq!(request.context.operation(), "event/demo.added.v1");
            Ok(Outcome::Handled)
        })
    })?;
    events.enable_protobuf::<Added>()?;
    let event = Envelope::new(EventId::new("same-event")?, Added { amount: 3 })?;
    let mut state = 0;
    events
        .dispatch(&mut state, Request::new(context()?, &event))
        .await?;
    // 仅传输适配需要编码，本地分发不会执行这一步。
    let encoded = Arc::new(Message::new(&event)?);
    events
        .receive(&mut state, Request::new(context()?, encoded))
        .await?;
    assert_eq!(state, 6);
    println!("本地与远程解码共用事件注册链验证通过");
    Ok(())
}
