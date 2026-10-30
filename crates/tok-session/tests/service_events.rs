//! 同一 logic 服务的服务级事件与 session 事件各自建立执行范围。
#[path = "../../../tests/support_codec/codec.rs"]
mod test_codec;
#[path = "../../../tests/support_codec/events.rs"]
mod test_events;
#[path = "../../../tests/support_codec/nats.rs"]
mod test_nats;
#[path = "../../../tests/support_codec/session.rs"]
mod test_session;
use prost::Message as _;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use test_nats::PublisherExt as _;
use test_session::EventRegistryExt as _;
use tok_events::{Event, EventId, Message, Outcome};
use tok_execution::{Request, Response};
use tok_lifecycle::{Builder, Init, InitFuture, task::Task};
use tok_session::logic::rpc::{self, protocol::internal::v1 as proto};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct Changed {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Changed {
    const NAME: &'static str = "test.service.changed";
}
#[derive(Clone)]
struct ServiceCapability;

#[tokio::test]
#[ignore = "需要真实 NATS"]
async fn logic_service_consumes_configuration_and_session_events_in_distinct_scopes() {
    use proto::logic_rpc_server::LogicRpc;
    let bus = Arc::new(
        tok_nats::Bus::connect(tok_nats::Config {
            url: std::env::var("TOK_TEST_NATS_URL")
                .unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
            namespace: format!("test-{}", EventId::generate()),
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let (endpoint, binding) = rpc::Service::deferred("node", "logic").unwrap();
    let configs = Arc::new(AtomicUsize::new(0));
    let sessions = Arc::new(AtomicUsize::new(0));
    let (seen, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let factory = Arc::new({
        let bus = Arc::clone(&bus);
        let configs = Arc::clone(&configs);
        let sessions = Arc::clone(&sessions);
        move |init: Init| -> InitFuture {
            let bus = Arc::clone(&bus);
            let binding = binding.clone();
            let configs = Arc::clone(&configs);
            let sessions = Arc::clone(&sessions);
            let seen = seen.clone();
            Box::pin(async move {
                let mut logic = rpc::HandlerRegistry::new();
                logic.register_event::<Changed, _>(move |request| {
                    let sessions = Arc::clone(&sessions);
                    Box::pin(async move {
                        assert_eq!(
                            request.input.state.session.as_ref().unwrap().subject_id,
                            "subject"
                        );
                        assert!(request.extensions().get::<ServiceCapability>().is_none());
                        sessions.fetch_add(1, Ordering::SeqCst);
                        Ok(Response::new(None))
                    })
                })?;
                binding.install(&init, logic.into_service())?;
                let chain = tower::service_fn(move |request: Request<Arc<Message>>| {
                    let configs = Arc::clone(&configs);
                    let seen = seen.clone();
                    async move {
                        assert_eq!(request.context.service().service_name, "logic");
                        assert!(request.extensions().get::<rpc::CallContext>().is_none());
                        assert!(request.extensions().get::<ServiceCapability>().is_some());
                        configs.fetch_add(1, Ordering::SeqCst);
                        seen.send(()).unwrap();
                        Ok::<_, tok_events::Error>(Response::new(Outcome::Handled))
                    }
                });
                let mut extensions = tok_core::Extensions::default();
                extensions.insert(ServiceCapability);
                tok_events::service::install(
                    &init,
                    bus.as_ref(),
                    tok_events::service::Config {
                        name: "configuration".into(),
                        event_types: vec![Changed::NAME.into()],
                        topics: tok_events::TopicFilter::All,
                        group: None,
                        extensions,
                        timeout: Duration::from_secs(2),
                    },
                    chain,
                )
                .await?;
                let stop = init.service.cancellation();
                Ok(Box::pin(async move {
                    stop.cancelled().await;
                    Ok(())
                }) as Task)
            })
        }
    });
    let identity = tok_core::service::Identity {
        node_id: "node".into(),
        service_name: "logic".into(),
    };
    let prepared = Builder::new(identity, 2, CancellationToken::new())
        .unwrap()
        .build(factory, Default::default(), Duration::from_secs(3))
        .await
        .unwrap();
    let running = prepared.start();
    let handle = running.handle();
    let run = tokio::spawn(running.run(Duration::from_secs(2)));
    let event = tok_events::Envelope::new(EventId::generate(), Changed { value: 7 }).unwrap();
    bus.publish_event(event).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), observed.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sessions.load(Ordering::SeqCst), 0);
    let result = endpoint
        .handle_event(tonic::Request::new(proto::EventRequest {
            context: Some(proto::CallContext {
                node_id: "node".into(),
                service_name: "logic".into(),
                bid: Some("subject".into()),
                request_id: 1,
                session: Some(proto::SessionIdentity {
                    subject_id: "subject".into(),
                    session_id: "s".into(),
                    epoch: 1,
                }),
            }),
            event_id: EventId::generate().to_string(),
            event_type: Changed::NAME.into(),
            body: Changed { value: 7 }.encode_to_vec(),
            topic: None,
        }))
        .await
        .unwrap();
    assert!(result.into_inner().error.is_none());
    assert_eq!(configs.load(Ordering::SeqCst), 1);
    assert_eq!(sessions.load(Ordering::SeqCst), 1);
    handle.request_stop();
    run.await.unwrap().unwrap();
}
