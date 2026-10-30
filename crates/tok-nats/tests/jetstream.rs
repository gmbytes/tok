//! 真实 JetStream 的持久发布、离线补收及失败重投。
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, Outcome, TopicFilter, service};
use tok_execution::{Request, Response};
use tok_lifecycle::{Builder, Init, InitFuture, task::Task};
use tok_nats::{self as nats, jetstream};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct Saved {
    #[prost(uint32, tag = "1")]
    value: u32,
}
impl Event for Saved {
    const NAME: &'static str = "test.saved";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
}
#[derive(Clone, PartialEq, prost::Message)]
struct Transient {}
impl Event for Transient {
    const NAME: &'static str = "test.transient";
}

#[tokio::test]
#[ignore = "需要启用 JetStream 的真实 NATS"]
async fn persistent_publication_without_stream_fails_without_downgrade() {
    let url =
        std::env::var("TOK_TEST_JETSTREAM_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".into());
    let namespace = format!("missing_{}", uuid::Uuid::new_v4().simple());
    let bus = nats::Bus::connect(nats::Config {
        url,
        namespace,
        ..Default::default()
    })
    .await
    .unwrap();
    let event = Envelope::new(EventId::generate(), Saved { value: 7 }).unwrap();
    assert!(matches!(
        bus.publish_event(event).await,
        Err(tok_events::Error::Unavailable(_))
    ));
    bus.close().await.unwrap();
}

#[tokio::test]
#[ignore = "需要启用 JetStream 的真实 NATS"]
async fn persistent_events_survive_offline_and_retry_failed_processing() {
    let url =
        std::env::var("TOK_TEST_JETSTREAM_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".into());
    let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
    let stream_name = format!("S{}", uuid::Uuid::new_v4().simple());
    let client = async_nats::connect(&url).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let mut core = client
        .subscribe(format!("tok.events.{namespace}.>"))
        .await
        .unwrap();
    client.flush().await.unwrap();
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("tok.persistent.{namespace}.>")],
        storage: async_nats::jetstream::stream::StorageType::File,
        max_messages: 100,
        ..Default::default()
    })
    .await
    .unwrap();
    let bus = nats::Bus::connect(nats::Config {
        url,
        namespace,
        handler_timeout_ms: 500,
        ..Default::default()
    })
    .await
    .unwrap();
    let event = Envelope::new(EventId::generate(), Saved { value: 7 }).unwrap();
    let context = tok_execution::Context::new(
        tok_core::Identity {
            node_id: "producer".into(),
            service_name: "publisher".into(),
        },
        "publish",
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .unwrap();
    tok_events::Publisher::publish(
        &bus,
        Request::new(context, tok_events::Publication::protobuf(&event).unwrap()),
    )
    .await
    .unwrap();
    bus.publish_event(Envelope::new(EventId::generate(), Transient {}).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), core.next())
        .await
        .unwrap()
        .unwrap();
    let mut stored = js.get_stream(&stream_name).await.unwrap();
    assert_eq!(stored.info().await.unwrap().state.messages, 1);
    let backend = jetstream::Consumer::new(
        bus.clone(),
        jetstream::Config {
            stream: stream_name.clone(),
            consumer: "worker".into(),
            ack_wait: Duration::from_secs(2),
            max_deliver: 4,
            retry_delay: Duration::from_millis(20),
        },
    )
    .unwrap();
    let conflicting_backend = backend.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
    let factory = Arc::new({
        let count = Arc::clone(&count);
        move |init: Init| -> InitFuture {
            let backend = backend.clone();
            let count = Arc::clone(&count);
            let seen = seen.clone();
            Box::pin(async move {
                let chain = tower::service_fn(move |_: Request<Arc<tok_events::Message>>| {
                    let attempt = count.fetch_add(1, Ordering::SeqCst);
                    let seen = seen.clone();
                    async move {
                        seen.send(attempt).unwrap();
                        if attempt == 0 {
                            return Err(tok_events::Error::Unavailable("业务失败"));
                        }
                        if attempt == 2 {
                            tokio::time::sleep(Duration::from_millis(700)).await;
                        }
                        Ok(Response::new(Outcome::Handled))
                    }
                });
                service::install(
                    &init,
                    &backend,
                    service::Config {
                        name: "persistent".into(),
                        event_types: vec![Saved::NAME.into()],
                        topics: TopicFilter::All,
                        group: Some("worker".into()),
                        extensions: Default::default(),
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
    let identity = tok_core::Identity {
        node_id: "node".into(),
        service_name: "service".into(),
    };
    let running = Builder::new(identity.clone(), 2, CancellationToken::new())
        .unwrap()
        .build(
            Arc::clone(&factory) as Arc<dyn tok_lifecycle::Factory>,
            Default::default(),
            Duration::from_secs(3),
        )
        .await
        .unwrap()
        .start();
    let handle = running.handle();
    let run = tokio::spawn(running.run(Duration::from_secs(2)));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap(),
        Some(1)
    );
    handle.request_stop();
    run.await.unwrap().unwrap();
    let conflict_builder = Builder::new(identity.clone(), 1, CancellationToken::new()).unwrap();
    let conflict_init = Init {
        identity: identity.clone(),
        service: conflict_builder.handle(),
        extensions: Default::default(),
    };
    let conflict_chain = tower::service_fn(|_: Request<Arc<tok_events::Message>>| async {
        Ok::<_, tok_events::Error>(Response::new(Outcome::Handled))
    });
    let conflict = service::install(
        &conflict_init,
        &conflicting_backend,
        service::Config {
            name: "conflict".into(),
            event_types: vec!["test.other".into()],
            topics: TopicFilter::All,
            group: Some("worker".into()),
            extensions: Default::default(),
            timeout: Duration::from_secs(2),
        },
        conflict_chain,
    )
    .await;
    assert!(matches!(
        conflict,
        Err(service::Error::Event(tok_events::Error::InvalidInput(_)))
    ));
    let running = Builder::new(identity, 2, CancellationToken::new())
        .unwrap()
        .build(factory, Default::default(), Duration::from_secs(3))
        .await
        .unwrap()
        .start();
    let handle = running.handle();
    let run = tokio::spawn(running.run(Duration::from_secs(2)));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), received.recv())
            .await
            .is_err()
    );
    bus.publish_event(Envelope::new(EventId::generate(), Saved { value: 8 }).unwrap())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap(),
        Some(3)
    );
    handle.request_stop();
    run.await.unwrap().unwrap();
    bus.close().await.unwrap();
    js.delete_stream(stream_name).await.unwrap();
}
