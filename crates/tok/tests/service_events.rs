//! 公共服务在同一工厂组合消息 RPC 与服务级事件。
use prost::Message as _;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok::{
    events::{self, Event, EventId, Message, Outcome},
    execution::{Request, Response},
    lifecycle::{Builder, Init, InitFuture, task::Task},
    message::rpc::{self, protocol::internal::v1 as proto, tonic},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, prost::Message)]
struct ConfigChanged {
    #[prost(uint32, tag = "1")]
    version: u32,
}
impl Event for ConfigChanged {
    const NAME: &'static str = "test.config.changed";
}

#[tokio::test]
#[ignore = "需要真实 NATS"]
async fn public_service_shares_state_between_rpc_and_its_own_event_subscription() {
    let bus = Arc::new(
        tok::nats::Bus::connect(tok::nats::Config {
            url: std::env::var("TOK_TEST_NATS_URL")
                .unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
            namespace: format!("test-{}", EventId::generate()),
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let (endpoint, binding) = rpc::Service::deferred("node", "public").unwrap();
    let version = Arc::new(AtomicUsize::new(0));
    let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
    let factory = Arc::new({
        let bus = Arc::clone(&bus);
        let version = Arc::clone(&version);
        move |init: Init| -> InitFuture {
            let bus = Arc::clone(&bus);
            let binding = binding.clone();
            let version = Arc::clone(&version);
            let seen = seen.clone();
            Box::pin(async move {
                let query_version = Arc::clone(&version);
                let rpc = tower::service_fn(
                    move |request: Request<(rpc::CallContext, proto::Payload)>| {
                        let version = query_version.load(Ordering::SeqCst);
                        async move {
                            Ok::<_, tonic::Status>(Response::new(proto::CallResponse {
                                request_id: request.input.0.request_id,
                                payload: Some(proto::Payload {
                                    message_id: 2,
                                    body: version.to_string().into_bytes(),
                                }),
                                ..Default::default()
                            }))
                        }
                    },
                );
                binding.install(&init, rpc)?;
                let event = tower::service_fn(move |request: Request<Arc<Message>>| {
                    let version = Arc::clone(&version);
                    let seen = seen.clone();
                    async move {
                        let config = ConfigChanged::decode(request.input.body.as_slice())
                            .map_err(|_| events::Error::InvalidInput("配置事件解码失败"))?;
                        version.store(config.version as usize, Ordering::SeqCst);
                        seen.send(()).unwrap();
                        Ok::<_, events::Error>(Response::new(Outcome::Handled))
                    }
                });
                events::service::install(
                    &init,
                    bus.as_ref(),
                    events::service::Config {
                        name: "configuration".into(),
                        event_types: vec![ConfigChanged::NAME.into()],
                        topics: events::TopicFilter::All,
                        group: None,
                        extensions: Default::default(),
                        timeout: Duration::from_secs(2),
                    },
                    event,
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
    let identity = tok::core::service::Identity {
        node_id: "node".into(),
        service_name: "public".into(),
    };
    let running = Builder::new(identity.clone(), 2, CancellationToken::new())
        .unwrap()
        .build(factory, Default::default(), Duration::from_secs(3))
        .await
        .unwrap()
        .start();
    let handle = running.handle();
    let run = tokio::spawn(running.run(Duration::from_secs(2)));
    bus.publish_event(
        events::Envelope::new(EventId::generate(), ConfigChanged { version: 7 }).unwrap(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tok::grpc::Listener::new()
        .add_service("public", endpoint.into_business())
        .unwrap();
    server.readiness().set("public", true);
    let server_stop = CancellationToken::new();
    let serve = tokio::spawn(server.serve(listener, server_stop.clone()).unwrap());
    let target = tok::core::ServiceDescriptor {
        node_id: "node".into(),
        service_name: "public".into(),
        protocol: "grpc".into(),
        address,
    };
    let client = rpc::Client::new(Arc::new(
        tok::grpc::Client::new(Duration::from_secs(2), 2).unwrap(),
    ));
    let context = tok::execution::Context::new(
        identity,
        "query",
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .unwrap();
    let response = client
        .call(
            &target,
            Request::new(
                context,
                (
                    rpc::CallContext {
                        node_id: "node".into(),
                        service_name: "public".into(),
                        bid: None,
                        request_id: 1,
                    },
                    proto::Payload {
                        message_id: 1,
                        body: vec![],
                    },
                ),
            ),
        )
        .await
        .unwrap()
        .output;
    assert_eq!(response.payload.unwrap().body, b"7");
    handle.request_stop();
    run.await.unwrap().unwrap();
    server_stop.cancel();
    serve.await.unwrap().unwrap();
}
