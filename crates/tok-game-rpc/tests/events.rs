//! 独立事件 RPC：来源隔离、身份校验、稳定 ID 与 timer 更新。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::ServiceDescriptor;
use tok_events::{Event, EventId};
use tok_game_rpc::CallContext;
use tok_game_rpc::{
    HandlerRegistry, RpcClients, RpcEndpoint, RpcService, protocol::internal::v1::Payload,
};
use tok_player::{SessionIdentity, TimerUpdate};
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, prost::Message)]
struct Update {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Update {
    const NAME: &'static str = "test.update.v1";
    fn topic(&self) -> Result<tok_events::Topic, tok_events::Error> {
        tok_events::Topic::named("realm:42")
    }
}
#[tokio::test]
async fn event_handlers_are_inaccessible_to_requests_and_timers_and_require_session_identity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let mut registry = HandlerRegistry::new();
    registry
        .register_event::<Update, _>(move |request| {
            // RPC 关联信息与事件元信息可同时存在，不再互斥选择来源。
            assert_eq!(
                request.context.extensions().get::<CallContext>(),
                Some(&*request.input.state)
            );
            assert_eq!(
                request.context.extensions().get::<EventId>(),
                Some(&request.input.event.event_id)
            );
            assert_eq!(
                request.context.extensions().get::<tok_events::Topic>(),
                Some(&request.input.event.topic)
            );
            let original = request
                .context
                .extensions()
                .get::<Arc<tok_events::Message>>()
                .unwrap();
            assert_eq!(original.event_id, request.input.event.event_id);
            let context = request.input.state;
            let id = &request.input.event.event_id;
            assert_eq!(request.input.event.topic.as_str(), Some("realm:42"));
            let event = &request.input.event.payload;
            let seen = Arc::clone(&seen);
            Box::pin(async move {
                assert_eq!(context.session.as_ref().unwrap().player_id, "42");
                assert_eq!(id.as_str(), "operation-1");
                assert_eq!(event.value, 7);
                assert!(context.timer.is_none());
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Some(TimerUpdate::ReplaceAll(vec![])))
            })
        })
        .unwrap();
    registry
        .register_event::<Update, _>(|_| Box::pin(async { Ok(None) }))
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("node", "player", Arc::new(registry))
                .unwrap()
                .into_player(),
        )
        .unwrap();
    endpoint.readiness().set("player", true);
    let stop = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, stop.clone()).unwrap());
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 2).unwrap());
    let player_client = tok_game_rpc::player::Client::new(Arc::clone(&clients));
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "player".into(),
        protocol: "grpc".into(),
        address,
    };
    let context = CallContext {
        node_id: "node".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(SessionIdentity {
            player_id: "42".into(),
            session_id: "s".into(),
            epoch: 1,
        }),
        timer: None,
    };
    use prost::Message;
    let event = tok_events::Message {
        topic: tok_events::Topic::named("realm:42").unwrap(),
        event_id: EventId::new("operation-1").unwrap(),
        event_type: Update::NAME.into(),
        body: Update { value: 7 }.encode_to_vec(),
        trace_context: Default::default(),
    };
    assert_eq!(
        player_client
            .call(
                &target,
                context.clone(),
                Payload {
                    message_id: 1,
                    body: event.body.clone()
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    let mut timer = context.clone();
    timer.timer = Some(tok_player::TimerTrigger {
        timer_id: "t".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 1,
    });
    assert_eq!(
        player_client
            .call(
                &target,
                timer,
                Payload {
                    message_id: 1,
                    body: event.body.clone()
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    let mut missing = context.clone();
    missing.session = None;
    assert_eq!(
        player_client
            .handle_event(&target, missing, &event)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut invalid = event.clone();
    invalid.body = vec![255];
    assert_eq!(
        player_client
            .handle_event(&target, context.clone(), &invalid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut unknown = event.clone();
    unknown.event_type = "test.unknown".into();
    assert_eq!(
        player_client
            .handle_event(&target, context.clone(), &unknown)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let response = player_client
        .handle_event(&target, context, &event)
        .await
        .unwrap();
    assert_eq!(response.request_id, 1);
    assert!(
        matches!(tok_game_rpc::timer::decode_update(response.timers.unwrap()).unwrap(),TimerUpdate::ReplaceAll(timers) if timers.is_empty())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn ordered_event_handlers_compose_timer_effects_without_client_payloads() {
    fn spec(id: &str) -> tok_player::TimerSpec {
        tok_player::TimerSpec {
            timer_id: id.into(),
            message_id: 1,
            body: vec![],
            due_at_ms: 1,
            interval_ms: None,
        }
    }
    let cases = vec![
        (
            vec![
                TimerUpdate::Patch {
                    upsert: vec![spec("a")],
                    cancel: vec!["b".into()],
                },
                TimerUpdate::Patch {
                    upsert: vec![spec("b")],
                    cancel: vec!["a".into()],
                },
            ],
            TimerUpdate::Patch {
                upsert: vec![spec("b")],
                cancel: vec!["a".into()],
            },
        ),
        (
            vec![
                TimerUpdate::Patch {
                    upsert: vec![spec("a")],
                    cancel: vec![],
                },
                TimerUpdate::ReplaceAll(vec![spec("c")]),
                TimerUpdate::Patch {
                    upsert: vec![spec("d")],
                    cancel: vec!["c".into()],
                },
            ],
            TimerUpdate::ReplaceAll(vec![spec("d")]),
        ),
    ];
    for (effects, expected) in cases {
        let mut registry = HandlerRegistry::new();
        for effect in effects {
            registry
                .register_event::<Update, _>(move |_| {
                    let effect = effect.clone();
                    Box::pin(async move { Ok(Some(effect)) })
                })
                .unwrap();
        }
        let id = EventId::generate();
        let event = Arc::new(
            tok_events::Message::new(
                &tok_events::Envelope::new(id.clone(), Update { value: 7 }).unwrap(),
            )
            .unwrap(),
        );
        let execution = tok_execution::Context::new(
            tok_core::service::Identity {
                node_id: "node".into(),
                service_name: "player".into(),
            },
            "event",
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .unwrap();
        let call = CallContext {
            node_id: "node".into(),
            service_name: "player".into(),
            bid: None,
            request_id: 1,
            session: None,
            timer: None,
        };
        let response = tok_game_rpc::Handler::event(
            &registry,
            tok_execution::Request::new(execution, (call, event)),
        )
        .await
        .unwrap();
        assert_eq!(
            tok_game_rpc::timer::decode_update(response.timers.unwrap()).unwrap(),
            expected
        );
    }
}
