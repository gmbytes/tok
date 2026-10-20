//! 通用服务订阅：类型隔离、状态捕获、异步处理和容量，不依赖网关。
mod support;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tok_events::{Envelope, Event, EventId, Handler, Message, Outcome, Registry};

#[derive(Clone, PartialEq, prost::Message)]
struct GuildChanged {
    #[prost(uint32, tag = "1")]
    amount: u32,
}
impl Event for GuildChanged {
    const NAME: &'static str = "game.guild.changed";
}
#[derive(Clone, PartialEq, prost::Message)]
struct MailSent {}
impl Event for MailSent {
    const NAME: &'static str = "game.mail.sent";
}

#[tokio::test]
async fn independent_services_subscribe_to_same_type_and_keep_their_own_state() {
    let id = EventId::generate();
    let message =
        Message::new(&Envelope::new(id.clone(), GuildChanged { amount: 3 }).unwrap()).unwrap();
    let mut services = vec![];
    for _ in 0..2 {
        let state = Arc::new(AtomicUsize::new(0));
        let mut handlers = Registry::new(1).unwrap();
        let expected = id.clone();
        let captured = Arc::clone(&state);
        handlers
            .register::<GuildChanged, _>(move |request| {
                let message = request
                    .context
                    .extensions()
                    .get::<Arc<Message>>()
                    .cloned()
                    .unwrap();
                let event = &request.input.event.payload;
                assert_eq!(message.event_id, expected);
                let state = Arc::clone(&captured);
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    state.fetch_add(event.amount as usize, Ordering::SeqCst);
                    Ok(Outcome::Handled)
                })
            })
            .unwrap();
        handlers.enable_protobuf::<GuildChanged>().unwrap();
        services.push((handlers, state));
    }
    for (handlers, state) in &services {
        handlers
            .handle(support::request(Arc::new(message.clone())))
            .await
            .unwrap();
        handlers
            .handle(support::request(Arc::new(message.clone())))
            .await
            .unwrap();
        assert_eq!(state.load(Ordering::SeqCst), 6); // 不隐式去重。
    }
}

#[tokio::test]
async fn registry_rejects_bad_known_payloads_and_ignores_unsubscribed_types() {
    assert!(Registry::<(), Outcome, tok_events::Error>::new(0).is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&calls);
    let mut handlers = Registry::new(1).unwrap();
    handlers
        .register::<GuildChanged, _>(move |_request| {
            captured.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(tok_events::Error::Unavailable("业务失败")) })
        })
        .unwrap();
    handlers.enable_protobuf::<GuildChanged>().unwrap();
    assert!(
        handlers
            .register::<GuildChanged, _>(|_| Box::pin(async { Ok(Outcome::Handled) }))
            .is_err()
    );
    assert!(matches!(
        handlers.register::<MailSent, _>(|_| Box::pin(async { Ok(Outcome::Handled) })),
        Err(tok_events::Error::CapacityExceeded)
    ));
    let unknown = Message::new(&Envelope::new(EventId::generate(), MailSent {}).unwrap()).unwrap();
    assert_eq!(
        handlers
            .handle(support::request(Arc::new(unknown)))
            .await
            .unwrap(),
        Outcome::Ignored
    );
    let mut known =
        Message::new(&Envelope::new(EventId::generate(), GuildChanged { amount: 1 }).unwrap())
            .unwrap();
    assert!(
        handlers
            .handle(support::request(Arc::new(known.clone())))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    known.body = vec![255];
    assert!(
        handlers
            .handle(support::request(Arc::new(known)))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn typed_handler_receives_the_original_shared_encoding_without_reencoding() {
    let mut encoded =
        Message::new(&Envelope::new(EventId::generate(), GuildChanged { amount: 3 }).unwrap())
            .unwrap();
    encoded.body.extend_from_slice(&[0xa0, 0x06, 0x01]);
    let original = Arc::new(encoded);
    let expected = Arc::clone(&original);
    let mut registry = Registry::new(1).unwrap();
    registry
        .register::<GuildChanged, _>(move |request| {
            let message = request
                .context
                .extensions()
                .get::<Arc<Message>>()
                .cloned()
                .unwrap();
            let event = &request.input.event.payload;
            assert!(Arc::ptr_eq(&message, &expected));
            assert_eq!(message.body, expected.body);
            assert_eq!(event.amount, 3);
            Box::pin(async { Ok(Outcome::Handled) })
        })
        .unwrap();
    registry.enable_protobuf::<GuildChanged>().unwrap();
    assert_eq!(registry.event_types(), vec![GuildChanged::NAME]);
    assert_eq!(
        registry.handle(support::request(original)).await.unwrap(),
        Outcome::Handled
    );
}

#[tokio::test]
async fn local_and_remote_execute_same_handlers_and_wire_metadata_does_not_leak_into_local_children()
 {
    let mut registry = Registry::<u32, Outcome, tok_events::Error>::new(2).unwrap();
    registry.register::<GuildChanged,_>(|request| Box::pin(async move {
        assert_eq!(request.context.deadline(),*request.context.extensions().get::<std::time::Instant>().unwrap());
        assert!(matches!(request.context.source(),tok_core::execution::Source::Event{event_id} if event_id==request.input.event.event_id.as_str()));
        if let Some(message)=request.context.extensions().get::<Arc<Message>>() {
            assert_eq!(message.event_id,request.input.event.event_id);
        }
        tokio::task::yield_now().await;
        *request.input.state+=request.input.event.payload.amount;Ok(Outcome::Handled)
    })).unwrap();
    registry
        .register::<GuildChanged, _>(|request| {
            Box::pin(async move {
                *request.input.state += 1;
                Ok(Outcome::Handled)
            })
        })
        .unwrap();
    let envelope = Envelope::new(EventId::generate(), GuildChanged { amount: 3 }).unwrap();
    let message = Arc::new(Message::new(&envelope.clone()).unwrap());
    let mut state = 0;
    let mut context = support::context();
    let deadline = context.deadline();
    context.extensions_mut().insert(deadline);
    assert!(matches!(
        registry
            .receive(
                &mut state,
                tok_service::Request::new(context.clone(), Arc::clone(&message))
            )
            .await,
        Err(tok_events::DispatchError::Unknown)
    ));
    registry.enable_protobuf::<GuildChanged>().unwrap();
    let inherited =
        Arc::new(Message::new(&Envelope::new(EventId::generate(), MailSent {}).unwrap()).unwrap());
    context.extensions_mut().insert(Arc::clone(&inherited));
    assert_eq!(
        registry
            .dispatch(
                &mut state,
                tok_service::Request::new(context.clone(), &envelope)
            )
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(Arc::ptr_eq(
        context.extensions().get::<Arc<Message>>().unwrap(),
        &inherited
    ));
    assert_eq!(
        registry
            .receive(&mut state, tok_service::Request::new(context, message))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(state, 8);
}
#[tokio::test]
async fn multiple_remote_handlers_run_in_order_and_stop_at_original_business_error() {
    let mut registry = Registry::<Vec<u32>, (), Arc<String>>::new(3).unwrap();
    registry
        .register::<GuildChanged, _>(|request| {
            Box::pin(async move {
                request.input.state.push(request.input.event.payload.amount);
                Ok(())
            })
        })
        .unwrap();
    let original = Arc::new("失败".to_string());
    let expected = Arc::clone(&original);
    registry
        .register::<GuildChanged, _>(move |_| {
            let error = Arc::clone(&original);
            Box::pin(async { Err(error) })
        })
        .unwrap();
    registry
        .register::<GuildChanged, _>(|_| panic!("首错后不能执行"))
        .unwrap();
    registry.enable_protobuf::<GuildChanged>().unwrap();
    let message = Arc::new(
        Message::new(&Envelope::new(EventId::generate(), GuildChanged { amount: 7 }).unwrap())
            .unwrap(),
    );
    let mut state = vec![];
    let result = registry
        .receive(&mut state, support::request(message))
        .await;
    assert!(
        matches!(result,Err(tok_events::DispatchError::Handler(error)) if Arc::ptr_eq(&error,&expected))
    );
    assert_eq!(state, [7]);
}
#[tokio::test]
async fn subscriber_requires_its_own_identity_and_exposes_only_explicit_remote_codecs() {
    use tok_events::Subscriber;
    let mut registry = Registry::new(1).unwrap();
    registry
        .register::<GuildChanged, _>(|_| Box::pin(async { Ok(Outcome::Handled) }))
        .unwrap();
    assert!(registry.event_types().is_empty());
    registry.enable_protobuf::<GuildChanged>().unwrap();
    let handler = Arc::new(registry);
    assert!(
        Subscriber::new(
            tok_core::service::Identity {
                node_id: "".into(),
                service_name: "guild".into()
            },
            Arc::clone(&handler) as Arc<dyn Handler>,
            tok_events::TopicFilter::Exact(tok_events::Topic::default()),
            None
        )
        .is_err()
    );
    assert!(
        Subscriber::new(
            tok_core::service::Identity {
                node_id: "node".into(),
                service_name: "guild".into()
            },
            handler,
            tok_events::TopicFilter::Exact(tok_events::Topic::default()),
            Some("bad..group")
        )
        .is_err()
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct Routed {
    #[prost(int64, tag = "1")]
    value: i64,
    #[prost(string, optional, tag = "2")]
    route: Option<String>,
}
impl Event for Routed {
    const NAME: &'static str = "test.routed.v1";
    fn topic(&self) -> Result<tok_events::Topic, tok_events::Error> {
        self.route.as_ref().map_or_else(
            || Ok(tok_events::Topic::default()),
            tok_events::Topic::named,
        )
    }
}
#[tokio::test]
async fn local_and_remote_paths_expose_the_same_topic_without_inheriting_parent_topic() {
    let mut handlers: Registry<Vec<tok_events::Topic>> = Registry::new(1).unwrap();
    handlers
        .register::<Routed, _>(|request| {
            Box::pin(async move {
                tokio::task::yield_now().await;
                request.input.state.push(request.input.event.topic.clone());
                Ok(Outcome::Handled)
            })
        })
        .unwrap();
    handlers.enable_protobuf::<Routed>().unwrap();
    let topic = tok_events::Topic::named("chat:7/测试").unwrap();
    let envelope = Envelope::new(
        EventId::generate(),
        Routed {
            value: 3,
            route: Some("chat:7/测试".into()),
        },
    )
    .unwrap();
    let message = Arc::new(Message::new(&envelope).unwrap());
    let mut seen = vec![];
    handlers
        .dispatch(&mut seen, support::request(&envelope))
        .await
        .unwrap();
    handlers
        .receive(&mut seen, support::request(Arc::clone(&message)))
        .await
        .unwrap();
    let child = Envelope::new(
        EventId::generate(),
        Routed {
            value: 1,
            route: None,
        },
    )
    .unwrap();
    let mut context = support::context();
    context.extensions_mut().insert(message);
    handlers
        .dispatch(&mut seen, tok_service::Request::new(context, &child))
        .await
        .unwrap();
    assert_eq!(
        seen,
        vec![topic.clone(), topic, tok_events::Topic::default()]
    );
}

#[tokio::test]
async fn decoded_route_must_match_event_fields_before_any_handler_runs() {
    let mut handlers: Registry<()> = Registry::new(1).unwrap();
    handlers
        .register::<Routed, _>(|_| Box::pin(async { panic!("错误路由不能进入业务") }))
        .unwrap();
    handlers.enable_protobuf::<Routed>().unwrap();
    let envelope = Envelope::new(
        EventId::generate(),
        Routed {
            value: 1,
            route: Some("chat:7".into()),
        },
    )
    .unwrap();
    let mut message = Message::new(&envelope).unwrap();
    message.topic = tok_events::Topic::named("chat:8").unwrap();
    assert!(matches!(
        handlers
            .receive(&mut (), support::request(Arc::new(message)))
            .await,
        Err(tok_events::DispatchError::Invalid(_))
    ));
}
