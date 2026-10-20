//! 通用服务订阅：类型隔离、状态捕获、异步处理和容量，不依赖网关。
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
        Message::new(Envelope::new(id.clone(), GuildChanged { amount: 3 }).unwrap()).unwrap();
    let mut services = vec![];
    for _ in 0..2 {
        let state = Arc::new(AtomicUsize::new(0));
        let mut handlers = Registry::new(1).unwrap();
        let expected = id.clone();
        let captured = Arc::clone(&state);
        handlers
            .register::<GuildChanged, _, _>(move |message, event| {
                assert_eq!(message.event_id, expected);
                let state = Arc::clone(&captured);
                async move {
                    tokio::task::yield_now().await;
                    state.fetch_add(event.amount as usize, Ordering::SeqCst);
                    Ok(Outcome::Handled)
                }
            })
            .unwrap();
        services.push((handlers, state));
    }
    for (handlers, state) in &services {
        handlers.handle(Arc::new(message.clone())).await.unwrap();
        handlers.handle(Arc::new(message.clone())).await.unwrap();
        assert_eq!(state.load(Ordering::SeqCst), 6); // 不隐式去重。
    }
}

#[tokio::test]
async fn registry_rejects_bad_known_payloads_and_ignores_unsubscribed_types() {
    assert!(Registry::new(0).is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&calls);
    let mut handlers = Registry::new(1).unwrap();
    handlers
        .register::<GuildChanged, _, _>(move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
            async { Err(tok_events::Error::Unavailable("业务失败")) }
        })
        .unwrap();
    assert!(
        handlers
            .register::<GuildChanged, _, _>(|_, _| async { Ok(Outcome::Handled) })
            .is_err()
    );
    assert!(matches!(
        handlers.register::<MailSent, _, _>(|_, _| async { Ok(Outcome::Handled) }),
        Err(tok_events::Error::CapacityExceeded)
    ));
    let unknown = Message::new(Envelope::new(EventId::generate(), MailSent {}).unwrap()).unwrap();
    assert_eq!(
        handlers.handle(Arc::new(unknown)).await.unwrap(),
        Outcome::Ignored
    );
    let mut known =
        Message::new(Envelope::new(EventId::generate(), GuildChanged { amount: 1 }).unwrap())
            .unwrap();
    assert!(handlers.handle(Arc::new(known.clone())).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    known.body = vec![255];
    assert!(handlers.handle(Arc::new(known)).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn typed_handler_receives_the_original_shared_encoding_without_reencoding() {
    let mut encoded =
        Message::new(Envelope::new(EventId::generate(), GuildChanged { amount: 3 }).unwrap())
            .unwrap();
    encoded.body.extend_from_slice(&[0xa0, 0x06, 0x01]);
    let original = Arc::new(encoded);
    let expected = Arc::clone(&original);
    let mut registry = Registry::new(1).unwrap();
    registry
        .register::<GuildChanged, _, _>(move |message, event| {
            assert!(Arc::ptr_eq(&message, &expected));
            assert_eq!(message.body, expected.body);
            assert_eq!(event.amount, 3);
            async { Ok(Outcome::Handled) }
        })
        .unwrap();
    assert_eq!(registry.event_types(), vec![GuildChanged::NAME]);
    assert_eq!(registry.handle(original).await.unwrap(), Outcome::Handled);
}
