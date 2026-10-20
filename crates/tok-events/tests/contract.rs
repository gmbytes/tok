//! 事件 ID、类型声明、目标和追踪载体的契约校验。
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, EventTarget, TraceContext};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Reward(u64);
impl Event for Reward {
    const NAME: &'static str = "game.reward.v1";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
    const LOAD_OFFLINE: bool = true;
}
#[test]
fn event_identity_survives_repeated_publication_preparation() {
    let id = EventId::generate();
    assert_eq!(EventId::new(id.to_string()).unwrap(), id);
    assert_ne!(id, EventId::generate());
    let envelope = Envelope::new(id.clone(), EventTarget::Players(vec![42u64]), Reward(3)).unwrap();
    let duplicate = envelope.clone();
    assert_eq!(duplicate.event_id, id);
    assert_eq!(duplicate, envelope);
}
#[test]
fn invalid_ids_are_rejected_without_echoing_the_input() {
    for value in [
        "".into(),
        "a b".into(),
        "bad\nheader".into(),
        "中文".into(),
        "x".repeat(129),
    ] {
        let error = EventId::new(value).unwrap_err();
        assert_eq!(error.to_string(), "事件参数无效：事件 ID 无效");
    }
    assert_eq!(EventId::new("x".repeat(128)).unwrap().as_str().len(), 128);
}
#[test]
fn target_structure_is_checked_but_generic_player_ids_are_preserved() {
    for target in [
        EventTarget::<u64>::All,
        EventTarget::Channel("公会:42".into()),
        EventTarget::Players(vec![42]),
    ] {
        Envelope::new(EventId::generate(), target, Reward(1))
            .unwrap()
            .validate()
            .unwrap();
    }
    for target in [
        EventTarget::<u64>::Channel(" ".into()),
        EventTarget::Channel("x".repeat(257)),
        EventTarget::Players(vec![]),
        EventTarget::Players(vec![42; 1025]),
    ] {
        assert!(Envelope::new(EventId::generate(), target, Reward(1)).is_err());
    }
    Envelope::new(
        EventId::generate(),
        EventTarget::Players(vec![42; 1024]),
        Reward(1),
    )
    .unwrap();
}
#[test]
fn trace_carrier_is_bounded_and_revalidated_after_public_field_updates() {
    let mut envelope =
        Envelope::<Reward>::new(EventId::generate(), EventTarget::All, Reward(1)).unwrap();
    envelope.trace_context = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: Some("vendor=value".into()),
    };
    envelope.validate().unwrap();
    for trace in [
        TraceContext {
            traceparent: Some("x".repeat(513)),
            tracestate: None,
        },
        TraceContext {
            traceparent: Some("bad\nheader".into()),
            tracestate: None,
        },
        TraceContext {
            traceparent: Some("has space".into()),
            tracestate: None,
        },
        TraceContext {
            traceparent: Some(String::new()),
            tracestate: None,
        },
        TraceContext {
            traceparent: None,
            tracestate: Some("vendor=value".into()),
        },
        TraceContext {
            traceparent: Some("opaque".into()),
            tracestate: Some("x".repeat(513)),
        },
    ] {
        envelope.trace_context = trace;
        assert!(envelope.validate().is_err());
    }
    // 第一批只承载数据，不将边界校验冒充完整 W3C 解析。
    TraceContext {
        traceparent: Some("opaque".into()),
        tracestate: None,
    }
    .validate()
    .unwrap();
}
#[test]
fn local_dispatch_does_not_apply_remote_delivery_or_offline_loading_options() {
    let mut dispatcher = tok_events::local::Dispatcher::<Reward, u64, ()>::new(1).unwrap();
    dispatcher
        .register(|value, reward| {
            *value += reward.0;
            Ok(())
        })
        .unwrap();
    let mut value = 0;
    dispatcher.dispatch(&mut value, &Reward(3)).unwrap();
    assert_eq!(value, 3);
}
