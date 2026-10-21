//! 事件 ID、类型声明和追踪载体的契约校验。
mod support;
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, TraceContext};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Reward(u64);
impl Event for Reward {
    const NAME: &'static str = "game.reward.v1";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
}
#[test]
fn event_identity_survives_repeated_publication_preparation() {
    let id = EventId::generate();
    assert_eq!(EventId::new(id.to_string()).unwrap(), id);
    assert_ne!(id, EventId::generate());
    let envelope = Envelope::new(id.clone(), Reward(3)).unwrap();
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
fn trace_carrier_is_bounded_and_revalidated_after_public_field_updates() {
    let mut envelope = Envelope::<Reward>::new(EventId::generate(), Reward(1)).unwrap();
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
#[tokio::test]
async fn local_dispatch_does_not_apply_remote_delivery_policy() {
    let mut registry = tok_events::Registry::<u64, (), tok_events::Error>::new(1).unwrap();
    registry
        .register::<Reward, _>(|request| {
            Box::pin(async move {
                *request.input.state += request.input.event.payload.0;
                Ok(tok_execution::Response::new(()))
            })
        })
        .unwrap();
    let event = Envelope::new(EventId::generate(), Reward(3)).unwrap();
    let mut value = 0;
    registry
        .dispatch(&mut value, support::request(&event))
        .await
        .unwrap();
    assert_eq!(value, 3);
}
