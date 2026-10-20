//! 通用即时事件编码与版本、大小校验；不依赖外部服务。
use prost::Message as _;
use tok_events::{DeliveryPolicy, Envelope, Event, EventId, Message};
#[derive(Clone, PartialEq, prost::Message)]
struct Changed {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Changed {
    const NAME: &'static str = "test.changed.v1";
}
#[derive(Clone, PartialEq, prost::Message)]
struct Persistent {}
impl Event for Persistent {
    const NAME: &'static str = "test.persistent";
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Persistent;
}
fn envelope() -> Envelope<Changed> {
    Envelope::new(EventId::generate(), Changed { value: 7 }).unwrap()
}
#[test]
fn immediate_messages_round_trip_without_changing_event_identity_or_body() {
    let message = Message::new(&envelope()).unwrap();
    {
        let decoded = Message::decode(&message.encode(4096).unwrap(), 4096).unwrap();
        assert_eq!(decoded.event_id, message.event_id);
        assert_eq!(Changed::decode(decoded.body.as_slice()).unwrap().value, 7);
    }
}
#[test]
fn persistent_policy_is_rejected_before_publication() {
    assert!(matches!(
        Message::new(&Envelope::<Persistent>::new(EventId::generate(), Persistent {}).unwrap()),
        Err(tok_events::Error::Unsupported(_))
    ));
}
#[test]
fn bounded_decode_rejects_unknown_versions_and_invalid_payloads() {
    let valid = Message::new(&envelope()).unwrap().encode(4096).unwrap();
    let original = tok_proto::internal::v1::EventEnvelope::decode(valid.as_slice()).unwrap();
    let wire = tok_proto::internal::v1::EventEnvelope {
        version: 0,
        ..original
    };
    assert!(Message::decode(&wire.encode_to_vec(), 4096).is_err());
    assert!(Message::decode(&valid, valid.len() - 1).is_err());
    assert!(Message::decode(&[255], 4096).is_err());
    assert!(Message::new(&envelope()).unwrap().encode(1).is_err());
}

#[derive(Clone, PartialEq, prost::Message)]
struct Large {
    #[prost(bytes, tag = "1")]
    body: Vec<u8>,
}
impl Event for Large {
    const NAME: &'static str = "test.large.v1";
}
#[test]
fn typed_constructor_rejects_oversized_protobuf_payloads() {
    let envelope = Envelope::<Large>::new(
        EventId::generate(),
        Large {
            body: vec![0; 1_048_576],
        },
    )
    .unwrap();
    assert!(matches!(
        Message::new(&envelope),
        Err(tok_events::Error::InvalidInput(_))
    ));
}

#[test]
fn topic_round_trip_distinguishes_default_from_named_and_rejects_bad_wire_names() {
    let default = Message::new(&envelope()).unwrap();
    let routed = |route: &str| {
        Message::new(
            &Envelope::new(
                EventId::generate(),
                Routed {
                    value: 7,
                    route: Some(route.into()),
                },
            )
            .unwrap(),
        )
        .unwrap()
    };
    let named = routed("default");
    assert_ne!(default.topic, named.topic);
    for original in [default, named, routed("聊天/区服:42.a")] {
        let bytes = original.encode(4096).unwrap();
        assert_eq!(Message::decode(&bytes, 4096).unwrap().topic, original.topic);
        for bad in ["", "room.*", "room.>", "a b", "a\n", "\0", &"a".repeat(257)] {
            let mut wire =
                tok_proto::internal::v1::EventEnvelope::decode(bytes.as_slice()).unwrap();
            wire.topic = Some(bad.to_owned());
            assert!(
                Message::decode(&wire.encode_to_vec(), 4096).is_err(),
                "{bad:?}"
            );
        }
    }
    assert!(tok_events::Topic::named("界".repeat(86)).is_err());
    assert!(tok_events::Topic::named("a".repeat(256)).is_ok());
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

#[test]
fn typed_events_reject_routes_that_disagree_with_the_event_contract() {
    let mut global = envelope();
    global.topic = tok_events::Topic::named("other").unwrap();
    assert!(global.validate().is_err());
    assert!(Message::new(&global).is_err());
    assert!(
        Envelope::new(
            EventId::generate(),
            Routed {
                value: 1,
                route: Some("*".into())
            }
        )
        .is_err()
    );
}
