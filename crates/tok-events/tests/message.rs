//! 通用即时事件编码与版本、大小校验；不依赖外部服务。
use prost::Message as _;
mod codec_support;
use tok_events::{Envelope, Event, EventId, Message};
#[derive(Clone, PartialEq)]
struct Changed {
    value: i64,
}
impl codec_support::Body for Changed {
    fn encode_body(&self) -> Vec<u8> {
        self.value.to_le_bytes().to_vec()
    }
    fn decode_body(body: &[u8]) -> Result<Self, std::io::Error> {
        let bytes: [u8; 8] = body.try_into().map_err(std::io::Error::other)?;
        Ok(Self {
            value: i64::from_le_bytes(bytes),
        })
    }
}
impl Event for Changed {
    const NAME: &'static str = "test.changed.v1";
}
fn envelope() -> Envelope<Changed> {
    Envelope::new(EventId::generate(), Changed { value: 7 }).unwrap()
}
#[test]
fn immediate_messages_round_trip_without_changing_event_identity_or_body() {
    let message = codec_support::message(&envelope()).unwrap();
    {
        let decoded = Message::decode(&message.encode(4096).unwrap(), 4096).unwrap();
        assert_eq!(decoded.event_id, message.event_id);
        assert_eq!(
            <Changed as codec_support::Body>::decode_body(&decoded.body)
                .unwrap()
                .value,
            7
        );
    }
}
#[test]
fn bounded_decode_rejects_unknown_versions_and_invalid_payloads() {
    let valid = codec_support::message(&envelope())
        .unwrap()
        .encode(4096)
        .unwrap();
    let original = tok_proto::internal::v1::EventEnvelope::decode(valid.as_slice()).unwrap();
    let wire = tok_proto::internal::v1::EventEnvelope {
        version: 0,
        ..original
    };
    assert!(Message::decode(&wire.encode_to_vec(), 4096).is_err());
    assert!(Message::decode(&valid, valid.len() - 1).is_err());
    assert!(Message::decode(&[255], 4096).is_err());
    assert!(
        codec_support::message(&envelope())
            .unwrap()
            .encode(1)
            .is_err()
    );
}

#[derive(Clone, PartialEq)]
struct Large {
    body: Vec<u8>,
}
impl codec_support::Body for Large {
    fn encode_body(&self) -> Vec<u8> {
        self.body.clone()
    }
    fn decode_body(body: &[u8]) -> Result<Self, std::io::Error> {
        Ok(Self {
            body: body.to_vec(),
        })
    }
}
impl Event for Large {
    const NAME: &'static str = "test.large.v1";
}
#[test]
fn typed_constructor_rejects_oversized_payloads() {
    let envelope = Envelope::<Large>::new(
        EventId::generate(),
        Large {
            body: vec![0; 1_048_577],
        },
    )
    .unwrap();
    assert!(matches!(
        codec_support::message(&envelope),
        Err(tok_events::EncodeError::Event(
            tok_events::Error::InvalidInput(_)
        ))
    ));
}

#[test]
fn topic_round_trip_distinguishes_default_from_named_and_rejects_bad_wire_names() {
    let default = codec_support::message(&envelope()).unwrap();
    let routed = |route: &str| {
        codec_support::message(
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

#[derive(Clone, PartialEq)]
struct Routed {
    value: i64,
    route: Option<String>,
}
impl codec_support::Body for Routed {
    fn encode_body(&self) -> Vec<u8> {
        self.value.to_le_bytes().to_vec()
    }
    fn decode_body(_: &[u8]) -> Result<Self, std::io::Error> {
        Err(std::io::Error::other("不用于解码"))
    }
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
    assert!(codec_support::message(&global).is_err());
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
