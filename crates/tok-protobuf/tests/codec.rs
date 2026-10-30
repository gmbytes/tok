//! protobuf 适配独立于消息分发和事件。
use tok_core::codec::{Decode, Encode};

#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}

#[test]
fn codec_round_trips_and_returns_prost_decode_error() {
    let encoded = tok_protobuf::Codec.encode(&Value { value: 7 }).unwrap();
    let result: Value = tok_protobuf::Codec.decode(&encoded).unwrap();
    assert_eq!(result.value, 7);
    let error: Result<Value, prost::DecodeError> = tok_protobuf::Codec.decode(&[255]);
    assert!(error.is_err());
}
