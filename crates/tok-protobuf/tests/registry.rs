//! 通用正文适配不依赖消息编号、游戏协议或 Tonic。
use prost::Message as _;
use std::time::Duration;
use tok_core::Identity;
use tok_execution::{Context, Request, registry::DispatchError};
use tok_protobuf as protobuf;
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}
#[derive(Debug, PartialEq, Eq)]
struct Denied;
fn context() -> Context {
    Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "test".into(),
        },
        "test",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap()
}
#[tokio::test]
async fn protobuf_adapter_accepts_string_keys_and_preserves_decode_and_business_errors() {
    let mut registry = protobuf::Registry::<String, u32, String, Denied>::new();
    registry
        .register(
            "increment".to_owned(),
            "result".to_owned(),
            |execution_request: tok_execution::Request<(u32, Value)>| async move {
                assert_eq!(
                    execution_request
                        .context
                        .extensions()
                        .get::<String>()
                        .unwrap(),
                    "caller-marker"
                );
                let (state, value) = execution_request.input;
                if value.value == 0 {
                    return Err(Denied);
                }
                Ok(protobuf::Reply {
                    message: Value {
                        value: state + value.value,
                    },
                    output: "saved".to_owned(),
                })
            },
        )
        .unwrap();
    let mut ctx = context();
    ctx.extensions_mut().insert(String::from("caller-marker"));
    let message = |body| protobuf::Message {
        key: "increment".to_owned(),
        body,
    };
    let reply = registry
        .dispatch(Request::new(
            ctx.clone(),
            (5, message(Value { value: 7 }.encode_to_vec())),
        ))
        .await
        .unwrap();
    assert_eq!(reply.message.key, "result");
    assert_eq!(
        Value::decode(reply.message.body.as_slice()).unwrap().value,
        12
    );
    assert_eq!(reply.output, "saved");
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx.clone(), (5, message(vec![]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Handler(Denied)))
    ));
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx.clone(), (5, message(vec![0xff]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Decode(_)))
    ));
    // 编解码也在执行范围内；已取消调用不再解析坏包。
    ctx.cancellation().cancel();
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx, (5, message(vec![0xff]))))
            .await,
        Err(DispatchError::Stopped(tok_execution::StopReason::Cancelled))
    ));
}

#[tokio::test]
async fn codec_does_not_reserve_zero_keys() {
    let mut registry = protobuf::Registry::<u32>::new();
    registry
        .register(
            0,
            0,
            |execution_request: tok_execution::Request<((), Value)>| async move {
                let (_, value) = execution_request.input;
                Ok(protobuf::Reply {
                    message: value,
                    output: (),
                })
            },
        )
        .unwrap();
    let reply = registry
        .dispatch(Request::new(
            context(),
            (
                (),
                protobuf::Message {
                    key: 0,
                    body: vec![],
                },
            ),
        ))
        .await
        .unwrap();
    assert_eq!(reply.message.key, 0);
}
