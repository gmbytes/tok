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
                    execution_request.extensions().get::<String>().unwrap(),
                    "caller-marker"
                );
                let (state, value) = execution_request.input;
                if value.value == 0 {
                    return Err(Denied);
                }
                Ok(tok_execution::Response::new(protobuf::Reply {
                    message: Value {
                        value: state + value.value,
                    },
                    output: "saved".to_owned(),
                }))
            },
        )
        .unwrap();
    let mut ctx = Request::new(context(), ());
    ctx.extensions_mut().insert(String::from("caller-marker"));
    let message = |body| protobuf::Message {
        key: "increment".to_owned(),
        body,
    };
    let reply = registry
        .dispatch(
            ctx.clone()
                .map(|()| (5, message(Value { value: 7 }.encode_to_vec()))),
        )
        .await
        .unwrap();
    assert_eq!(reply.output.message.key, "result");
    assert_eq!(
        Value::decode(reply.output.message.body.as_slice())
            .unwrap()
            .value,
        12
    );
    assert_eq!(reply.output.output, "saved");
    assert!(matches!(
        registry
            .dispatch(ctx.clone().map(|()| (5, message(vec![]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Handler(Denied)))
    ));
    assert!(matches!(
        registry
            .dispatch(ctx.clone().map(|()| (5, message(vec![0xff]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Decode(_)))
    ));
    // 编解码也在执行范围内；已取消调用不再解析坏包。
    ctx.context.cancellation().cancel();
    assert!(matches!(
        registry
            .dispatch(ctx.map(|()| (5, message(vec![0xff]))))
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
                Ok(tok_execution::Response::new(protobuf::Reply {
                    message: value,
                    output: (),
                }))
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
    assert_eq!(reply.output.message.key, 0);
}

#[tokio::test]
async fn direct_service_preserves_policy_metadata_decode_errors_and_registration_snapshots() {
    use tower::ServiceExt as _;
    let mut registry = protobuf::Registry::<u32, (), (), Denied>::new();
    registry
        .register(1, 2, |request: Request<((), Value)>| async move {
            assert_eq!(request.extensions().get::<u64>(), Some(&42));
            let mut response = tok_execution::Response::new(protobuf::Reply {
                message: request.input.1,
                output: (),
            });
            response
                .extensions_mut()
                .insert(String::from("encoded-result"));
            Ok(response)
        })
        .unwrap();
    let frozen = registry.clone();
    registry
        .register(3, 4, |request: Request<((), Value)>| async move {
            Ok(tok_execution::Response::new(protobuf::Reply {
                message: request.input.1,
                output: (),
            }))
        })
        .unwrap();
    let input = |key, body| Request::new(context(), ((), protobuf::Message { key, body }));
    assert!(matches!(
        frozen.clone().oneshot(input(3, vec![])).await,
        Err(DispatchError::Unknown(3))
    ));
    assert_eq!(
        registry
            .oneshot(input(3, vec![]))
            .await
            .unwrap()
            .output
            .message
            .key,
        4
    );
    let chain = tower::ServiceBuilder::new()
        .map_request(|mut request: Request<((), protobuf::Message<u32>)>| {
            request.extensions_mut().insert(42u64);
            request
        })
        .service(frozen);
    let reply = chain
        .clone()
        .oneshot(input(1, Value { value: 7 }.encode_to_vec()))
        .await
        .unwrap();
    assert_eq!(
        reply.extensions().get::<String>().unwrap(),
        "encoded-result"
    );
    assert!(reply.extensions().get::<u64>().is_none());
    assert_eq!(reply.output.message.key, 2);
    assert_eq!(
        Value::decode(reply.output.message.body.as_slice())
            .unwrap()
            .value,
        7
    );
    assert!(matches!(
        chain.clone().oneshot(input(1, vec![0xff])).await,
        Err(DispatchError::Handler(protobuf::Error::Decode(_)))
    ));
    let request = input(99, vec![]);
    request.context.cancellation().cancel();
    assert!(matches!(
        chain.oneshot(request).await,
        Err(DispatchError::Stopped(tok_execution::StopReason::Cancelled))
    ));
}
