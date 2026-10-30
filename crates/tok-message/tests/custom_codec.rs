//! 编号消息使用非 protobuf 编解码，保留执行范围与具体错误。
use std::time::Duration;
use tok_core::{
    Identity,
    codec::{Decode, Encode},
};
use tok_execution::{Context, Request, Response};
use tok_message::{Message, Registry, Reply};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

#[derive(Clone)]
struct ByteCodec;
impl Decode<u8> for ByteCodec {
    type Error = std::io::Error;
    fn decode(&self, body: &[u8]) -> Result<u8, Self::Error> {
        match body {
            [value] => Ok(*value),
            _ => Err(std::io::Error::other("正文无效")),
        }
    }
}
impl Encode<u8> for ByteCodec {
    type Error = std::io::Error;
    fn encode(&self, value: &u8) -> Result<Vec<u8>, Self::Error> {
        if *value == u8::MAX {
            return Err(std::io::Error::other("回应无效"));
        }
        Ok(vec![*value])
    }
}
fn request(body: Vec<u8>) -> Request<((), Message)> {
    let context = Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "service".into(),
        },
        "test",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let mut request = Request::new(context, ((), Message { key: 1, body }));
    request.extensions_mut().insert(42_u64);
    request
}
#[tokio::test]
async fn numbered_service_uses_custom_codec_and_preserves_request_and_response_extensions() {
    let mut registry = Registry::<()>::new();
    registry
        .register_service_with_codec(
            1,
            2,
            ByteCodec,
            ByteCodec,
            tower::service_fn(|request: Request<((), u8)>| async move {
                assert_eq!(request.extensions().get::<u64>(), Some(&42));
                let mut response = Response::new(Reply {
                    message: request.input.1,
                    output: (),
                });
                response.extensions_mut().insert(7_u32);
                Ok::<_, std::convert::Infallible>(response)
            }),
        )
        .unwrap();
    let response = registry.clone().oneshot(request(vec![3])).await.unwrap();
    assert_eq!(response.output.message.body, [3]);
    assert_eq!(response.extensions().get::<u32>(), Some(&7));
    let invalid = registry.clone().oneshot(request(vec![])).await.unwrap_err();
    assert!(matches!(
        invalid,
        tok_execution::Error::Inner(tok_execution::registry::DispatchError::Handler(
            tok_message::codec::Error::Decode(_)
        ))
    ));
    let invalid = registry.oneshot(request(vec![255])).await.unwrap_err();
    assert!(matches!(
        invalid,
        tok_execution::Error::Inner(tok_execution::registry::DispatchError::Handler(
            tok_message::codec::Error::Encode(_)
        ))
    ));
}

#[tokio::test]
async fn custom_body_codec_crosses_real_rpc_boundary() {
    use std::sync::Arc;
    let mut registry = Registry::<()>::new();
    registry
        .register_service_with_codec(
            1,
            2,
            ByteCodec,
            ByteCodec,
            tower::service_fn(|request: Request<((), u8)>| async move {
                Ok::<_, std::convert::Infallible>(Response::new(Reply {
                    message: request.input.1 + 1,
                    output: (),
                }))
            }),
        )
        .unwrap();
    let handler = tok_message::rpc::RegistryHandler::new(
        Arc::new(registry),
        |_| Ok(()),
        |never| match never {},
    );
    let service = tok_message::rpc::Service::new("node", "service", handler).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tok_grpc::Listener::new()
        .add_service("service", service.into_business())
        .unwrap();
    server.readiness().set("service", true);
    let stop = CancellationToken::new();
    let running = tokio::spawn(server.serve(listener, stop.clone()).unwrap());
    let client = tok_message::rpc::Client::new(Arc::new(
        tok_grpc::Client::new(Duration::from_secs(2), 2).unwrap(),
    ));
    let target = tok_core::ServiceDescriptor {
        node_id: "node".into(),
        service_name: "service".into(),
        protocol: "grpc".into(),
        address,
    };
    let context = Context::new(
        Identity {
            node_id: "client".into(),
            service_name: "caller".into(),
        },
        "call",
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .unwrap();
    let result = client
        .call(
            &target,
            Request::new(
                context,
                (
                    tok_message::rpc::CallContext {
                        node_id: "node".into(),
                        service_name: "service".into(),
                        bid: None,
                        request_id: 1,
                    },
                    tok_proto::internal::v1::Payload {
                        message_id: 1,
                        body: vec![7],
                    },
                ),
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.output.payload.unwrap().body, [8]);
    stop.cancel();
    running.await.unwrap().unwrap();
}
