//! 普通业务 RPC 独立于玩家上下文，保留执行范围与传输校验。
use std::{sync::Arc, time::Duration};
use tok_core::{Extensions, Identity, ServiceDescriptor};
use tok_execution::{Context, Request, Response};
use tok_message::{
    Registry, Reply,
    rpc::{self, Client as BusinessClient, RegistryHandler},
};
use tok_proto::internal::v1::{
    CallRequest, Payload, TimerTrigger, business_rpc_client::BusinessRpcClient,
};
use tokio_util::sync::CancellationToken;
use tonic::Code;

#[derive(Clone, PartialEq)]
struct Value {
    number: u32,
}
struct TestCodec;
impl tok_core::codec::Encode<Value> for TestCodec {
    type Error = std::convert::Infallible;
    fn encode(&self, value: &Value) -> Result<Vec<u8>, Self::Error> {
        Ok(value.number.to_le_bytes().to_vec())
    }
}
impl tok_core::codec::Decode<Value> for TestCodec {
    type Error = std::io::Error;
    fn decode(&self, body: &[u8]) -> Result<Value, Self::Error> {
        let bytes: [u8; 4] = body.try_into().map_err(std::io::Error::other)?;
        Ok(Value {
            number: u32::from_le_bytes(bytes),
        })
    }
}

#[tokio::test]
async fn business_rpc_preserves_extensions_and_rejects_timer() {
    let mut registry = Registry::<u32, (), std::convert::Infallible>::new();
    registry
        .register_service_with_codec(
            1,
            2,
            TestCodec,
            TestCodec,
            tower::service_fn(|request: Request<(u32, Value)>| async move {
                assert_eq!(request.extensions().get::<u64>(), Some(&42));
                Ok(Response::new(Reply {
                    message: Value {
                        number: request.input.0 + request.input.1.number,
                    },
                    output: (),
                }))
            }),
        )
        .unwrap();
    let handler = RegistryHandler::new(
        Arc::new(registry),
        |call| {
            call.bid
                .as_deref()
                .and_then(|v| v.parse::<u32>().ok())
                .ok_or_else(|| tonic::Status::invalid_argument("bid 无效"))
        },
        |error: std::convert::Infallible| match error {},
    );
    let mut extensions = Extensions::default();
    extensions.insert(42_u64);
    let service = rpc::Service::new("node", "guild", handler)
        .unwrap()
        .with_execution(Duration::from_secs(1), CancellationToken::new(), extensions)
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = tok_grpc::Listener::new()
        .add_service("guild", service.into_business())
        .unwrap();
    assert!(endpoint.readiness().set("guild", true));
    let stop = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, stop.clone()).unwrap());
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "guild".into(),
        protocol: "grpc".into(),
        address,
    };
    let call = rpc::CallContext {
        node_id: "node".into(),
        service_name: "guild".into(),
        bid: Some("7".into()),
        request_id: 3,
    };
    let ctx = Context::new(
        Identity {
            node_id: "caller".into(),
            service_name: "source".into(),
        },
        "test",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    let client = BusinessClient::new(Arc::new(
        tok_grpc::Client::new(Duration::from_secs(1), 2).unwrap(),
    ));
    let result = client
        .call(
            &target,
            Request::new(
                ctx,
                (
                    call,
                    Payload {
                        message_id: 1,
                        body: 5_u32.to_le_bytes().to_vec(),
                    },
                ),
            ),
        )
        .await
        .unwrap();
    let response = result.output;
    assert_eq!(response.request_id, 3);
    let payload = response.payload.unwrap();
    assert_eq!(payload.message_id, 2);
    assert_eq!(
        tok_core::codec::Decode::<Value>::decode(&TestCodec, &payload.body)
            .unwrap()
            .number,
        12
    );
    let mut raw = BusinessRpcClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let error = raw
        .call(CallRequest {
            context: Some(tok_proto::internal::v1::CallContext {
                node_id: "node".into(),
                service_name: "guild".into(),
                bid: Some("7".into()),
                request_id: 4,
                session: None,
            }),
            payload: Some(Payload {
                message_id: 1,
                body: Vec::new(),
            }),
            timer: Some(TimerTrigger::default()),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    let error = raw
        .call(CallRequest {
            context: Some(tok_proto::internal::v1::CallContext {
                node_id: "node".into(),
                service_name: "guild".into(),
                bid: None,
                request_id: 5,
                session: Some(Default::default()),
            }),
            payload: Some(Payload {
                message_id: 1,
                body: Vec::new(),
            }),
            timer: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
