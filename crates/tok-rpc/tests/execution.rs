//! 通用设施可接入任意输入、结果及借用，不依赖游戏协议。
use std::time::Duration;
use tok_core::{Extensions, Identity, ServiceDescriptor};
use tok_execution::Request;
use tok_rpc::{RpcClients, Server};
use tokio_util::sync::CancellationToken;
use tonic::{Code, metadata::MetadataMap};

fn server(stop: CancellationToken) -> Server {
    Server::new(Identity {
        node_id: "node".into(),
        service_name: "custom".into(),
    })
    .unwrap()
    .with_execution(Duration::from_secs(1), stop, Extensions::default())
    .unwrap()
}
#[tokio::test]
async fn arbitrary_borrowed_input_and_result_use_the_common_execution_chain() {
    let server = server(CancellationToken::new());
    let mut value = String::from("custom");
    let context = server.context(&MetadataMap::new(), "append").unwrap();
    let service = tok_execution::sync_handler(|request: Request<&mut String>| {
        request.input.push_str(" protocol");
        Ok::<_, tonic::Status>(request.input.len())
    });
    assert_eq!(
        server
            .execute(Request::new(context, &mut value), service)
            .await
            .unwrap(),
        15
    );
    assert_eq!(value, "custom protocol");
}
#[tokio::test]
async fn remote_budget_only_shortens_local_budget_and_stop_prevents_execution() {
    let stop = CancellationToken::new();
    let server = server(stop.clone());
    let mut metadata = MetadataMap::new();
    metadata.insert("grpc-timeout", "1H".parse().unwrap());
    let context = server.context(&metadata, "test").unwrap();
    assert!(context.remaining() <= Duration::from_secs(1));
    metadata.insert("grpc-timeout", "1m".parse().unwrap());
    assert!(server.context(&metadata, "test").unwrap().remaining() <= Duration::from_millis(1));
    metadata.insert("grpc-timeout", "invalid".parse().unwrap());
    assert_eq!(
        server.context(&metadata, "test").unwrap_err().code(),
        Code::InvalidArgument
    );
    stop.cancel();
    let result = server
        .execute(
            Request::new(context, ()),
            tok_execution::sync_handler(|_: Request<()>| -> Result<(), tonic::Status> {
                panic!("cancelled handler was polled")
            }),
        )
        .await;
    assert_eq!(result.unwrap_err().code(), Code::Cancelled);
}
#[tokio::test]
async fn arbitrary_client_protocol_gets_a_deadline_without_business_types() {
    let clients = RpcClients::new(Duration::from_secs(1), 1).unwrap();
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "custom".into(),
        protocol: "grpc".into(),
        address: "127.0.0.1:12345".parse().unwrap(),
    };
    let result = clients
        .execute(
            &target,
            7,
            tonic::Request::new("custom"),
            tracing::Span::none(),
            |_, request| async move {
                assert!(request.metadata().contains_key("grpc-timeout"));
                Ok(tonic::Response::new(request.into_inner().len()))
            },
        )
        .await
        .unwrap();
    assert_eq!(result, 6);
}
