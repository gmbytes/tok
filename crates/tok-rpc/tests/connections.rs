//! 共享端点上多个 protobuf 服务复用 TCP 连接的集成测试。

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{
    CallRequest, CallResponse, Payload,
    business_rpc_server::{BusinessRpc, BusinessRpcServer},
    player_rpc_server::{PlayerRpc, PlayerRpcServer},
};
use tok_rpc::CallContext;
use tok_rpc::{RpcClients, RpcEndpoint};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct Probe(Arc<Mutex<Vec<std::net::SocketAddr>>>);
impl Probe {
    fn reply(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.0.lock().unwrap().push(request.remote_addr().unwrap());
        let request = request.into_inner();
        Ok(Response::new(CallResponse {
            request_id: request.context.unwrap().request_id,
            payload: request.payload,
            error: None,
            timers: None,
        }))
    }
}
#[tonic::async_trait]
impl PlayerRpc for Probe {
    async fn handle_event(
        &self,
        _: Request<tok_rpc::protocol::internal::v1::EventRequest>,
    ) -> Result<Response<tok_rpc::protocol::internal::v1::EventResponse>, Status> {
        Err(Status::unimplemented("此连接探针不处理事件"))
    }

    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.reply(request)
    }
}
#[tonic::async_trait]
impl BusinessRpc for Probe {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.reply(request)
    }
}

#[tokio::test]
async fn two_services_on_one_endpoint_share_a_tcp_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let probe = Probe::default();
    let endpoint = RpcEndpoint::new()
        .add_service("player", PlayerRpcServer::new(probe.clone()))
        .unwrap()
        .add_service("guild", BusinessRpcServer::new(probe.clone()))
        .unwrap();
    endpoint.readiness().set("player", true);
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 1).unwrap());
    let player_client = tok_rpc::player::Client::new(Arc::clone(&clients));
    let business_client = tok_rpc::business::Client::new(Arc::clone(&clients));
    for (name, player) in [("player", true), ("guild", false)] {
        let target = ServiceDescriptor {
            node_id: "local".into(),
            service_name: name.into(),
            protocol: "grpc".into(),
            address,
        };
        let context = CallContext {
            timer: None,
            node_id: "local".into(),
            service_name: name.into(),
            bid: None,
            request_id: 1,
            session: None,
        };
        let payload = Payload {
            message_id: 1,
            body: vec![],
        };
        if player {
            player_client.call(&target, context, payload).await.unwrap();
        } else {
            business_client
                .call(&target, context, payload)
                .await
                .unwrap();
        }
    }
    let peers = probe.0.lock().unwrap().clone();
    assert_eq!(peers.len(), 2);
    assert_eq!(peers[0], peers[1]);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
