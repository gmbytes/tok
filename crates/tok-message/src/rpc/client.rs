//! 普通业务 RPC 的出站调用。
use super::CallContext;
use super::call;
use std::sync::Arc;
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;

/// 普通业务协议客户端；与其他适配器共享连接而不共享业务语义。
#[derive(Clone)]
pub struct Client {
    connections: Arc<tok_grpc::Client>,
}
impl Client {
    /// 注入共享连接管理器，不建立连接。
    pub fn new(connections: Arc<tok_grpc::Client>) -> Self {
        Self { connections }
    }
    /// 调用普通业务入口；拒绝内部 timer，不附加锁或重试。
    pub async fn call(
        &self,
        target: &ServiceDescriptor,
        request: tok_execution::Request<(CallContext, Payload)>,
    ) -> Result<tok_execution::Response<CallResponse>, Status> {
        call::call(
            &self.connections,
            target,
            request,
            |channel, request| async move {
                proto::business_rpc_client::BusinessRpcClient::new(channel)
                    .call(request)
                    .await
            },
        )
        .await
    }
}
