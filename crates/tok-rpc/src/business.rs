//! 普通 BusinessRpc 客户端适配；不要求会话，不自动加锁。
use crate::{RpcClients, call};
use std::sync::Arc;
use tok_core::{CallContext, ServiceDescriptor};
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;

/// 普通业务协议客户端；与其他适配器共享连接而不共享业务语义。
#[derive(Clone)]
pub struct Client {
    connections: Arc<RpcClients>,
}
impl Client {
    /// 注入共享连接管理器，不建立连接。
    pub fn new(connections: Arc<RpcClients>) -> Self {
        Self { connections }
    }
    /// 调用普通业务入口；拒绝内部 timer，不附加锁或重试。
    pub async fn call(
        &self,
        target: &ServiceDescriptor,
        context: CallContext,
        payload: Payload,
    ) -> Result<CallResponse, Status> {
        if context.timer.is_some() {
            return Err(Status::invalid_argument("计时触发只能调用 player 入口"));
        }
        call::call(
            &self.connections,
            target,
            context,
            payload,
            |channel, request| async move {
                proto::business_rpc_client::BusinessRpcClient::new(channel)
                    .call(request)
                    .await
            },
        )
        .await
    }
}
