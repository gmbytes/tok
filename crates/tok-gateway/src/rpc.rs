//! 网关内部 RPC 入口，按网关服务名分发管理操作；由组装层绑定到受信内部端点。

use crate::player::{HandoffAction, SessionManager};
use crate::{ErrorCode, Result};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tok_message::rpc::{
    protocol::internal::v1 as proto,
    tonic::{self, Request, Response, Status},
};

/// 网关内部 RPC 服务，复用按网关服务名登记的会话管理器；当前提供会话交接方法。
#[derive(Clone)]
pub struct GatewayRpcService {
    player_managers: Arc<Mutex<BTreeMap<String, SessionManager>>>,
    max_handoff_bytes: usize,
}
impl GatewayRpcService {
    /// 设置单次快照上限；RPC 额外预留有界信封空间。
    pub fn new(max_handoff_bytes: usize) -> Result<Self> {
        if !(65536..=134217728).contains(&max_handoff_bytes) {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(Self {
            player_managers: Arc::new(Mutex::new(BTreeMap::new())),
            max_handoff_bytes,
        })
    }
    /// 在网关启动前登记玩家会话管理器，按网关服务名分发，重复名称拒绝。
    pub fn register_player(&self, manager: SessionManager) -> Result<()> {
        let mut player_managers = self
            .player_managers
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        let name = manager.service_name().to_owned();
        if player_managers.contains_key(&name) {
            return Err(ErrorCode::InvalidState.into());
        }
        player_managers.insert(name, manager);
        Ok(())
    }
    pub(crate) fn unregister_player(&self, manager: &SessionManager) -> Result<()> {
        let mut managers = self
            .player_managers
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        if managers
            .get(manager.service_name())
            .is_some_and(|current| current.same_instance(manager))
        {
            managers.remove(manager.service_name());
        }
        Ok(())
    }
    /// 生成可由 Listener 显式绑定的 tonic 服务。
    pub fn into_rpc(self) -> proto::gateway_rpc_server::GatewayRpcServer<Self> {
        let limit = self.max_handoff_bytes + 65536;
        proto::gateway_rpc_server::GatewayRpcServer::new(self)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit)
    }
    async fn invoke(
        &self,
        request: Request<proto::HandoffRequest>,
        action: Option<HandoffAction>,
    ) -> std::result::Result<Response<proto::HandoffResponse>, Status> {
        let manager = self.player_manager(&request.get_ref().source_service_name)?;
        let gateway = manager.gateway();
        let server = tok_grpc::Server::new(gateway.identity().clone())
            .and_then(|server| {
                server.with_execution(
                    std::time::Duration::from_millis(manager.handoff_budget_ms()),
                    gateway.cancellation(),
                    manager.extensions(),
                )
            })
            .map_err(|_| Status::internal("网关执行配置无效"))?;
        let operation = match action {
            None => "gateway/prepare_handoff",
            Some(HandoffAction::Commit) => "gateway/commit_handoff",
            Some(HandoffAction::Cancel) => "gateway/cancel_handoff",
            Some(HandoffAction::Complete) => "gateway/complete_handoff",
        };
        let metadata = request.metadata().clone();
        let mut input = request.into_inner();
        let request = server.request(&metadata, operation, ())?;
        if action.is_none() {
            input.budget_ms = input
                .budget_ms
                .min(request.context.remaining().as_millis() as u64);
        }
        server
            .execute(
                request.map(|()| input),
                tower::service_fn(
                    move |request: tok_execution::Request<proto::HandoffRequest>| {
                        let manager = manager.clone();
                        async move {
                            match action {
                                None => manager.prepare_handoff(request).await,
                                Some(action) => manager.finish_handoff(request.input, action).await,
                            }
                            .map(tok_execution::Response::new)
                            .map_err(handoff_status)
                        }
                    },
                ),
            )
            .await
            .map(|response| Response::new(response.output))
    }
    fn player_manager(&self, service_name: &str) -> std::result::Result<SessionManager, Status> {
        self.player_managers
            .lock()
            .map_err(|_| Status::internal("网关 RPC 入口不可用"))?
            .get(service_name)
            .cloned()
            .ok_or_else(|| Status::unavailable("网关未就绪或不存在"))
    }
}
fn handoff_status(error: crate::GatewayError) -> Status {
    match error.code {
        ErrorCode::CapacityExceeded => Status::resource_exhausted("交接容量不足"),
        ErrorCode::InvalidRequest => Status::invalid_argument("交接参数无效"),
        ErrorCode::Timeout => Status::deadline_exceeded("交接超时"),
        ErrorCode::Unavailable => Status::unavailable("交接依赖不可用"),
        _ => Status::failed_precondition("原会话无法完成交接"),
    }
}
#[tonic::async_trait]
impl proto::gateway_rpc_server::GatewayRpc for GatewayRpcService {
    async fn prepare_handoff(
        &self,
        request: Request<proto::HandoffRequest>,
    ) -> std::result::Result<Response<proto::HandoffResponse>, Status> {
        self.invoke(request, None).await
    }
    async fn commit_handoff(
        &self,
        request: Request<proto::HandoffRequest>,
    ) -> std::result::Result<Response<proto::HandoffResponse>, Status> {
        self.invoke(request, Some(HandoffAction::Commit)).await
    }
    async fn cancel_handoff(
        &self,
        request: Request<proto::HandoffRequest>,
    ) -> std::result::Result<Response<proto::HandoffResponse>, Status> {
        self.invoke(request, Some(HandoffAction::Cancel)).await
    }
    async fn complete_handoff(
        &self,
        request: Request<proto::HandoffRequest>,
    ) -> std::result::Result<Response<proto::HandoffResponse>, Status> {
        self.invoke(request, Some(HandoffAction::Complete)).await
    }
}
