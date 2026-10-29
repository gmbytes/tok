use super::{CallContext, Handler};
use std::{sync::Arc, time::Duration};
use tok_core::service::Identity;
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse};
use tonic::{Request, Response, Status};

/// 普通编号消息的 gRPC 适配；通用执行由 tok_grpc::Server 承担。
#[derive(Clone)]
pub struct Service {
    node_id: String,
    service_name: String,
    handler: Arc<dyn Handler>,
    server: tok_grpc::Server,
}

impl Service {
    /// 显式绑定节点和业务服务名，拒绝空身份。
    pub fn new(
        node_id: impl Into<String>,
        service_name: impl Into<String>,
        handler: Arc<dyn Handler>,
    ) -> Result<Self, super::Error> {
        let node_id = node_id.into();
        let service_name = service_name.into();
        if node_id.trim().is_empty() || service_name.trim().is_empty() {
            return Err(super::Error::Configuration("RPC 服务身份不能为空".into()));
        }
        Ok(Self {
            node_id: node_id.clone(),
            service_name: service_name.clone(),
            handler,
            server: tok_grpc::Server::new(Identity {
                node_id: node_id.clone(),
                service_name: service_name.clone(),
            })?,
        })
    }

    /// 创建尚未安装 handler 的入口及绑定槽；工厂通过 Binding::install 安装实际处理链。
    pub fn deferred(
        node_id: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Result<(Self, super::Binding), super::Error> {
        let identity = Identity {
            node_id: node_id.into(),
            service_name: service_name.into(),
        };
        let binding = super::Binding::new(identity.clone());
        let service = Self::new(
            identity.node_id,
            identity.service_name,
            Arc::new(binding.clone()),
        )?;
        Ok((service, binding))
    }
    /// 绑定本服务的执行预算、取消与能力；默认预算为 30 秒，远端超时只会缩短它。
    pub fn with_execution(
        mut self,
        budget: Duration,
        cancellation: tokio_util::sync::CancellationToken,
        extensions: tok_core::Extensions,
    ) -> Result<Self, super::Error> {
        self.server = self
            .server
            .with_execution(budget, cancellation, extensions)?;
        Ok(self)
    }
    /// 转换为普通业务入口，不自动附加锁或重试策略。
    pub fn into_business(self) -> proto::business_rpc_server::BusinessRpcServer<Self> {
        proto::business_rpc_server::BusinessRpcServer::new(self)
    }

    fn context(
        &self,
        context: Option<proto::CallContext>,
        timer: Option<proto::TimerTrigger>,
    ) -> Result<CallContext, Status> {
        let context = context.ok_or_else(|| Status::invalid_argument("缺少调用上下文"))?;
        if context.node_id != self.node_id || context.service_name != self.service_name {
            return Err(Status::failed_precondition("调用目标与绑定服务不匹配"));
        }
        if context.request_id == 0
            || context
                .bid
                .as_ref()
                .is_some_and(|bid| bid.trim().is_empty())
        {
            return Err(Status::invalid_argument("调用序号或业务 ID 无效"));
        }
        if timer.is_some() {
            return Err(Status::invalid_argument("普通业务入口不能接受计时触发"));
        }
        if context.session.is_some() {
            return Err(Status::invalid_argument("普通业务入口不能接受玩家会话身份"));
        }
        Ok(CallContext {
            node_id: context.node_id,
            service_name: context.service_name,
            bid: context.bid,
            request_id: context.request_id,
        })
    }

    async fn invoke(
        &self,
        request: Request<CallRequest>,
    ) -> Result<Response<CallResponse>, Status> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let context = self.context(request.context, request.timer)?;
        let payload = request
            .payload
            .ok_or_else(|| Status::invalid_argument("缺少游戏消息"))?;
        if payload.message_id == 0 {
            return Err(Status::invalid_argument("消息 ID 无效"));
        }
        let request_id = context.request_id;
        let operation = format!("rpc/{}", payload.message_id);
        let mut execution =
            self.server
                .request(&metadata, operation, (context.clone(), payload))?;
        execution.extensions_mut().insert(context.clone());
        let handler = Arc::clone(&self.handler);
        let mut response = self
            .server
            .execute(
                execution,
                tower::service_fn(move |request| {
                    let handler = Arc::clone(&handler);
                    async move { handler.call(request).await }
                }),
            )
            .await?;
        response.output.request_id = request_id;
        Ok(Response::new(response.output))
    }
}

#[tonic::async_trait]
impl proto::business_rpc_server::BusinessRpc for Service {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request).await
    }
}
