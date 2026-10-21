use super::{CallContext, Handler};
use std::{sync::Arc, time::Duration};
use tok_core::service::Identity;
use tok_execution::Request as ExecutionRequest;
use tok_player::{SessionIdentity, TimerTrigger};
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse};
use tonic::{Request, Response, Status};

/// 游戏消息与玩家执行协议的 gRPC 适配；通用执行由 tok_grpc::Server 承担。
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
    /// 转换为 player 入口，要求调用上下文携带与 bid 匹配的会话身份。
    pub fn into_player(self) -> proto::player_rpc_server::PlayerRpcServer<Self> {
        proto::player_rpc_server::PlayerRpcServer::new(self)
    }
    /// 转换为普通业务入口，不自动附加锁或重试策略。
    pub fn into_business(self) -> proto::business_rpc_server::BusinessRpcServer<Self> {
        proto::business_rpc_server::BusinessRpcServer::new(self)
    }

    fn context(
        &self,
        context: Option<proto::CallContext>,
        timer: Option<proto::TimerTrigger>,
        player: bool,
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
        let session = context.session.map(|identity| SessionIdentity {
            player_id: identity.player_id,
            session_id: identity.session_id,
            epoch: identity.epoch,
        });
        if player
            && !session.as_ref().is_some_and(|identity| {
                !identity.player_id.trim().is_empty()
                    && !identity.session_id.trim().is_empty()
                    && identity.epoch > 0
                    && context.bid.as_deref() == Some(identity.player_id.as_str())
            })
        {
            return Err(Status::failed_precondition("player 调用需要匹配的会话身份"));
        }
        let timer = timer.map(|timer| TimerTrigger {
            timer_id: timer.timer_id,
            scheduled_at_ms: timer.scheduled_at_ms,
            fired_at_ms: timer.fired_at_ms,
        });
        if timer.as_ref().is_some_and(|timer| {
            !player
                || timer.timer_id.trim().is_empty()
                || timer.timer_id.len() > 256
                || timer.scheduled_at_ms < 0
                || timer.fired_at_ms < timer.scheduled_at_ms
        }) {
            return Err(Status::invalid_argument("计时来源或时刻无效"));
        }
        Ok(CallContext {
            node_id: context.node_id,
            service_name: context.service_name,
            bid: context.bid,
            request_id: context.request_id,
            session,
            timer,
        })
    }

    async fn invoke(
        &self,
        request: Request<CallRequest>,
        player: bool,
    ) -> Result<Response<CallResponse>, Status> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let context = self.context(request.context, request.timer, player)?;
        let payload = request
            .payload
            .ok_or_else(|| Status::invalid_argument("缺少游戏消息"))?;
        if payload.message_id == 0 {
            return Err(Status::invalid_argument("消息 ID 无效"));
        }
        let request_id = context.request_id;
        let operation = format!(
            "{}/{}",
            if context.timer.is_some() {
                "timer"
            } else {
                "rpc"
            },
            payload.message_id
        );
        let mut execution = self.server.context(&metadata, operation)?;
        execution.extensions_mut().insert(context.clone());
        if let Some(trigger) = &context.timer {
            execution.extensions_mut().insert(trigger.clone());
        }
        let handler = Arc::clone(&self.handler);
        let mut response = self
            .server
            .execute(
                ExecutionRequest::new(execution, (context, payload)),
                tower::service_fn(move |request| {
                    let handler = Arc::clone(&handler);
                    async move { handler.call(request).await }
                }),
            )
            .await?;
        response.request_id = request_id;
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl proto::player_rpc_server::PlayerRpc for Service {
    async fn handle_event(
        &self,
        request: Request<proto::EventRequest>,
    ) -> Result<Response<proto::EventResponse>, Status> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let context = self.context(request.context, None, true)?;
        let event = tok_events::Message {
            topic: request
                .topic
                .map_or_else(
                    || Ok(tok_events::Topic::default()),
                    tok_events::Topic::named,
                )
                .map_err(|_| Status::invalid_argument("事件 topic 无效"))?,
            event_id: tok_events::EventId::new(request.event_id)
                .map_err(|_| Status::invalid_argument("事件 ID 无效"))?,
            event_type: request.event_type,
            body: request.body,
            trace_context: Default::default(),
        };
        event
            .validate(1_048_576)
            .map_err(|_| Status::invalid_argument("事件参数无效"))?;
        let request_id = context.request_id;
        let mut execution = self
            .server
            .context(&metadata, format!("event/{}", event.event_type))?;
        execution.extensions_mut().insert(context.clone());
        execution.extensions_mut().insert(event.event_id.clone());
        execution.extensions_mut().insert(event.topic.clone());
        let event = Arc::new(event);
        execution.extensions_mut().insert(Arc::clone(&event));
        let handler = Arc::clone(&self.handler);
        let mut response = self
            .server
            .execute(
                ExecutionRequest::new(execution, (context, event)),
                tower::service_fn(move |request| {
                    let handler = Arc::clone(&handler);
                    async move { handler.event(request).await }
                }),
            )
            .await?;
        response.request_id = request_id;
        Ok(Response::new(response))
    }

    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request, true).await
    }
}
#[tonic::async_trait]
impl proto::business_rpc_server::BusinessRpc for Service {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request, false).await
    }
}
