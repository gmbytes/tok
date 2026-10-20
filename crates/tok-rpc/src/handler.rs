use prost::Message;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use tok_core::{CallContext, SessionIdentity, TimerTrigger, TimerUpdate};
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse, Payload};
use tonic::{Request, Response, Status};
use tower::{Service, ServiceExt};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// handler 的业务结果；系统级失败使用 gRPC Status，业务错误可放 RpcError。
pub type CallTask<'a> = Pin<Box<dyn Future<Output = Result<CallResponse, Status>> + Send + 'a>>;

/// 网关之外的组件通过此契约提供业务分发，不依赖具体游戏。
pub trait CallHandler: Send + Sync {
    /// 根据调用上下文处理游戏消息，返回业务回应或系统级 gRPC 错误。
    fn call(&self, context: CallContext, payload: Payload) -> CallTask<'_>;
}

/// 将用户组装的 Tower Service 适配为业务 handler。
///
/// Service 使用 `(CallContext, Payload)` 输入并返回 CallResponse。
/// 每次调用克隆 Service，建议用户使用共享并发限制器；其他 Layer 的错误
/// 应在适配前通过 map_err 映射为 Status。此适配不添加重试或默认锁。
pub struct TowerHandler<S>(
    /// 用户已组装的业务调用 Service，克隆时应共享所需的并发限制状态。
    pub S,
);

impl<S> CallHandler for TowerHandler<S>
where
    S: Service<(CallContext, Payload), Response = CallResponse, Error = Status>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    fn call(&self, context: CallContext, payload: Payload) -> CallTask<'_> {
        Box::pin(self.0.clone().oneshot((context, payload)))
    }
}

type RegisteredHandler = dyn Fn(CallContext, Vec<u8>) -> CallTask<'static> + Send + Sync;

/// 强类型业务回应及仅供 session 消费的可选计时更新。
pub struct HandlerReply<M> {
    /// 编码给调用方的业务回应。
    pub message: M,
    /// None 保持已有计时集合；更新成功应用前不会发送客户端回应。
    pub timers: Option<TimerUpdate>,
}

/// 显式维护消息 ID 和 Request / Response 配对的强类型分发表。
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: BTreeMap<u32, Arc<RegisteredHandler>>,
    timers: BTreeMap<u32, Arc<RegisteredHandler>>,
}

impl HandlerRegistry {
    /// 创建空分发表，注册完成后交给 RPC 服务使用。
    pub fn new() -> Self {
        Self::default()
    }

    /// 为一个请求 ID 注册解码、handler 和回应 ID；拒绝零 ID 及重复请求注册。
    /// 业务 handler 返回的类型由 prost 编码，未知 ID 和非法正文不会执行 handler。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_message_id: u32,
        response_message_id: u32,
        handler: F,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, Status>> + Send + 'static,
    {
        self.register_with_timers(
            request_message_id,
            response_message_id,
            move |context, request| {
                let future = handler(context, request);
                async move {
                    Ok(HandlerReply {
                        message: future.await?,
                        timers: None,
                    })
                }
            },
        )
    }

    /// 注册可同时返回业务回应与计时更新的强类型请求处理函数。
    pub fn register_with_timers<Req, Resp, F, Fut>(
        &mut self,
        request_message_id: u32,
        response_message_id: u32,
        handler: F,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HandlerReply<Resp>, Status>> + Send + 'static,
    {
        if request_message_id == 0
            || response_message_id == 0
            || self.handlers.contains_key(&request_message_id)
        {
            return Err(crate::Error::Configuration(
                "消息 ID 必须非零且请求只能注册一次".into(),
            ));
        }
        let handler = Arc::new(handler);
        self.handlers.insert(
            request_message_id,
            Arc::new(move |context, body| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let request = Req::decode(body.as_slice())
                        .map_err(|_| Status::invalid_argument("游戏消息解码失败"))?;
                    let request_id = context.request_id;
                    let response = handler(context, request).await?;
                    Ok(CallResponse {
                        request_id,
                        payload: Some(Payload {
                            message_id: response_message_id,
                            body: response.message.encode_to_vec(),
                        }),
                        error: None,
                        timers: response.timers.map(crate::timer::encode_update),
                    })
                })
            }),
        );
        Ok(())
    }

    /// 注册内部 timer 处理函数；参数从 spec 正文解码，来源和预定、实际时刻来自 context.timer。
    /// 返回的更新只交给 session，不产生客户端回应；客户端业务请求不能进入此分发表。
    pub fn register_timer<Req, F, Fut>(
        &mut self,
        message_id: u32,
        handler: F,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<TimerUpdate>, Status>> + Send + 'static,
    {
        if message_id == 0 || self.timers.contains_key(&message_id) {
            return Err(crate::Error::Configuration(
                "计时消息 ID 必须非零且只能注册一次".into(),
            ));
        }
        let handler = Arc::new(handler);
        self.timers.insert(
            message_id,
            Arc::new(move |context, body| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let request = Req::decode(body.as_slice())
                        .map_err(|_| Status::invalid_argument("计时参数解码失败"))?;
                    let request_id = context.request_id;
                    let timers = handler(context, request).await?;
                    Ok(CallResponse {
                        request_id,
                        payload: None,
                        error: None,
                        timers: timers.map(crate::timer::encode_update),
                    })
                })
            }),
        );
        Ok(())
    }
}

impl CallHandler for HandlerRegistry {
    fn call(&self, context: CallContext, payload: Payload) -> CallTask<'_> {
        Box::pin(async move {
            let handlers = if context.timer.is_some() {
                &self.timers
            } else {
                &self.handlers
            };
            let handler = handlers
                .get(&payload.message_id)
                .ok_or_else(|| Status::unimplemented("游戏消息尚未注册"))?;
            handler(context, payload.body).await
        })
    }
}

/// 绑定到一个节点业务服务的通用 gRPC 适配。业务名不从 proto 名称推导。
#[derive(Clone)]
pub struct RpcService {
    node_id: String,
    service_name: String,
    handler: Arc<dyn CallHandler>,
}

impl RpcService {
    /// 显式绑定节点和业务服务名，拒绝空身份。
    pub fn new(
        node_id: impl Into<String>,
        service_name: impl Into<String>,
        handler: Arc<dyn CallHandler>,
    ) -> Result<Self, crate::Error> {
        let node_id = node_id.into();
        let service_name = service_name.into();
        if node_id.trim().is_empty() || service_name.trim().is_empty() {
            return Err(crate::Error::Configuration("RPC 服务身份不能为空".into()));
        }
        Ok(Self {
            node_id,
            service_name,
            handler,
        })
    }

    /// 转换为 player 入口，要求调用上下文携带与 bid 匹配的会话身份。
    pub fn into_player(self) -> proto::player_rpc_server::PlayerRpcServer<Self> {
        proto::player_rpc_server::PlayerRpcServer::new(self)
    }
    /// 转换为普通业务入口，不自动附加锁或重试策略。
    pub fn into_business(self) -> proto::business_rpc_server::BusinessRpcServer<Self> {
        proto::business_rpc_server::BusinessRpcServer::new(self)
    }

    async fn invoke(
        &self,
        request: Request<CallRequest>,
        player: bool,
    ) -> Result<Response<CallResponse>, Status> {
        let parent = crate::propagation::extract(request.metadata());
        let request = request.into_inner();
        let context = request
            .context
            .ok_or_else(|| Status::invalid_argument("缺少调用上下文"))?;
        if context.node_id != self.node_id || context.service_name != self.service_name {
            return Err(Status::failed_precondition("调用目标与绑定服务不匹配"));
        }
        let payload = request
            .payload
            .ok_or_else(|| Status::invalid_argument("缺少游戏消息"))?;
        if context.request_id == 0
            || payload.message_id == 0
            || context
                .bid
                .as_ref()
                .is_some_and(|bid| bid.trim().is_empty())
        {
            return Err(Status::invalid_argument("调用序号、消息 ID 或业务 ID 无效"));
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
        let timer = request.timer.map(|timer| TimerTrigger {
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
        let context = CallContext {
            node_id: context.node_id,
            service_name: context.service_name,
            bid: context.bid,
            request_id: context.request_id,
            session,
            timer,
        };
        let span = tracing::info_span!("rpc_server", node_id = %self.node_id, service_name = %self.service_name, request_id = context.request_id, bid = ?context.bid);
        #[expect(
            clippy::let_underscore_must_use,
            reason = "追踪父上下文关联失败不能中断已通过验证的业务调用。"
        )]
        let _ = span.set_parent(parent);
        let started = std::time::Instant::now();
        let request_id = context.request_id;
        let result = self.handler.call(context, payload).instrument(span).await;
        metrics::histogram!("tok_rpc_server_duration_seconds", "service_name" => self.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_server_calls_total", "service_name" => self.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        let mut response = result?;
        response.request_id = request_id;
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl proto::player_rpc_server::PlayerRpc for RpcService {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request, true).await
    }
}
#[tonic::async_trait]
impl proto::business_rpc_server::BusinessRpc for RpcService {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request, false).await
    }
}
