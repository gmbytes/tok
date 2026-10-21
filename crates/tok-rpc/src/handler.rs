use crate::CallContext;
use prost::Message;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use tok_core::{
    TimerTrigger,
    execution::{Context, Source, StopReason},
    service::Identity,
};
use tok_player::{SessionIdentity, TimerUpdate};
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse, Payload};
use tok_service::Request as ExecutionRequest;
use tonic::{Request, Response, Status};
use tower::{Layer as _, Service, ServiceExt};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// RPC 边界处理结果；通用处理链保留各自业务错误，边界映射为 Status。
pub type CallTask<'a> = Pin<Box<dyn Future<Output = Result<CallResponse, Status>> + Send + 'a>>;
/// 内部事件执行结果，不含客户端业务回应。
pub type EventTask<'a> =
    Pin<Box<dyn Future<Output = Result<proto::EventResponse, Status>> + Send + 'a>>;

/// RPC 接入适配；执行上下文由可信入口建立，消息和事件各自保持结果类型。
pub trait CallHandler: Send + Sync {
    /// 处理已校验的调用，不重置预算或自行入队。
    fn call(&self, request: ExecutionRequest<(CallContext, Payload)>) -> CallTask<'_>;
    /// 处理独立的服务器事件，不进入请求或 timer 分发表。
    fn event(
        &self,
        _request: ExecutionRequest<(CallContext, Arc<tok_events::Message>)>,
    ) -> EventTask<'_> {
        Box::pin(async { Err(Status::unimplemented("内部事件入口尚未实现")) })
    }
}

/// 将已安装中间件的上下文 Tower Service 适配为 RPC handler，不添加重试或默认锁。
pub struct TowerHandler<S>(
    /// 克隆应共享所需并发或限流状态的 Service。
    pub S,
);
impl<S> CallHandler for TowerHandler<S>
where
    S: Service<ExecutionRequest<(CallContext, Payload)>, Response = CallResponse, Error = Status>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    fn call(&self, request: ExecutionRequest<(CallContext, Payload)>) -> CallTask<'_> {
        Box::pin(self.0.clone().oneshot(request))
    }
}
type RegisteredHandler =
    dyn Fn(ExecutionRequest<(CallContext, Vec<u8>)>) -> CallTask<'static> + Send + Sync;

/// 强类型业务回应与仅供 session 消费的计时更新。
pub struct HandlerReply<M> {
    /// 编码给调用方的业务回应。
    pub message: M,
    /// None 保持已有计时集合，应用成功后才发送回应。
    pub timers: Option<TimerUpdate>,
}

/// 消息与 timer 的编解码路由；服务器事件委托统一事件 Registry。
pub struct HandlerRegistry {
    handlers: BTreeMap<u32, Arc<RegisteredHandler>>,
    timers: BTreeMap<u32, Arc<RegisteredHandler>>,
    events: tok_events::Registry<CallContext, Option<TimerUpdate>, Status>,
}
impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl HandlerRegistry {
    /// 创建空表；事件处理者总量上限为 65536，不按玩家分配注册项。
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
            timers: BTreeMap::new(),
            events: tok_events::Registry::default(),
        }
    }
    /// 注册 protobuf 请求与对应回应；同步于当前执行范围，不隐式重试。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, Status>> + Send + 'static,
    {
        self.register_with_timers(request_id, response_id, move |context, input| {
            let future = handler(context, input);
            async move {
                Ok(HandlerReply {
                    message: future.await?,
                    timers: None,
                })
            }
        })
    }
    /// 注册可返回 timer 更新的回调；需要执行元信息或中间件时使用 register_service。
    pub fn register_with_timers<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HandlerReply<Resp>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_id,
            response_id,
            tower::service_fn(move |request: ExecutionRequest<(CallContext, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request.input.0, request.input.1).await }
            }),
        )
    }
    /// 安装请求路由的类型化 Tower 链；与事件、timer 使用同一 Request 和 Layer 协议。
    pub fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        S: Service<
                ExecutionRequest<(CallContext, Req)>,
                Response = HandlerReply<Resp>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        if request_id == 0 || response_id == 0 || self.handlers.contains_key(&request_id) {
            return Err(crate::Error::Configuration(
                "消息 ID 必须非零且请求只能注册一次".into(),
            ));
        }
        self.handlers.insert(
            request_id,
            Arc::new(move |request| {
                let service = service.clone();
                Box::pin(async move {
                    let (call, body) = request.input;
                    let input = Req::decode(body.as_slice())
                        .map_err(|_| Status::invalid_argument("游戏消息解码失败"))?;
                    let id = call.request_id;
                    let response = tok_service::Layer
                        .layer(service)
                        .oneshot(ExecutionRequest::new(request.context, (call, input)))
                        .await
                        .map_err(execution_status)?;
                    Ok(CallResponse {
                        request_id: id,
                        payload: Some(Payload {
                            message_id: response_id,
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
    /// 安装服务器事件的共同注册链；多个处理者按序执行，timer 更新依次合成。
    pub fn register_event<E, H>(&mut self, handler: H) -> Result<(), crate::Error>
    where
        E: tok_events::Event + Message + Default,
        H: for<'a> Fn(
                ExecutionRequest<tok_events::Input<'a, CallContext, E>>,
            ) -> tok_events::Task<'a, Option<TimerUpdate>, Status>
            + Send
            + Sync
            + 'static,
    {
        self.events
            .register::<E, _>(handler)
            .map_err(configuration)?;
        self.events.enable_protobuf::<E>().map_err(configuration)
    }
    /// 读取同一事件表，在当前调用中直接分发本地事件。
    pub fn events(&self) -> &tok_events::Registry<CallContext, Option<TimerUpdate>, Status> {
        &self.events
    }
    /// 组装期间安装事件处理者、路由级 Tower 链及解码适配。
    pub fn events_mut(
        &mut self,
    ) -> &mut tok_events::Registry<CallContext, Option<TimerUpdate>, Status> {
        &mut self.events
    }
    /// 注册内部 timer 回调，客户端业务请求不进入此表。
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
        let handler = Arc::new(handler);
        self.register_timer_service(
            message_id,
            tower::service_fn(move |request: ExecutionRequest<(CallContext, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request.input.0, request.input.1).await }
            }),
        )
    }
    /// 安装 timer 的类型化 Tower 链，保留原上下文及无客户端回应的结果约束。
    pub fn register_timer_service<Req, S>(
        &mut self,
        message_id: u32,
        service: S,
    ) -> Result<(), crate::Error>
    where
        Req: Message + Default + Send + 'static,
        S: Service<
                ExecutionRequest<(CallContext, Req)>,
                Response = Option<TimerUpdate>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        if message_id == 0 || self.timers.contains_key(&message_id) {
            return Err(crate::Error::Configuration(
                "计时消息 ID 必须非零且只能注册一次".into(),
            ));
        }
        self.timers.insert(
            message_id,
            Arc::new(move |request| {
                let service = service.clone();
                Box::pin(async move {
                    let (call, body) = request.input;
                    let input = Req::decode(body.as_slice())
                        .map_err(|_| Status::invalid_argument("计时参数解码失败"))?;
                    let id = call.request_id;
                    let timers = tok_service::Layer
                        .layer(service)
                        .oneshot(ExecutionRequest::new(request.context, (call, input)))
                        .await
                        .map_err(execution_status)?;
                    Ok(CallResponse {
                        request_id: id,
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
    fn event(
        &self,
        request: ExecutionRequest<(CallContext, Arc<tok_events::Message>)>,
    ) -> EventTask<'_> {
        Box::pin(async move {
            let (mut call, event) = request.input;
            if call.timer.is_some() {
                return Err(Status::invalid_argument("timer 不能进入事件入口"));
            }
            let id = call.request_id;
            let effects = self
                .events
                .receive(&mut call, ExecutionRequest::new(request.context, event))
                .await
                .map_err(event_status)?;
            Ok(proto::EventResponse {
                request_id: id,
                error: None,
                timers: merge_timers(effects).map(crate::timer::encode_update),
            })
        })
    }
    fn call(&self, request: ExecutionRequest<(CallContext, Payload)>) -> CallTask<'_> {
        Box::pin(async move {
            let (call, payload) = request.input;
            let timer = call.timer.is_some();
            let table = if timer { &self.timers } else { &self.handlers };
            let handler = table
                .get(&payload.message_id)
                .ok_or_else(|| Status::unimplemented("游戏消息尚未注册"))?;
            let context = request
                .context
                .child(
                    format!(
                        "{}/{}",
                        if timer { "timer" } else { "rpc" },
                        payload.message_id
                    ),
                    request.context.source().clone(),
                )
                .map_err(|_| Status::invalid_argument("调用执行上下文无效"))?;
            handler(ExecutionRequest::new(context, (call, payload.body))).await
        })
    }
}
fn configuration(error: tok_events::Error) -> crate::Error {
    crate::Error::Configuration(error.to_string())
}
fn execution_status(error: tok_service::Error<Status>) -> Status {
    match error {
        tok_service::Error::Inner(error) => error,
        tok_service::Error::Stopped(StopReason::Cancelled) => Status::cancelled("调用已取消"),
        tok_service::Error::Stopped(StopReason::DeadlineExceeded) => {
            Status::deadline_exceeded("调用预算已耗尽")
        }
    }
}
fn event_status(error: tok_events::DispatchError<Status>) -> Status {
    match error {
        tok_events::DispatchError::Handler(error) => error,
        tok_events::DispatchError::Invalid(error) => Status::invalid_argument(error.to_string()),
        tok_events::DispatchError::Unknown => Status::unimplemented("事件尚未注册"),
        tok_events::DispatchError::Stopped(reason) => {
            execution_status(tok_service::Error::Stopped(reason))
        }
    }
}
// 每次更新按注册顺序生效；Patch 保留原集合语义，ReplaceAll 覆盖之前的全部更新。
fn merge_timers(effects: Vec<Option<TimerUpdate>>) -> Option<TimerUpdate> {
    let mut result = None;
    for effect in effects.into_iter().flatten() {
        result = Some(match (result, effect) {
            (_, TimerUpdate::ReplaceAll(entries)) => TimerUpdate::ReplaceAll(entries),
            (None, patch) => patch,
            (Some(previous), TimerUpdate::Patch { upsert, cancel }) => {
                let (replace, mut entries, mut removed) = match previous {
                    TimerUpdate::ReplaceAll(entries) => (
                        true,
                        entries
                            .into_iter()
                            .map(|v| (v.timer_id.clone(), v))
                            .collect::<BTreeMap<_, _>>(),
                        std::collections::BTreeSet::new(),
                    ),
                    TimerUpdate::Patch { upsert, cancel } => (
                        false,
                        upsert
                            .into_iter()
                            .map(|v| (v.timer_id.clone(), v))
                            .collect(),
                        cancel.into_iter().collect(),
                    ),
                };
                for id in cancel {
                    entries.remove(&id);
                    removed.insert(id);
                }
                for entry in upsert {
                    removed.remove(&entry.timer_id);
                    entries.insert(entry.timer_id.clone(), entry);
                }
                if replace {
                    TimerUpdate::ReplaceAll(entries.into_values().collect())
                } else {
                    TimerUpdate::Patch {
                        upsert: entries.into_values().collect(),
                        cancel: removed.into_iter().collect(),
                    }
                }
            }
        });
    }
    result
}

/// 绑定到一个节点业务服务的通用 gRPC 适配。业务名不从 proto 名称推导。
#[derive(Clone)]
pub struct RpcService {
    node_id: String,
    service_name: String,
    handler: Arc<dyn CallHandler>,
    budget: Duration,
    cancellation: tokio_util::sync::CancellationToken,
    extensions: tok_core::execution::Extensions,
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
            budget: Duration::from_secs(30),
            cancellation: tokio_util::sync::CancellationToken::new(),
            extensions: tok_core::execution::Extensions::default(),
        })
    }

    /// 创建尚未安装 handler 的入口及绑定槽；工厂通过 Binding::install 安装实际处理链。
    pub fn deferred(
        node_id: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Result<(Self, crate::Binding), crate::Error> {
        let identity = Identity {
            node_id: node_id.into(),
            service_name: service_name.into(),
        };
        let binding = crate::Binding::new(identity.clone());
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
        extensions: tok_core::execution::Extensions,
    ) -> Result<Self, crate::Error> {
        if budget.is_zero() || budget > Duration::from_secs(86_400) {
            return Err(crate::Error::Configuration(
                "RPC 执行预算须大于零且不超过一天".into(),
            ));
        }
        self.budget = budget;
        self.cancellation = cancellation;
        self.extensions = extensions;
        Ok(self)
    }
    fn execution(
        &self,
        call: &CallContext,
        source: Source,
        budget: Duration,
        operation: String,
    ) -> Result<Context, Status> {
        let mut context = Context::new(
            Identity {
                node_id: self.node_id.clone(),
                service_name: self.service_name.clone(),
            },
            operation,
            source,
            budget,
            self.cancellation.child_token(),
        )
        .map_err(|_| Status::invalid_argument("调用执行上下文无效"))?;
        *context.extensions_mut() = self.extensions.clone();
        context.extensions_mut().insert(call.clone());
        Ok(context)
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

    async fn execute<T, R, S>(
        &self,
        parent: opentelemetry::Context,
        request_id: u64,
        bid: Option<String>,
        mut request: ExecutionRequest<T>,
        service: S,
    ) -> Result<R, Status>
    where
        S: Service<ExecutionRequest<T>, Response = R, Error = Status> + Clone,
    {
        let span = tracing::info_span!("rpc_server",node_id=%self.node_id,service_name=%self.service_name,request_id,bid=?bid);
        #[expect(
            clippy::let_underscore_must_use,
            reason = "追踪关联失败不能中断有效业务调用。"
        )]
        let _ = span.set_parent(parent);
        request.context = request.context.with_span(span);
        let started = std::time::Instant::now();
        let result = tok_service::Layer
            .layer(service)
            .oneshot(request)
            .await
            .map_err(execution_status);
        metrics::histogram!("tok_rpc_server_duration_seconds","service_name"=>self.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_server_calls_total","service_name"=>self.service_name.clone(),"result"=>if result.is_ok(){"ok"}else{"error"}).increment(1);
        result
    }

    async fn invoke(
        &self,
        request: Request<CallRequest>,
        player: bool,
    ) -> Result<Response<CallResponse>, Status> {
        let parent = crate::propagation::extract(request.metadata());
        let budget = request_budget(request.metadata(), self.budget)?;
        let request = request.into_inner();
        let context = self.context(request.context, request.timer, player)?;
        let payload = request
            .payload
            .ok_or_else(|| Status::invalid_argument("缺少游戏消息"))?;
        if payload.message_id == 0 {
            return Err(Status::invalid_argument("消息 ID 无效"));
        }
        let request_id = context.request_id;
        let source = context
            .timer
            .clone()
            .map_or(Source::Rpc { request_id }, Source::Timer);
        let operation = format!(
            "{}/{}",
            if context.timer.is_some() {
                "timer"
            } else {
                "rpc"
            },
            payload.message_id
        );
        let execution = self.execution(&context, source, budget, operation)?;
        let handler = Arc::clone(&self.handler);
        let mut response = self
            .execute(
                parent,
                request_id,
                context.bid.clone(),
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
impl proto::player_rpc_server::PlayerRpc for RpcService {
    async fn handle_event(
        &self,
        request: Request<proto::EventRequest>,
    ) -> Result<Response<proto::EventResponse>, Status> {
        let parent = crate::propagation::extract(request.metadata());
        let budget = request_budget(request.metadata(), self.budget)?;
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
        let execution = self.execution(
            &context,
            Source::Event {
                event_id: event.event_id.to_string(),
            },
            budget,
            format!("event/{}", event.event_type),
        )?;
        let handler = Arc::clone(&self.handler);
        let mut response = self
            .execute(
                parent,
                request_id,
                context.bid.clone(),
                ExecutionRequest::new(execution, (context, Arc::new(event))),
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
impl proto::business_rpc_server::BusinessRpc for RpcService {
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        self.invoke(request, false).await
    }
}

fn request_budget(
    metadata: &tonic::metadata::MetadataMap,
    limit: Duration,
) -> Result<Duration, Status> {
    let Some(value) = metadata.get("grpc-timeout") else {
        return Ok(limit);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("RPC 超时格式无效"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Status::invalid_argument("RPC 超时格式无效"));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("RPC 超时格式无效"));
    }
    let count = digits
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("RPC 超时格式无效"))?;
    let duration = match unit {
        "H" => Duration::from_secs(count * 3600),
        "M" => Duration::from_secs(count * 60),
        "S" => Duration::from_secs(count),
        "m" => Duration::from_millis(count),
        "u" => Duration::from_micros(count),
        "n" => Duration::from_nanos(count),
        _ => return Err(Status::invalid_argument("RPC 超时单位无效")),
    };
    Ok(duration.min(limit))
}
