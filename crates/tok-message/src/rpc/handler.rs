use super::CallContext;
use prost::Message;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use tok_execution::{Request as ExecutionRequest, Response, StopReason};
use tok_player::TimerTrigger;
use tok_player::TimerUpdate;
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;
use tower::{Layer as _, Service, ServiceExt};

/// RPC 边界处理结果；通用处理链保留各自业务错误，边界映射为 Status。
pub type CallTask<'a> =
    Pin<Box<dyn Future<Output = Result<Response<CallResponse>, Status>> + Send + 'a>>;
/// 内部事件执行结果，不含客户端业务回应。
pub type EventTask<'a> =
    Pin<Box<dyn Future<Output = Result<Response<proto::EventResponse>, Status>> + Send + 'a>>;

/// 游戏协议处理入口；请求、timer 与服务器事件保持各自语义和结果类型。
///
/// 这是上层游戏协议契约，不属于通用 RPC 设施。
pub trait Handler: Send + Sync {
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
impl<S> Handler for TowerHandler<S>
where
    S: Service<
            ExecutionRequest<(CallContext, Payload)>,
            Response = Response<CallResponse>,
            Error = Status,
        > + Clone
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
    handlers: crate::Registry<CallContext, Option<TimerUpdate>, Status>,
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
            handlers: crate::Registry::new(),
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
    ) -> Result<(), super::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(ExecutionRequest<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
    {
        self.register_with_timers(request_id, response_id, move |request| {
            let future = handler(request);
            async move {
                Ok(future.await?.map(|message| HandlerReply {
                    message,
                    timers: None,
                }))
            }
        })
    }
    /// 注册可返回 timer 更新的回调，保留执行上下文；需要组合中间件时使用 register_service。
    pub fn register_with_timers<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), super::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(ExecutionRequest<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<HandlerReply<Resp>>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_id,
            response_id,
            tower::service_fn(move |request: ExecutionRequest<(CallContext, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
    /// 安装请求路由的类型化 Tower 链；与事件、timer 使用同一 Request 和 Layer 协议。
    pub fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), super::Error>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        S: Service<
                ExecutionRequest<(CallContext, Req)>,
                Response = Response<HandlerReply<Resp>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.handlers
            .register_service(
                request_id,
                response_id,
                service.map_response(|response: Response<HandlerReply<Resp>>| {
                    response.map(|reply| tok_protobuf::Reply {
                        message: reply.message,
                        output: reply.timers,
                    })
                }),
            )
            .map_err(|error| super::Error::Configuration(error.to_string()))
    }
    /// 安装服务器事件的共同注册链；多个处理者按序执行，timer 更新依次合成。
    pub fn register_event<E, H>(&mut self, handler: H) -> Result<(), super::Error>
    where
        E: tok_events::Event + Message + Default,
        H: for<'a> Fn(
                ExecutionRequest<tok_events::Input<'a, CallContext, E>>,
            ) -> tok_events::Task<'a, Response<Option<TimerUpdate>>, Status>
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
    ) -> Result<(), super::Error>
    where
        Req: Message + Default + Send + 'static,
        F: Fn(ExecutionRequest<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Option<TimerUpdate>>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_timer_service(
            message_id,
            tower::service_fn(move |request: ExecutionRequest<(CallContext, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
    /// 安装 timer 的类型化 Tower 链，保留原上下文及无客户端回应的结果约束。
    pub fn register_timer_service<Req, S>(
        &mut self,
        message_id: u32,
        service: S,
    ) -> Result<(), super::Error>
    where
        Req: Message + Default + Send + 'static,
        S: Service<
                ExecutionRequest<(CallContext, Req)>,
                Response = Response<Option<TimerUpdate>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        if message_id == 0 || self.timers.contains_key(&message_id) {
            return Err(super::Error::Configuration(
                "计时消息 ID 必须非零且只能注册一次".into(),
            ));
        }
        self.timers.insert(
            message_id,
            Arc::new(move |request| {
                let service = service.clone();
                Box::pin(async move {
                    let (context, (call, body), extensions) = request.into_parts();
                    let input = Req::decode(body.as_slice())
                        .map_err(|_| Status::invalid_argument("计时参数解码失败"))?;
                    let id = call.request_id;
                    let timers = tok_execution::Layer
                        .layer(service)
                        .oneshot(ExecutionRequest::from_parts(
                            context,
                            (call, input),
                            extensions,
                        ))
                        .await
                        .map_err(execution_status)?;
                    Ok(timers.map(|timers| CallResponse {
                        request_id: id,
                        payload: None,
                        error: None,
                        timers: timers.map(super::timer::encode_update),
                    }))
                })
            }),
        );
        Ok(())
    }
}
impl Handler for HandlerRegistry {
    fn event(
        &self,
        request: ExecutionRequest<(CallContext, Arc<tok_events::Message>)>,
    ) -> EventTask<'_> {
        Box::pin(async move {
            let (context, (mut call, event), mut extensions) = request.into_parts();
            if call.timer.is_some() {
                return Err(Status::invalid_argument("timer 不能进入事件入口"));
            }
            let id = call.request_id;
            extensions.insert(call.clone());
            let effects = self
                .events
                .receive(
                    &mut call,
                    ExecutionRequest::from_parts(context, event, extensions),
                )
                .await
                .map_err(event_status)?;
            Ok(effects.map(|effects| proto::EventResponse {
                request_id: id,
                error: None,
                timers: merge_timers(effects).map(super::timer::encode_update),
            }))
        })
    }
    fn call(&self, request: ExecutionRequest<(CallContext, Payload)>) -> CallTask<'_> {
        Box::pin(async move {
            let (request_context, (call, payload), mut extensions) = request.into_parts();
            extensions.insert(call.clone());
            if let Some(trigger) = &call.timer {
                extensions.insert(trigger.clone());
            } else {
                drop(extensions.remove::<TimerTrigger>());
            }
            let timer = call.timer.is_some();
            let context = request_context
                .child(format!(
                    "{}/{}",
                    if timer { "timer" } else { "rpc" },
                    payload.message_id
                ))
                .map_err(|_| Status::invalid_argument("调用执行上下文无效"))?;
            if timer {
                let handler = self
                    .timers
                    .get(&payload.message_id)
                    .ok_or_else(|| Status::unimplemented("游戏消息尚未注册"))?;
                return handler(ExecutionRequest::from_parts(
                    context,
                    (call, payload.body),
                    extensions,
                ))
                .await;
            }
            let id = call.request_id;
            let reply = self
                .handlers
                .dispatch(ExecutionRequest::from_parts(
                    context,
                    (
                        call,
                        tok_protobuf::Message {
                            key: payload.message_id,
                            body: payload.body,
                        },
                    ),
                    extensions,
                ))
                .await
                .map_err(|error| dispatch_status(error, |error| error))?;
            Ok(reply.map(|reply| CallResponse {
                request_id: id,
                payload: Some(Payload {
                    message_id: reply.message.key,
                    body: reply.message.body,
                }),
                error: None,
                timers: reply.output.map(super::timer::encode_update),
            }))
        })
    }
}
pub(super) fn dispatch_status<E>(
    error: tok_execution::registry::DispatchError<u32, tok_protobuf::Error<E>>,
    map_error: impl FnOnce(E) -> Status,
) -> Status {
    use tok_execution::registry::DispatchError;
    match error {
        DispatchError::Unknown(_) => Status::unimplemented("游戏消息尚未注册"),
        DispatchError::Stopped(reason) => execution_status(tok_execution::Error::Stopped(reason)),
        DispatchError::Handler(tok_protobuf::Error::Decode(_)) => {
            Status::invalid_argument("游戏消息解码失败")
        }
        DispatchError::Handler(tok_protobuf::Error::Handler(error)) => map_error(error),
    }
}

fn configuration(error: tok_events::Error) -> super::Error {
    super::Error::Configuration(error.to_string())
}
fn execution_status(error: tok_execution::Error<Status>) -> Status {
    match error {
        tok_execution::Error::Inner(error) => error,
        tok_execution::Error::Stopped(StopReason::Cancelled) => Status::cancelled("调用已取消"),
        tok_execution::Error::Stopped(StopReason::DeadlineExceeded) => {
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
            execution_status(tok_execution::Error::Stopped(reason))
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
