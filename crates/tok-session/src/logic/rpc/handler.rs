use super::CallContext;
use crate::TimerTrigger;
use crate::TimerUpdate;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use tok_execution::{Request as ExecutionRequest, Response, StopReason};
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;
use tower::{Layer as _, Service, ServiceExt};

/// RPC 边界处理结果；通用处理链保留各自业务错误，边界映射为 Status。
pub(crate) type CallTask<'a> =
    Pin<Box<dyn Future<Output = Result<Response<CallResponse>, Status>> + Send + 'a>>;
/// 内部事件执行结果，不含客户端业务回应。
pub(crate) type EventTask<'a> =
    Pin<Box<dyn Future<Output = Result<Response<proto::EventResponse>, Status>> + Send + 'a>>;

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
pub struct HandlerRegistry<
    G: tok_execution::Registry + ?Sized = tok_message::codec::Table<
        u32,
        CallContext,
        Option<TimerUpdate>,
        Status,
    >,
> {
    handlers: tok_message::Registry<CallContext, Option<TimerUpdate>, Status, G>,
    error: Box<dyn Fn(G::DispatchError) -> Status + Send + Sync>,
    timers: BTreeMap<u32, Arc<RegisteredHandler>>,
    events: tok_events::Registry<CallContext, Option<TimerUpdate>, Status>,
}
impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl HandlerRegistry {
    /// 使用默认静态 Table 创建空表；事件处理者总量上限为 65536。
    pub fn new() -> Self {
        Self::from_registry(tok_message::codec::Table::new(), |error| {
            table_status(error, |status| status)
        })
    }
}
impl<G: tok_execution::Registry + ?Sized> HandlerRegistry<G> {
    /// 注入共享自定义消息注册器与其派发错误到 Status 的映射；执行停止由边界统一映射。
    /// 事件和 timer 仍使用各自隔离入口。
    pub fn from_shared_registry(
        registry: Arc<G>,
        error: impl Fn(G::DispatchError) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self {
            handlers: tok_message::Registry::from_shared_registry(registry),
            error: Box::new(error),
            timers: BTreeMap::new(),
            events: tok_events::Registry::default(),
        }
    }
}
impl<G: tok_execution::Registry> HandlerRegistry<G> {
    /// 完成注册，构建消息、事件和 timer 共用的 Tower Service。
    pub fn into_service(self) -> super::RegistryService<G> {
        super::RegistryService::new(Arc::new(self))
    }
    /// 注入自定义消息注册器与派发错误映射，保留游戏调用、事件和 timer 的协议约束。
    pub fn from_registry(
        registry: G,
        error: impl Fn(G::DispatchError) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self::from_shared_registry(Arc::new(registry), error)
    }
}
impl<G> HandlerRegistry<G>
where
    G: tok_execution::Registry<
            Key = u32,
            Input = (CallContext, Vec<u8>),
            Output = tok_message::codec::Reply<
                tok_message::codec::Message<u32>,
                Option<TimerUpdate>,
            >,
            HandlerError = tok_message::codec::Error<Status>,
        > + Clone
        + 'static,
    G::RegistrationError: std::fmt::Display,
{
    /// 使用指定正文编解码器安装请求处理链。
    pub fn register_service_with_codec<Req, Resp, D, A, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        decoder: D,
        encoder: A,
        service: S,
    ) -> Result<(), super::Error>
    where
        Req: Send + 'static,
        Resp: Send + 'static,
        D: tok_core::codec::Decode<Req> + Send + Sync + 'static,
        D::Error: std::error::Error + Send + Sync + 'static,
        A: tok_core::codec::Encode<Resp> + Send + Sync + 'static,
        A::Error: std::error::Error + Send + Sync + 'static,
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
            .register_service_with_codec(
                request_id,
                response_id,
                decoder,
                encoder,
                service.map_response(|response: Response<HandlerReply<Resp>>| {
                    response.map(|reply| tok_message::codec::Reply {
                        message: reply.message,
                        output: reply.timers,
                    })
                }),
            )
            .map_err(|error| super::Error::Configuration(error.to_string()))
    }
}
impl<G: tok_execution::Registry + ?Sized> HandlerRegistry<G> {
    /// 使用指定正文解码器注册服务器事件。
    pub fn register_event_with_codec<E, D, H>(
        &mut self,
        decoder: D,
        handler: H,
    ) -> Result<(), super::Error>
    where
        E: tok_events::Event,
        D: tok_core::codec::Decode<E> + Send + Sync + 'static,
        D::Error: std::error::Error + Send + Sync + 'static,
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
        self.events
            .enable_codec::<E, _>(decoder)
            .map_err(configuration)
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
    /// 使用指定解码器安装 timer 的类型化 Tower 链，保留原上下文及无客户端回应的结果约束。
    pub fn register_timer_service_with_codec<Req, D, S>(
        &mut self,
        message_id: u32,
        decoder: D,
        service: S,
    ) -> Result<(), super::Error>
    where
        Req: Send + 'static,
        D: tok_core::codec::Decode<Req> + Send + Sync + 'static,
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
        let decoder = Arc::new(decoder);
        self.timers.insert(
            message_id,
            Arc::new(move |request| {
                let service = service.clone();
                let decoder = Arc::clone(&decoder);
                Box::pin(async move {
                    let (context, (call, body), extensions) = request.into_parts();
                    let input = decoder
                        .decode(body.as_slice())
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
                        timers: timers.map(crate::logic::rpc::timer::encode_update),
                    }))
                })
            }),
        );
        Ok(())
    }
}
impl<G: ?Sized> HandlerRegistry<G>
where
    G: tok_execution::Registry<
            Key = u32,
            Input = (CallContext, Vec<u8>),
            Output = tok_message::codec::Reply<
                tok_message::codec::Message<u32>,
                Option<TimerUpdate>,
            >,
            HandlerError = tok_message::codec::Error<Status>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    pub(super) fn event(
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
                timers: merge_timers(effects).map(crate::logic::rpc::timer::encode_update),
            }))
        })
    }
    pub(super) fn call(&self, request: ExecutionRequest<(CallContext, Payload)>) -> CallTask<'_> {
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
                        tok_message::codec::Message {
                            key: payload.message_id,
                            body: payload.body,
                        },
                    ),
                    extensions,
                ))
                .await
                .map_err(|error| dispatch_status(error, &self.error))?;
            Ok(reply.map(|reply| CallResponse {
                request_id: id,
                payload: Some(Payload {
                    message_id: reply.message.key,
                    body: reply.message.body,
                }),
                error: None,
                timers: reply.output.map(crate::logic::rpc::timer::encode_update),
            }))
        })
    }
}
pub(super) fn dispatch_status<E>(
    error: tok_execution::Error<E>,
    map_error: impl FnOnce(E) -> Status,
) -> Status {
    match error {
        tok_execution::Error::Inner(error) => map_error(error),
        tok_execution::Error::Stopped(reason) => {
            execution_status(tok_execution::Error::Stopped(reason))
        }
    }
}
pub(super) fn table_status<E>(
    error: tok_execution::registry::DispatchError<u32, tok_message::codec::Error<E>>,
    map_error: impl FnOnce(E) -> Status,
) -> Status {
    use tok_execution::registry::DispatchError;
    match error {
        DispatchError::Unknown(_) => Status::unimplemented("游戏消息尚未注册"),
        DispatchError::Handler(tok_message::codec::Error::Decode(_)) => {
            Status::invalid_argument("游戏消息解码失败")
        }
        DispatchError::Handler(tok_message::codec::Error::Encode(_)) => {
            Status::internal("游戏消息编码失败")
        }
        DispatchError::Handler(tok_message::codec::Error::Handler(error)) => map_error(error),
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
        tok_events::DispatchError::Decode(_) => Status::invalid_argument("事件正文解码失败"),
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
