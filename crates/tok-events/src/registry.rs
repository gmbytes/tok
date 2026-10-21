use crate::{Envelope, Error, Event, EventId, Message, Topic, validate_name};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};
use tok_execution::{Context, Request, StopReason};
use tower::{Layer as _, ServiceExt};

/// 当前处理阶段的结果；转发完成不代表后续 session 业务已完成。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// 没有处理目标。
    Ignored,
    /// 当前处理职责已完成。
    Handled,
    /// 所有目标均拒绝。
    Rejected,
    /// 部分目标完成，部分拒绝。
    Partial,
}
impl Outcome {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ignored => "ignored",
            Self::Handled => "handled",
            Self::Rejected => "rejected",
            Self::Partial => "partial",
        }
    }
    fn combine(results: Vec<Self>) -> Self {
        let handled = results
            .iter()
            .any(|r| matches!(r, Self::Handled | Self::Partial));
        let rejected = results
            .iter()
            .any(|r| matches!(r, Self::Rejected | Self::Partial));
        match (handled, rejected) {
            (true, true) => Self::Partial,
            (true, false) => Self::Handled,
            (false, true) => Self::Rejected,
            _ => Self::Ignored,
        }
    }
}

/// 可借用当前状态和事件的异步处理任务，不隐式 spawn。
pub type Task<'a, Output = Outcome, Failure = Error> =
    Pin<Box<dyn Future<Output = Result<Output, Failure>> + Send + 'a>>;

/// 服务器订阅适配器；只处理远程接收，不决定业务注册或编码方式。
pub trait Handler: Send + Sync {
    /// 已安装远程解码器的稳定事件名。
    fn event_types(&self) -> Vec<&str>;
    /// 处理可信入口建立的上下文及原始编码；不向发布者返回业务结果。
    fn handle(&self, request: Request<Arc<Message>>) -> Task<'_>;
}

/// 统一事件处理输入；直接借用状态和强类型封装，不编码、不入队。
pub struct Input<'a, State, E> {
    /// 当前调用方独占借用的状态。
    pub state: &'a mut State,
    /// 本地与远程使用相同事件封装。
    pub event: &'a Envelope<E>,
}

/// 分发失败；业务错误保留原类型，协议、注册与执行停止分别表达。
#[derive(Debug, thiserror::Error)]
pub enum DispatchError<F> {
    /// 事件参数、解码或类型注册失败。
    #[error(transparent)]
    Invalid(Error),
    /// 没有匹配的事件注册或远程解码器。
    #[error("事件尚未注册或未安装解码器")]
    Unknown,
    /// 上下文取消或预算耗尽。
    #[error(transparent)]
    Stopped(StopReason),
    /// 处理者业务错误。
    #[error("事件处理失败：{0}")]
    Handler(F),
}
impl<F> From<tok_execution::Error<F>> for DispatchError<F> {
    fn from(error: tok_execution::Error<F>) -> Self {
        match error {
            tok_execution::Error::Stopped(reason) => Self::Stopped(reason),
            tok_execution::Error::Inner(error) => Self::Handler(error),
        }
    }
}

type Erased = dyn Any + Send + Sync;
type Decoder = fn(&Message) -> Result<Box<Erased>, Error>;
trait Route<C, O, F>: Send + Sync {
    fn call<'a>(
        &'a self,
        state: &'a mut C,
        context: Context,
        event: &'a Erased,
    ) -> Task<'a, O, DispatchError<F>>;
}
struct TypedRoute<E, S> {
    service: S,
    marker: std::marker::PhantomData<fn(E)>,
}
impl<C: Send, O, F, E: Event, S> Route<C, O, F> for TypedRoute<E, S>
where
    S: Clone + Send + Sync + 'static,
    for<'a> S: tower::Service<Request<Input<'a, C, E>>, Response = O, Error = F>,
    for<'a> <S as tower::Service<Request<Input<'a, C, E>>>>::Future: Send,
{
    fn call<'a>(
        &'a self,
        state: &'a mut C,
        context: Context,
        event: &'a Erased,
    ) -> Task<'a, O, DispatchError<F>> {
        Box::pin(async move {
            let event = event
                .downcast_ref::<Envelope<E>>()
                .ok_or(DispatchError::Invalid(Error::InvalidInput(
                    "事件 Rust 类型与注册不匹配",
                )))?;
            tok_execution::Layer
                .layer(self.service.clone())
                .oneshot(Request::new(context, Input { state, event }))
                .await
                .map_err(Into::into)
        })
    }
}
struct Entry<C, O, F> {
    type_id: TypeId,
    decoder: Option<Decoder>,
    handlers: Vec<Box<dyn Route<C, O, F>>>,
}

/// 多类型、有序多处理者的统一注册表；本地分发和远程接收执行同一处理链。
///
/// 注册容量按处理者总数计。只有显式安装解码器的类型才可远程接收。
/// 状态由调用方借用，串行或互斥访问由调用方保障；首错停止，无重试或回滚。
/// Default 使用 65536 个处理者的容量，new 可设置更小上限。
pub struct Registry<C = (), O = Outcome, F = Error> {
    capacity: usize,
    count: usize,
    entries: BTreeMap<String, Entry<C, O, F>>,
}
impl<C, O, F> Default for Registry<C, O, F> {
    fn default() -> Self {
        Self {
            capacity: 65_536,
            count: 0,
            entries: BTreeMap::new(),
        }
    }
}
impl<C: Send, O, F> Registry<C, O, F> {
    /// 创建有界空表，不连接网络或创建任务。
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            count: 0,
            entries: BTreeMap::new(),
        })
    }
    /// 注册借用状态的处理者；需要中间件时用 register_service 安装 Tower 链。
    pub fn register<E, H>(&mut self, handler: H) -> Result<(), Error>
    where
        E: Event,
        H: for<'a> Fn(Request<Input<'a, C, E>>) -> Task<'a, O, F> + Send + Sync + 'static,
    {
        self.register_service::<E, _>(Callback(Arc::new(handler)))
    }
    /// 按顺序安装类型化 Tower Service；相同事件可有多个处理者，共享原期限。
    pub fn register_service<E, S>(&mut self, service: S) -> Result<(), Error>
    where
        E: Event,
        S: Clone + Send + Sync + 'static,
        for<'a> S: tower::Service<Request<Input<'a, C, E>>, Response = O, Error = F>,
        for<'a> <S as tower::Service<Request<Input<'a, C, E>>>>::Future: Send,
    {
        validate_name(E::NAME)?;
        if self
            .entries
            .get(E::NAME)
            .is_some_and(|entry| entry.type_id != TypeId::of::<E>())
        {
            return Err(Error::InvalidInput("同名事件已绑定其他 Rust 类型"));
        }
        if self.count >= self.capacity {
            return Err(Error::CapacityExceeded);
        }
        let entry = self.entries.entry(E::NAME.into()).or_insert_with(|| Entry {
            type_id: TypeId::of::<E>(),
            decoder: None,
            handlers: Vec::new(),
        });
        entry.handlers.push(Box::new(TypedRoute::<E, S> {
            service,
            marker: std::marker::PhantomData,
        }));
        self.count += 1;
        Ok(())
    }
    /// 为已注册类型安装 protobuf 解码适配；本地事件不需要 prost 或此操作。
    pub fn enable_protobuf<E: Event + prost::Message + Default>(&mut self) -> Result<(), Error> {
        let entry = self
            .entries
            .get_mut(E::NAME)
            .filter(|entry| entry.type_id == TypeId::of::<E>())
            .ok_or(Error::InvalidInput("先注册事件处理者再安装解码器"))?;
        entry.decoder = Some(|message| {
            let payload = E::decode(message.body.as_slice())
                .map_err(|_| Error::InvalidInput("事件正文解码失败"))?;
            let envelope = Envelope {
                topic: message.topic.clone(),
                event_id: message.event_id.clone(),
                payload,
                trace_context: message.trace_context.clone(),
            };
            envelope.validate()?;
            Ok(Box::new(envelope))
        });
        Ok(())
    }
    /// 在当前任务直接分发强类型事件；没有订阅者返回空结果，不产生客户端回应。
    pub async fn dispatch<E: Event>(
        &self,
        state: &mut C,
        request: Request<&Envelope<E>>,
    ) -> Result<Vec<O>, DispatchError<F>> {
        request.input.validate().map_err(DispatchError::Invalid)?;
        let mut context = event_context(
            &request.context,
            E::NAME,
            &request.input.event_id,
            &request.input.topic,
        )?;
        drop(context.extensions_mut().remove::<Arc<Message>>());
        let Some(entry) = self.entries.get(E::NAME) else {
            context.check().map_err(DispatchError::Stopped)?;
            return Ok(Vec::new());
        };
        if entry.type_id != TypeId::of::<E>() {
            return Err(DispatchError::Invalid(Error::InvalidInput(
                "事件 Rust 类型与注册不匹配",
            )));
        }
        Self::execute(entry, state, context, request.input).await
    }
    /// 解码一次后执行同一注册链；保留原 `Arc<Message>` 于扩展中，转发不得重新编码。
    pub async fn receive(
        &self,
        state: &mut C,
        request: Request<Arc<Message>>,
    ) -> Result<Vec<O>, DispatchError<F>> {
        request
            .input
            .validate(1_048_576)
            .map_err(DispatchError::Invalid)?;
        let mut context = event_context(
            &request.context,
            &request.input.event_type,
            &request.input.event_id,
            &request.input.topic,
        )?;
        context.check().map_err(DispatchError::Stopped)?;
        let entry = self
            .entries
            .get(&request.input.event_type)
            .ok_or(DispatchError::Unknown)?;
        let decoder = entry.decoder.ok_or(DispatchError::Unknown)?;
        let event = decoder(&request.input).map_err(DispatchError::Invalid)?;
        context.extensions_mut().insert(request.input);
        Self::execute(entry, state, context, event.as_ref()).await
    }
    async fn execute(
        entry: &Entry<C, O, F>,
        state: &mut C,
        context: Context,
        event: &Erased,
    ) -> Result<Vec<O>, DispatchError<F>> {
        let mut results = Vec::with_capacity(entry.handlers.len());
        for handler in &entry.handlers {
            results.push(handler.call(state, context.clone(), event).await?);
        }
        Ok(results)
    }
}
fn event_context<F>(
    parent: &Context,
    name: &str,
    id: &EventId,
    topic: &Topic,
) -> Result<Context, DispatchError<F>> {
    let mut context = parent
        .child(format!("event/{name}"))
        .map_err(|_| DispatchError::Invalid(Error::InvalidInput("事件执行上下文无效")))?;
    context.extensions_mut().insert(id.clone());
    context.extensions_mut().insert(topic.clone());
    Ok(context)
}
impl Handler for Registry {
    fn event_types(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.decoder.is_some())
            .map(|(name, _)| name.as_str())
            .collect()
    }
    fn handle(&self, request: Request<Arc<Message>>) -> Task<'_> {
        Box::pin(async move {
            match self.receive(&mut (), request).await {
                Ok(results) => Ok(Outcome::combine(results)),
                Err(DispatchError::Unknown) => Ok(Outcome::Ignored),
                Err(DispatchError::Invalid(error) | DispatchError::Handler(error)) => Err(error),
                Err(DispatchError::Stopped(reason)) => Err(Error::Stopped(reason)),
            }
        })
    }
}

/// 将借用输入的异步回调适配成 Tower Service；可继续安装通用或路由级 Layer。
pub fn handler<C, E, O, F, H>(callback: H) -> Callback<H>
where
    H: for<'a> Fn(Request<Input<'a, C, E>>) -> Task<'a, O, F>,
{
    Callback(Arc::new(callback))
}

/// 持有可共享回调的类型化 Tower 末端处理者。
pub struct Callback<H>(Arc<H>);
impl<H> Clone for Callback<H> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
impl<'a, C, E, O, F, H> tower::Service<Request<Input<'a, C, E>>> for Callback<H>
where
    H: Fn(Request<Input<'a, C, E>>) -> Task<'a, O, F>,
{
    type Response = O;
    type Error = F;
    type Future = Task<'a, O, F>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), F>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Input<'a, C, E>>) -> Self::Future {
        (self.0)(request)
    }
}
