//! protobuf 正文编解码适配；分发键 K 由调用方定义，不包含 RPC 信封。
//! Registry 可直接作为 Tower Service，公共策略包住分发表，路由策略保留在注册项内。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use std::{future::Future, sync::Arc};
use tok_execution::registry;
use tok_execution::{Request, Response};
use tower::{Service, ServiceExt};

/// 已编码的正文与调用方定义的消息键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<K> {
    /// 分发或回应键，合法性由协议边界判断。
    pub key: K,
    /// protobuf 编码正文。
    pub body: Vec<u8>,
}
/// 强类型或已编码的回应与调用方定义的附加结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply<M, O = ()> {
    /// 回应消息。
    pub message: M,
    /// 交给调用方消费的附加结果。
    pub output: O,
}
/// 正文编解码适配错误；业务错误仍保留原类型。
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// 正文不符合注册的请求类型。
    #[error("消息正文解码失败：{0}")]
    Decode(#[source] prost::DecodeError),
    /// 处理链返回的业务错误。
    #[error("消息处理失败：{0}")]
    Handler(#[source] E),
}

/// protobuf 注册器使用的默认静态底层表；存储解码前的正文处理链。
pub type Table<K, C = (), O = (), E = std::convert::Infallible> =
    tok_execution::Table<K, (C, Vec<u8>), Reply<Message<K>, O>, Error<E>>;

/// 在通用分发表上增加 protobuf 编解码，C 是调用状态，O 是附加结果，E 是业务错误。
///
/// K 可以是数字、字符串或枚举。本适配器不规定零或空字符串是否有效，
/// 编解码、内层就绪等待和业务执行均在原 Context 约束内，不创建后台任务。
/// 可直接作为接受 `Request<(C, Message<K>)>` 的 Tower Service，供公共 Layer 包装。
/// poll_ready 不预留目标路由容量；默认 Table 使用注册快照，自定义实现自行定义更新可见性。
pub struct Registry<K, C = (), O = (), E = std::convert::Infallible, G: ?Sized = Table<K, C, O, E>>
{
    inner: Arc<G>,
    marker: std::marker::PhantomData<fn(K, C, O, E)>,
}
impl<K, C, O, E> Default for Registry<K, C, O, E> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Table::default()),
            marker: std::marker::PhantomData,
        }
    }
}
impl<K, C, O, E, G: ?Sized> Clone for Registry<K, C, O, E, G> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            marker: std::marker::PhantomData,
        }
    }
}
impl<K, C, O, E> Registry<K, C, O, E> {
    /// 使用默认静态 Table 创建空分发表。
    pub fn new() -> Self {
        Self::default()
    }
}
impl<K, C, O, E, G: ?Sized> Registry<K, C, O, E, G> {
    /// 注入共享注册器，支持 dyn Registry；更新可见性由具体实现定义。
    pub fn from_shared_registry(registry: Arc<G>) -> Self {
        Self {
            inner: registry,
            marker: std::marker::PhantomData,
        }
    }
}
impl<K, C, O, E, G> Registry<K, C, O, E, G> {
    /// 注入自定义底层注册器，协议适配仍负责正文编解码。
    pub fn from_registry(registry: G) -> Self {
        Self::from_shared_registry(Arc::new(registry))
    }
}
impl<K, C, O, E, G> Registry<K, C, O, E, G>
where
    K: Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: registry::Registry<
            Key = K,
            Input = (C, Vec<u8>),
            Output = Reply<Message<K>, O>,
            HandlerError = Error<E>,
        > + Clone
        + 'static,
{
    /// 注册异步业务函数；业务错误不转换为传输状态码。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_key: K,
        response_key: K,
        handler: F,
    ) -> Result<(), G::RegistrationError>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(C, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Reply<Resp, O>>, E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_key,
            response_key,
            tower::service_fn(move |request: Request<(C, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
    /// 在正文编解码之间安装强类型 Tower 链，执行约束由通用分发表统一施加。
    pub fn register_service<Req, Resp, S>(
        &mut self,
        request_key: K,
        response_key: K,
        service: S,
    ) -> Result<(), G::RegistrationError>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<Request<(C, Req)>, Response = Response<Reply<Resp, O>>, Error = E>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        Arc::make_mut(&mut self.inner).register_service(
            request_key,
            tower::service_fn(move |request: Request<(C, Vec<u8>)>| {
                let service = service.clone();
                let response_key = response_key.clone();
                async move {
                    let (context, (state, body), extensions) = request.into_parts();
                    let input = Req::decode(body.as_slice()).map_err(Error::Decode)?;
                    let reply = service
                        .oneshot(Request::from_parts(context, (state, input), extensions))
                        .await
                        .map_err(Error::Handler)?;
                    Ok(reply.map(|reply| Reply {
                        message: Message {
                            key: response_key,
                            body: reply.message.encode_to_vec(),
                        },
                        output: reply.output,
                    }))
                }
            }),
        )
    }
}
impl<K, C, O, E, G: ?Sized> Registry<K, C, O, E, G>
where
    K: Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: registry::Registry<
            Key = K,
            Input = (C, Vec<u8>),
            Output = Reply<Message<K>, O>,
            HandlerError = Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    /// 使用消息键分发并编解码正文，业务状态不进入线协议。
    pub async fn dispatch(
        &self,
        request: Request<(C, Message<K>)>,
    ) -> Result<Response<Reply<Message<K>, O>>, tok_execution::Error<G::DispatchError>> {
        self.clone().oneshot(request).await
    }
}

impl<K, C, O, E, G: ?Sized> Service<Request<(C, Message<K>)>> for Registry<K, C, O, E, G>
where
    K: Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: registry::Registry<
            Key = K,
            Input = (C, Vec<u8>),
            Output = Reply<Message<K>, O>,
            HandlerError = Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    type Response = Response<Reply<Message<K>, O>>;
    type Error = tok_execution::Error<G::DispatchError>;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<(C, Message<K>)>) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let (context, (state, message), extensions) = request.into_parts();
            let key = message.key;
            registry::Service::new(inner, move |_: &Request<(C, Vec<u8>)>| key.clone())
                .oneshot(Request::from_parts(
                    context,
                    (state, message.body),
                    extensions,
                ))
                .await
        })
    }
}
