use std::{future::Future, sync::Arc};
use tok_execution::{Request, Response};
use tower::{Service, ServiceExt};

/// 编号消息的参数或注册错误。
#[derive(Debug, thiserror::Error)]
pub enum Error<E = tok_execution::registry::Error<u32>> {
    /// 消息编号必须非零。
    #[error("消息编号无效：{0}")]
    Invalid(String),
    /// 底层注册器拒绝注册，保留具体错误来源。
    #[error("消息注册失败：{0}")]
    Registration(#[from] E),
}

/// 编号业务消息的 protobuf 分发表。
///
/// C 是调用状态，O 是附加结果，E 是业务错误。请求和回应编号必须非零；
/// 编解码、就绪等待和处理链执行共用原始执行预算，不构造 RPC 信封。
/// 可直接交给 ServiceBuilder 安装公共 Layer；输入为 `Request<(C, Message)>`。
/// poll_ready 仅代表分发入口可接收调用，目标就绪在调用范围内等待。
pub struct Registry<
    C = (),
    O = (),
    E = std::convert::Infallible,
    G: ?Sized = tok_protobuf::Table<u32, C, O, E>,
> {
    inner: tok_protobuf::Registry<u32, C, O, E, G>,
}
impl<C, O, E> Default for Registry<C, O, E> {
    fn default() -> Self {
        Self {
            inner: tok_protobuf::Registry::default(),
        }
    }
}
impl<C, O, E, G: ?Sized> Clone for Registry<C, O, E, G> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<C, O, E> Registry<C, O, E> {
    /// 使用默认静态 Table 创建空表。
    pub fn new() -> Self {
        Self::default()
    }
}
impl<C, O, E, G: ?Sized> Registry<C, O, E, G> {
    /// 注入共享自定义注册器；允许使用运行时选择的 dyn Registry。
    pub fn from_shared_registry(registry: Arc<G>) -> Self {
        Self {
            inner: tok_protobuf::Registry::from_shared_registry(registry),
        }
    }
}
impl<C, O, E, G> Registry<C, O, E, G> {
    /// 注入底层自定义注册器，仍保留非零消息编号及 protobuf 编解码规则。
    pub fn from_registry(registry: G) -> Self {
        Self {
            inner: tok_protobuf::Registry::from_registry(registry),
        }
    }
}
impl<C, O, E, G> Registry<C, O, E, G>
where
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = tok_protobuf::Reply<tok_protobuf::Message<u32>, O>,
            HandlerError = tok_protobuf::Error<E>,
        > + Clone
        + 'static,
{
    /// 注册强类型请求和回应；原始业务错误保持不变。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), Error<G::RegistrationError>>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(C, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<tok_protobuf::Reply<Resp, O>>, E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_id,
            response_id,
            tower::service_fn(move |request: Request<(C, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
    /// 安装强类型 Tower 处理链。
    pub fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), Error<G::RegistrationError>>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<Request<(C, Req)>, Response = Response<tok_protobuf::Reply<Resp, O>>, Error = E>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        if request_id == 0 || response_id == 0 {
            return Err(Error::Invalid("请求和回应消息编号必须非零".into()));
        }
        self.inner
            .register_service(request_id, response_id, service)
            .map_err(Error::Registration)
    }
}
impl<C, O, E, G: ?Sized> Registry<C, O, E, G>
where
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = tok_protobuf::Reply<tok_protobuf::Message<u32>, O>,
            HandlerError = tok_protobuf::Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    /// 本地分发编号消息；不将业务错误转换为传输状态码。
    pub async fn dispatch(
        &self,
        request: Request<(C, tok_protobuf::Message<u32>)>,
    ) -> Result<
        Response<tok_protobuf::Reply<tok_protobuf::Message<u32>, O>>,
        tok_execution::Error<G::DispatchError>,
    > {
        self.clone().oneshot(request).await
    }
}

impl<C, O, E, G: ?Sized> Service<Request<(C, tok_protobuf::Message<u32>)>> for Registry<C, O, E, G>
where
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = tok_protobuf::Reply<tok_protobuf::Message<u32>, O>,
            HandlerError = tok_protobuf::Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    type Response = Response<tok_protobuf::Reply<tok_protobuf::Message<u32>, O>>;
    type Error = tok_execution::Error<G::DispatchError>;
    type Future = <tok_protobuf::Registry<u32, C, O, E, G> as Service<
        Request<(C, tok_protobuf::Message<u32>)>,
    >>::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request<(C, tok_protobuf::Message<u32>)>) -> Self::Future {
        self.inner.call(request)
    }
}
