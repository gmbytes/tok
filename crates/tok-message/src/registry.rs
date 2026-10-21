use std::{future::Future, sync::Arc};
use tok_execution::Request;
use tower::Service;

/// 编号消息的参数或注册错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 消息编号必须非零。
    #[error("消息编号无效：{0}")]
    Invalid(String),
    /// 请求编号已经注册。
    #[error("消息注册失败：{0}")]
    Registration(#[from] tok_execution::registry::Error<u32>),
}

/// 编号业务消息的 protobuf 分发表。
///
/// C 是调用状态，O 是附加结果，E 是业务错误。请求和回应编号必须非零；
/// 编解码、就绪等待和处理链执行共用原始执行预算，不构造 RPC 信封。
pub struct Registry<C = (), O = (), E = std::convert::Infallible> {
    inner: tok_protobuf::Registry<u32, C, O, E>,
}
impl<C, O, E> Default for Registry<C, O, E> {
    fn default() -> Self {
        Self {
            inner: tok_protobuf::Registry::default(),
        }
    }
}
impl<C, O, E> Clone for Registry<C, O, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<C, O, E> Registry<C, O, E>
where
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    /// 创建空表。
    pub fn new() -> Self {
        Self::default()
    }
    /// 注册强类型请求和回应；原始业务错误保持不变。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(C, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<tok_protobuf::Reply<Resp, O>, E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_id,
            response_id,
            tower::service_fn(move |request: Request<(C, Req)>| {
                let handler = Arc::clone(&handler);
                async move { handler(request.input.0, request.input.1).await }
            }),
        )
    }
    /// 安装强类型 Tower 处理链。
    pub fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<Request<(C, Req)>, Response = tok_protobuf::Reply<Resp, O>, Error = E>
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
            .map_err(Error::from)
    }
    /// 本地分发编号消息；不将业务错误转换为传输状态码。
    pub async fn dispatch(
        &self,
        request: Request<(C, tok_protobuf::Message<u32>)>,
    ) -> Result<
        tok_protobuf::Reply<tok_protobuf::Message<u32>, O>,
        tok_execution::registry::DispatchError<u32, tok_protobuf::Error<E>>,
    > {
        self.inner.dispatch(request).await
    }
}
