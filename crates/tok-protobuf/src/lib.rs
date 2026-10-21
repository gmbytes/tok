//! protobuf 正文编解码适配；分发键 K 由调用方定义，不包含 RPC 信封。
//! Registry 可直接作为 Tower Service，公共策略包住分发表，路由策略保留在注册项内。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use std::{future::Future, sync::Arc};
use tok_execution::Request;
use tok_execution::registry::{self, DispatchError};
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

type EncodedRegistry<K, C, O, E> =
    tok_execution::registry::Registry<K, (C, Vec<u8>), Reply<Message<K>, O>, Error<E>>;

/// 在通用分发表上增加 protobuf 编解码，C 是调用状态，O 是附加结果，E 是业务错误。
///
/// K 可以是数字、字符串或枚举。本适配器不规定零或空字符串是否有效，
/// 编解码、内层就绪等待和业务执行均在原 Context 约束内，不创建后台任务。
/// 可直接作为接受 `Request<(C, Message<K>)>` 的 Tower Service，供公共 Layer 包装。
/// poll_ready 不预留目标路由容量；克隆共享路由快照，克隆后注册新路由不会修改旧快照。
pub struct Registry<K, C = (), O = (), E = std::convert::Infallible> {
    inner: Arc<EncodedRegistry<K, C, O, E>>,
}
impl<K, C, O, E> Default for Registry<K, C, O, E> {
    fn default() -> Self {
        Self {
            inner: Arc::new(tok_execution::registry::Registry::default()),
        }
    }
}
impl<K: Clone, C, O, E> Clone for Registry<K, C, O, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
impl<K, C, O, E> Registry<K, C, O, E>
where
    K: Ord + Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    /// 创建空的 protobuf 分发表。
    pub fn new() -> Self {
        Self::default()
    }
    /// 注册异步业务函数；业务错误不转换为传输状态码。
    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        request_key: K,
        response_key: K,
        handler: F,
    ) -> Result<(), registry::Error<K>>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(C, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Reply<Resp, O>, E>> + Send + 'static,
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
    ) -> Result<(), registry::Error<K>>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<Request<(C, Req)>, Response = Reply<Resp, O>, Error = E>
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
                    let (state, body) = request.input;
                    let input = Req::decode(body.as_slice()).map_err(Error::Decode)?;
                    let reply = service
                        .oneshot(Request::new(request.context, (state, input)))
                        .await
                        .map_err(Error::Handler)?;
                    Ok(Reply {
                        message: Message {
                            key: response_key,
                            body: reply.message.encode_to_vec(),
                        },
                        output: reply.output,
                    })
                }
            }),
        )
    }
    /// 使用消息键分发并编解码正文，业务状态不进入线协议。
    pub async fn dispatch(
        &self,
        request: Request<(C, Message<K>)>,
    ) -> Result<Reply<Message<K>, O>, DispatchError<K, Error<E>>> {
        self.clone().oneshot(request).await
    }
}

impl<K, C, O, E> Service<Request<(C, Message<K>)>> for Registry<K, C, O, E>
where
    K: Ord + Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    type Response = Reply<Message<K>, O>;
    type Error = DispatchError<K, Error<E>>;
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
            let (state, message) = request.input;
            inner
                .dispatch(
                    &message.key,
                    Request::new(request.context, (state, message.body)),
                )
                .await
        })
    }
}
