//! 消息正文的 protobuf 便捷适配。
#![allow(dead_code)]

use std::{future::Future, sync::Arc};
use tok_execution::{Request, Response};
use tok_message::codec::{Error, Message, Reply};
use tower::Service;

/// 为正文分发表注册 protobuf 请求和回应。
pub trait RegistryExt<K, C, O, E> {
    /// 注册配置错误。
    type RegistrationError;

    /// 安装 protobuf Tower 处理链。
    fn register_service<Req, Resp, S>(
        &mut self,
        request_key: K,
        response_key: K,
        service: S,
    ) -> Result<(), Self::RegistrationError>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<Request<(C, Req)>, Response = Response<Reply<Resp, O>>, Error = E>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static;

    /// 注册异步业务函数。
    fn register<Req, Resp, F, Fut>(
        &mut self,
        request_key: K,
        response_key: K,
        handler: F,
    ) -> Result<(), Self::RegistrationError>
    where
        C: Send + 'static,
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(C, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Reply<Resp, O>>, E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_key,
            response_key,
            tower::service_fn(move |request| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
}

impl<K, C, O, E, G> RegistryExt<K, C, O, E> for tok_message::codec::Registry<K, C, O, E, G>
where
    K: Clone + Send + Sync + 'static,
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = K,
            Input = (C, Vec<u8>),
            Output = Reply<Message<K>, O>,
            HandlerError = Error<E>,
        > + Clone
        + 'static,
{
    type RegistrationError = G::RegistrationError;
    fn register_service<Req, Resp, S>(
        &mut self,
        request_key: K,
        response_key: K,
        service: S,
    ) -> Result<(), Self::RegistrationError>
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
        self.register_service_with_codec(
            request_key,
            response_key,
            crate::test_codec::Codec,
            crate::test_codec::Codec,
            service,
        )
    }
}

impl<C, O, E, G> RegistryExt<u32, C, O, E> for tok_message::Registry<C, O, E, G>
where
    C: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = Reply<Message<u32>, O>,
            HandlerError = Error<E>,
        > + Clone
        + 'static,
{
    type RegistrationError = tok_message::Error<G::RegistrationError>;
    fn register_service<Req, Resp, S>(
        &mut self,
        request_key: u32,
        response_key: u32,
        service: S,
    ) -> Result<(), Self::RegistrationError>
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
        self.register_service_with_codec(
            request_key,
            response_key,
            crate::test_codec::Codec,
            crate::test_codec::Codec,
            service,
        )
    }
}
