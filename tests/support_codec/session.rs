//! 逻辑会话请求和事件的 protobuf 注册。
#![allow(dead_code)]

use std::{future::Future, sync::Arc};
use tok_execution::{Request, Response};
use tok_session::{
    TimerUpdate,
    logic::rpc::{CallContext, Error, HandlerRegistry, HandlerReply},
};
use tonic::Status;
use tower::Service;

/// 逻辑会话 protobuf 处理链的便捷注册。
pub trait RegistryExt {
    /// 注册使用 protobuf 的 Tower 处理链。
    fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<
                Request<(CallContext, Req)>,
                Response = Response<HandlerReply<Resp>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static;

    /// 注册可以更新 timer 的业务函数。
    fn register_with_timers<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<HandlerReply<Resp>>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            request_id,
            response_id,
            tower::service_fn(move |request| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }

    /// 注册不更新 timer 的业务函数。
    fn register<Req, Resp, F, Fut>(
        &mut self,
        request_id: u32,
        response_id: u32,
        handler: F,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Request<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
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
}

impl<G> RegistryExt for HandlerRegistry<G>
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
    fn register_service<Req, Resp, S>(
        &mut self,
        request_id: u32,
        response_id: u32,
        service: S,
    ) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        S: Service<
                Request<(CallContext, Req)>,
                Response = Response<HandlerReply<Resp>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.register_service_with_codec(
            request_id,
            response_id,
            crate::test_codec::Codec,
            crate::test_codec::Codec,
            service,
        )
    }
}

/// 逻辑会话的 protobuf 事件注册，不要求消息注册器支持静态注册。
pub trait EventRegistryExt {
    /// 注册 protobuf 服务器事件。
    fn register_event<E, H>(&mut self, handler: H) -> Result<(), Error>
    where
        E: tok_events::Event + prost::Message + Default,
        H: for<'a> Fn(
                Request<tok_events::Input<'a, CallContext, E>>,
            ) -> tok_events::Task<'a, Response<Option<TimerUpdate>>, Status>
            + Send
            + Sync
            + 'static;
}
impl<G: tok_execution::Registry + ?Sized> EventRegistryExt for HandlerRegistry<G> {
    fn register_event<E, H>(&mut self, handler: H) -> Result<(), Error>
    where
        E: tok_events::Event + prost::Message + Default,
        H: for<'a> Fn(
                Request<tok_events::Input<'a, CallContext, E>>,
            ) -> tok_events::Task<'a, Response<Option<TimerUpdate>>, Status>
            + Send
            + Sync
            + 'static,
    {
        self.register_event_with_codec::<E, _, _>(crate::test_codec::Codec, handler)
    }
}

/// 使用 protobuf 解码逻辑会话内部 timer 的参数。
pub trait TimerRegistryExt {
    /// 安装 timer 的类型化 Tower 处理链。
    fn register_timer_service<Req, S>(&mut self, message_id: u32, service: S) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        S: Service<
                Request<(CallContext, Req)>,
                Response = Response<Option<TimerUpdate>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static;

    /// 注册内部 timer 回调。
    fn register_timer<Req, F, Fut>(&mut self, message_id: u32, handler: F) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        F: Fn(Request<(CallContext, Req)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Option<TimerUpdate>>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_timer_service(
            message_id,
            tower::service_fn(move |request| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }
}
impl<G: tok_execution::Registry + ?Sized> TimerRegistryExt for HandlerRegistry<G> {
    fn register_timer_service<Req, S>(&mut self, message_id: u32, service: S) -> Result<(), Error>
    where
        Req: prost::Message + Default + Send + 'static,
        S: Service<
                Request<(CallContext, Req)>,
                Response = Response<Option<TimerUpdate>>,
                Error = Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.register_timer_service_with_codec(message_id, crate::test_codec::Codec, service)
    }
}
