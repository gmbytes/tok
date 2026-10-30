//! 将 logic 注册器接入统一 Tower 输入与结果。
use super::{HandlerRegistry, Input, Output};
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tok_execution::{Request, Response};
use tonic::Status;

/// 可克隆的 logic 注册器执行入口；克隆共享同一注册快照。
pub struct RegistryService<G: tok_execution::Registry + ?Sized> {
    registry: Arc<HandlerRegistry<G>>,
}

impl<G: tok_execution::Registry + ?Sized> Clone for RegistryService<G> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
        }
    }
}

impl<G: tok_execution::Registry + ?Sized> RegistryService<G> {
    /// 使用已组装的 logic 注册器。
    pub fn new(registry: Arc<HandlerRegistry<G>>) -> Self {
        Self { registry }
    }
}

impl<G: ?Sized> tower::Service<Request<Input>> for RegistryService<G>
where
    G: tok_execution::Registry<
            Key = u32,
            Input = (super::CallContext, Vec<u8>),
            Output = tok_message::Reply<tok_message::Message<u32>, Option<crate::TimerUpdate>>,
            HandlerError = tok_message::codec::Error<Status>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    type Response = Response<Output>;
    type Error = Status;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Status>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Input>) -> Self::Future {
        let registry = Arc::clone(&self.registry);
        Box::pin(async move {
            let (context, input, extensions) = request.into_parts();
            match input {
                Input::Message(call, payload) => {
                    if call.timer.is_some() {
                        return Err(Status::invalid_argument("消息入口不能携带 timer"));
                    }
                    registry
                        .call(Request::from_parts(context, (call, payload), extensions))
                        .await
                        .map(|response| response.map(Output::Message))
                }
                Input::Timer(call, payload) => {
                    if call.timer.is_none() {
                        return Err(Status::invalid_argument("timer 入口缺少触发信息"));
                    }
                    registry
                        .call(Request::from_parts(context, (call, payload), extensions))
                        .await
                        .map(|response| response.map(Output::Timer))
                }
                Input::Event(call, event) => registry
                    .event(Request::from_parts(context, (call, event), extensions))
                    .await
                    .map(|response| response.map(Output::Event)),
            }
        })
    }
}
