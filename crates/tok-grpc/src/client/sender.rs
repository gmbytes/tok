use crate::Outgoing;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status, transport::Channel};

pub(super) struct Sender<F> {
    send: Option<F>,
}
impl<F> Sender<F> {
    pub(super) fn new(send: F) -> Self {
        Self { send: Some(send) }
    }
}
impl<T, R, F, Fut> tower::Service<tok_execution::Request<Outgoing<T>>> for Sender<F>
where
    F: FnOnce(Channel, Request<T>) -> Fut,
    Fut: Future<Output = Result<Response<R>, Status>>,
{
    type Response = Response<R>;
    type Error = Status;
    type Future = SendFuture<Fut>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, call: tok_execution::Request<Outgoing<T>>) -> Self::Future {
        match (self.send.take(), call.input.into_parts(&call.context)) {
            (Some(send), Ok((channel, request))) => SendFuture {
                running: Some(Box::pin(send(channel, request))),
                error: None,
            },
            (_, Err(error)) => SendFuture {
                running: None,
                error: Some(error),
            },
            (None, _) => SendFuture {
                running: None,
                error: Some(Status::failed_precondition("发送回调只能调用一次")),
            },
        }
    }
}
pub(super) struct SendFuture<F> {
    running: Option<Pin<Box<F>>>,
    error: Option<Status>,
}
impl<F, R> Future for SendFuture<F>
where
    F: Future<Output = Result<Response<R>, Status>>,
{
    type Output = Result<Response<R>, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.running {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Ready(Err(this
                .error
                .take()
                .unwrap_or_else(|| Status::failed_precondition("发送回调已经完成")))),
        }
    }
}
