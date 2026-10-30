use crate::{DeliveryPolicy, Message, Task};
#[cfg(feature = "protobuf")]
use crate::{Envelope, Error, Event};
use tok_execution::{Request, Response};

/// 传输无关的已编码发布，投递要求不因编码而丢失。
#[derive(Clone, Debug)]
pub struct Publication {
    /// 已编码事件。
    pub message: Message,
    /// 业务要求的投递语义；后端不支持时必须明确拒绝。
    pub delivery: DeliveryPolicy,
}
impl Publication {
    /// 使用 protobuf 编码强类型事件；不自动采集当前任务的隐式 span。
    #[cfg(feature = "protobuf")]
    pub fn protobuf<E: Event + prost::Message>(event: &Envelope<E>) -> Result<Self, Error> {
        let mut message = Message::from_protobuf(event)?;
        message.trace_context = event.trace_context.clone();
        Ok(Self {
            message,
            delivery: E::DELIVERY,
        })
    }
}

/// 事件传输的发布契约；原请求范围与本地扩展交由实现接续。
///
/// 即时发布成功表示后端接受；持久发布成功表示后端按其配置确认存储，
/// 不表示订阅者已完成处理。不支持的持久化不得降级为即时投递。
pub trait Publisher: Send + Sync {
    /// 发布一条已编码事件，返回协议终端结果，不表示订阅者业务完成。
    fn publish(&self, request: Request<Publication>) -> Task<'_, Response<()>>;
}
impl<P: Publisher + ?Sized> Publisher for std::sync::Arc<P> {
    fn publish(&self, request: Request<Publication>) -> Task<'_, Response<()>> {
        self.as_ref().publish(request)
    }
}

/// 将通用发布器接入 Tower，供发布链叠加中间件。
#[derive(Clone)]
pub struct PublisherService<P>(P);
impl<P> PublisherService<P> {
    /// 构造发布处理链末端。
    pub fn new(publisher: P) -> Self {
        Self(publisher)
    }
}
impl<P> tower::Service<Request<Publication>> for PublisherService<P>
where
    P: Publisher + Clone + Send + Sync + 'static,
{
    type Response = Response<()>;
    type Error = crate::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Publication>) -> Self::Future {
        let publisher = self.0.clone();
        Box::pin(async move {
            let handler = tower::service_fn(move |request| {
                let publisher = publisher.clone();
                async move { publisher.publish(request).await }
            });
            tok_execution::Call::new(handler, request)
                .await
                .map_err(|error| match error {
                    tok_execution::Error::Stopped(reason) => crate::Error::Stopped(reason),
                    tok_execution::Error::Inner(error) => error,
                })
        })
    }
}
