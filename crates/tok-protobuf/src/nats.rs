//! NATS 发布时在容量保护内编码 protobuf 正文。

use std::future::Future;
use tok_events::{Envelope, Error, Event};
use tok_execution::{Request, Response};

/// 为 NATS 发布器提供 protobuf 事件便捷入口。
pub trait PublisherExt {
    /// 在发布容量保护内编码并发布事件。
    fn publish_event<E: Event + prost::Message>(
        &self,
        event: Envelope<E>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// 在原执行范围和发布容量保护内编码并发布事件。
    fn publish_request<E: Event + prost::Message>(
        &self,
        request: Request<Envelope<E>>,
    ) -> impl Future<Output = Result<Response<()>, Error>> + Send;
}
impl PublisherExt for tok_nats::Bus {
    async fn publish_event<E: Event + prost::Message>(
        &self,
        event: Envelope<E>,
    ) -> Result<(), Error> {
        self.publish_event_with(event, crate::events::encode).await
    }
    async fn publish_request<E: Event + prost::Message>(
        &self,
        request: Request<Envelope<E>>,
    ) -> Result<Response<()>, Error> {
        self.publish_request_with(request, crate::events::encode)
            .await
    }
}
