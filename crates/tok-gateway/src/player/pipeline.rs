//! 玩家准入、执行与新下行使用共同 Tower 协议，调度与编解码仍由网关掌握。
use super::{EventInput, EventTask, Executor, Reply, RequestInput, Task, TimerInput};
use crate::{GatewayError, Result};
use std::sync::Arc;
use tok_execution::Request;
use tok_player::{LogicalMessage, TimerUpdate};
use tower::{Layer, Service, ServiceExt, util::BoxCloneSyncService};
type Chain<I, O> = BoxCloneSyncService<Request<I>, O, GatewayError>;

/// 一个服务组装的玩家处理链集合；克隆共享用户 Layer 自身声明的并发状态。
/// 不取代 FIFO、认证、所有权及下行编号，不将事件转换为客户端回应。
#[derive(Clone)]
pub struct Pipeline {
    admission: Chain<LogicalMessage, Request<LogicalMessage>>,
    request: Chain<RequestInput, Reply>,
    event: Chain<EventInput, Option<TimerUpdate>>,
    timer: Chain<TimerInput, Reply>,
    downlink: Chain<LogicalMessage, LogicalMessage>,
}
impl Pipeline {
    /// 使用明确注入的执行器构建处理链，准入和下行默认原样通过。
    pub fn new(executor: Arc<dyn Executor>) -> Self {
        let request = Arc::clone(&executor);
        let event = Arc::clone(&executor);
        Self {
            admission: Chain::new(tower::service_fn(
                |request: Request<LogicalMessage>| async move { Ok(request) },
            )),
            downlink: Chain::new(tower::service_fn(
                |request: Request<LogicalMessage>| async move { Ok(request.input) },
            )),
            request: Chain::new(tower::service_fn(move |input| {
                let executor = Arc::clone(&request);
                async move { executor.execute_request(input).await }
            })),
            event: Chain::new(tower::service_fn(move |input| {
                let executor = Arc::clone(&event);
                async move { executor.execute_event(input).await }
            })),
            timer: Chain::new(tower::service_fn(move |input| {
                let executor = Arc::clone(&executor);
                async move { executor.execute_timer(input).await }
            })),
        }
    }
    /// 安装可信连接业务准入 Layer，完成后仍须校验连接代次和队列。
    pub fn with_admission_layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Chain<LogicalMessage, Request<LogicalMessage>>>,
        L::Service: Service<
                Request<LogicalMessage>,
                Response = Request<LogicalMessage>,
                Error = GatewayError,
            > + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as Service<Request<LogicalMessage>>>::Future: Send + 'static,
    {
        self.admission = Chain::new(layer.layer(self.admission));
        self
    }
    /// 安装 session FIFO 内的请求执行 Layer，不能绕过所有权校验。
    pub fn with_request_layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Chain<RequestInput, Reply>>,
        L::Service: Service<Request<RequestInput>, Response = Reply, Error = GatewayError>
            + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as Service<Request<RequestInput>>>::Future: Send + 'static,
    {
        self.request = Chain::new(layer.layer(self.request));
        self
    }
    /// 安装内部事件执行 Layer，结果不含客户端回应。
    pub fn with_event_layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Chain<EventInput, Option<TimerUpdate>>>,
        L::Service: Service<Request<EventInput>, Response = Option<TimerUpdate>, Error = GatewayError>
            + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as Service<Request<EventInput>>>::Future: Send + 'static,
    {
        self.event = Chain::new(layer.layer(self.event));
        self
    }
    /// 安装内部 timer 执行 Layer，保留计时来源与时刻。
    pub fn with_timer_layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Chain<TimerInput, Reply>>,
        L::Service: Service<Request<TimerInput>, Response = Reply, Error = GatewayError>
            + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as Service<Request<TimerInput>>>::Future: Send + 'static,
    {
        self.timer = Chain::new(layer.layer(self.timer));
        self
    }
    /// 安装新下行 Layer，在编号、编码和缓存前执行，补发不重复执行。
    pub fn with_downlink_layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Chain<LogicalMessage, LogicalMessage>>,
        L::Service: Service<Request<LogicalMessage>, Response = LogicalMessage, Error = GatewayError>
            + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as Service<Request<LogicalMessage>>>::Future: Send + 'static,
    {
        self.downlink = Chain::new(layer.layer(self.downlink));
        self
    }
    pub(super) async fn admit(
        &self,
        request: Request<LogicalMessage>,
    ) -> Result<Request<LogicalMessage>> {
        self.admission.clone().oneshot(request).await
    }
    pub(in crate::player) async fn downlink(
        &self,
        request: Request<LogicalMessage>,
    ) -> Result<LogicalMessage> {
        self.downlink.clone().oneshot(request).await
    }
}
impl Executor for Pipeline {
    fn execute_request(&self, request: Request<RequestInput>) -> Task<'_> {
        Box::pin(self.request.clone().oneshot(request))
    }
    fn execute_event(&self, request: Request<EventInput>) -> EventTask<'_> {
        Box::pin(self.event.clone().oneshot(request))
    }
    fn execute_timer(&self, request: Request<TimerInput>) -> Task<'_> {
        Box::pin(self.timer.clone().oneshot(request))
    }
}

#[cfg(test)]
pub(crate) fn test_pipeline() -> Arc<Pipeline> {
    struct Reject;
    impl Executor for Reject {
        fn execute_request(&self, _: Request<RequestInput>) -> Task<'_> {
            Box::pin(async { Err(crate::ErrorCode::UnknownMessage.into()) })
        }
    }
    Arc::new(Pipeline::new(Arc::new(Reject)))
}
