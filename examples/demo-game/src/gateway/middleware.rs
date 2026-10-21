//! 示例用同一 Layer 观察各入口，并通过显式适配迁移每会话统计状态。
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};
use tok::observability::tracing;
use tok::rpc::tower::{Layer, Service, ServiceExt};
use tok::{
    core::execution::Extensions,
    gateway::{
        ErrorCode, GatewayError, Result,
        player::{
            SessionHandle, SessionManager,
            session::extension::{self, Change},
        },
    },
    player::SessionIdentity,
    service::Request,
};

#[derive(Default)]
struct Statistics {
    executions: AtomicU64,
}
/// 同一个中间件协议可安装到准入、请求、事件、timer 和新下行。
#[derive(Clone, Copy)]
pub(super) struct Observe;
#[derive(Clone)]
pub(super) struct Observed<S>(S);
impl<S> Layer<S> for Observe {
    type Service = Observed<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Observed(inner)
    }
}
impl<T: Send + 'static, O: Send + 'static, S> Service<Request<T>> for Observed<S>
where
    S: Service<Request<T>, Response = O, Error = GatewayError> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = O;
    type Error = GatewayError;
    type Future = Pin<Box<dyn Future<Output = Result<O>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let inner = self.0.clone();
        Box::pin(async move {
            let session = request
                .context
                .extensions()
                .get::<SessionHandle>()
                .ok_or(ErrorCode::InvalidState)?;
            let manager = request
                .context
                .extensions()
                .get::<SessionManager>()
                .ok_or(ErrorCode::InvalidState)?;
            if request.context.operation().starts_with("player/request/")
                || request.context.operation().starts_with("player/event/")
                || request.context.operation().starts_with("player/timer/")
            {
                request
                    .context
                    .extensions()
                    .get::<Statistics>()
                    .ok_or(ErrorCode::InvalidState)?
                    .executions
                    .fetch_add(1, Ordering::Relaxed);
            }
            tracing::debug!(operation=request.context.operation(),player_id=%session.identity().player_id,gateway=%manager.owner().service_name,"进入玩家处理链");
            inner.oneshot(request).await
        })
    }
}
/// 示例状态属于逻辑会话，跨网关恢复必须明确迁移，不能直接丢弃。
pub(super) struct SessionStatistics;
impl extension::Factory for SessionStatistics {
    fn kind(&self) -> &str {
        "demo.session-statistics.v1"
    }
    fn initialize(&self, _: Request<SessionIdentity>) -> Result<Extensions> {
        let mut extensions = Extensions::default();
        extensions.insert(Statistics::default());
        Ok(extensions)
    }
    fn snapshot(&self, request: Request<SessionHandle>) -> Result<Vec<u8>> {
        let state = request
            .input
            .extensions()
            .get::<Statistics>()
            .ok_or(ErrorCode::InvalidState)?;
        Ok(state
            .executions
            .load(Ordering::Relaxed)
            .to_le_bytes()
            .to_vec())
    }
    fn validate(&self, snapshot: &[u8]) -> Result<()> {
        if snapshot.len() == 8 {
            Ok(())
        } else {
            Err(ErrorCode::ResumeUnavailable.into())
        }
    }
    fn restore(&self, request: Request<(SessionIdentity, &[u8])>) -> Result<Extensions> {
        self.validate(request.input.1)?;
        let bytes: [u8; 8] = request
            .input
            .1
            .try_into()
            .map_err(|_| ErrorCode::ResumeUnavailable)?;
        let mut extensions = Extensions::default();
        extensions.insert(Statistics {
            executions: AtomicU64::new(u64::from_le_bytes(bytes)),
        });
        Ok(extensions)
    }
    fn on_change(&self, request: Request<Change>) {
        tracing::debug!(change=?request.input,"玩家会话生命周期变化");
    }
}
