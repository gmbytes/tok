//! 普通业务 RPC 的可选请求锁，按逻辑服务名与 bid 互斥执行。
//!
//! 在上下文校验后的 handler 上安装，不负责 RPC 建连，也不自动加入 player 或 GatewayRpc。
//! 不续租、不重试业务；外层丢弃调用 future 后依靠后端有限 TTL 释放。

use crate::CallContext;
use crate::{CallTask, Error};
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tok_core::lock::{Backend as LockBackend, Key as LockKey};
use tok_proto::internal::v1::{CallResponse, Payload};
use tokio::time::{Instant, timeout};
use tonic::Status;
use tower::ServiceExt;

/// 一次请求的获取、执行和释放预算；以获取开始时刻保守计算租约有效期。
#[derive(Clone, Debug)]
pub struct Config {
    /// 固定逻辑业务服务名，必须匹配调用上下文，不包含节点名。
    pub namespace: String,
    /// 后端锁竞争的等待上限；零表示仅尝试一次。
    pub wait: Duration,
    /// 整个获取操作的期限，包括连接；自定义后端也不能无限等待。
    pub acquire_timeout: Duration,
    /// 后端租期，不自动续租。
    pub ttl: Duration,
    /// handler 的最大执行时长。
    pub execution_timeout: Duration,
    /// 显式条件释放的期限，同时作为执行结束前的租约余量。
    pub release_timeout: Duration,
}
impl Config {
    /// 构造适合短 RPC 的默认预算；安装时仍需校验。
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            wait: Duration::from_millis(100),
            acquire_timeout: Duration::from_millis(500),
            ttl: Duration::from_secs(3),
            execution_timeout: Duration::from_secs(1),
            release_timeout: Duration::from_millis(100),
        }
    }
    fn validate(&self) -> Result<(), Error> {
        let positive = [
            self.acquire_timeout,
            self.ttl,
            self.execution_timeout,
            self.release_timeout,
        ];
        if self.namespace.trim().is_empty()
            || self.namespace.len() > 256
            || positive
                .iter()
                .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
            || self.ttl < Duration::from_millis(1)
            || self.wait > self.acquire_timeout
            || self
                .execution_timeout
                .checked_add(self.release_timeout)
                .is_none_or(|budget| budget >= self.ttl)
        {
            return Err(Error::Configuration("请求锁命名空间或时间预算无效".into()));
        }
        Ok(())
    }
}

/// 显式注入后端的 Tower 请求锁 Layer；克隆共享同一后端。
#[derive(Clone)]
pub struct Layer {
    backend: Arc<dyn LockBackend>,
    config: Config,
}
impl Layer {
    /// 校验有限预算并创建 Layer，不访问后端。
    pub fn new(backend: Arc<dyn LockBackend>, config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { backend, config })
    }
}
impl<S> tower::Layer<S> for Layer {
    type Service = Service<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Service {
            inner,
            layer: self.clone(),
        }
    }
}

/// 锁获取后等待内部 Service 就绪并执行；就绪等待计入执行预算。
#[derive(Clone)]
pub struct Service<S> {
    inner: S,
    layer: Layer,
}
impl<S> tower::Service<tok_execution::Request<(CallContext, Payload)>> for Service<S>
where
    S: tower::Service<
            tok_execution::Request<(CallContext, Payload)>,
            Response = CallResponse,
            Error = Status,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = CallResponse;
    type Error = Status;
    type Future = CallTask<'static>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        // 不预占租约；内部 readiness 由 call 中的 oneshot 驱动，受执行预算约束。
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tok_execution::Request<(CallContext, Payload)>) -> Self::Future {
        let execution = request.context;
        let (context, payload) = request.input;
        let layer = self.layer.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let config = &layer.config;
            if context.service_name != config.namespace {
                return Err(Status::failed_precondition(
                    "请求锁命名空间与目标服务不匹配",
                ));
            }
            let bid = context
                .bid
                .as_ref()
                .filter(|bid| !bid.trim().is_empty() && bid.len() <= 1024)
                .ok_or_else(|| Status::invalid_argument("请求锁需要有效 bid"))?;
            let started = Instant::now();
            let lease = timeout(
                config.acquire_timeout,
                layer.backend.acquire(
                    LockKey {
                        namespace: config.namespace.clone(),
                        bid: bid.clone(),
                    },
                    config.wait,
                    config.ttl,
                ),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("请求锁获取超时"))?
            .map_err(status)?;
            let elapsed = started.elapsed();
            let remaining = config.ttl.saturating_sub(elapsed);
            let required = config.execution_timeout + config.release_timeout;
            let result = if elapsed >= config.acquire_timeout {
                Err(Status::deadline_exceeded("请求锁获取超时"))
            } else if remaining <= required {
                Err(Status::deadline_exceeded("请求锁剩余租期不足"))
            } else {
                let execution_started = Instant::now();
                let result = timeout(
                    config.execution_timeout,
                    inner.oneshot(tok_execution::Request::new(execution, (context, payload))),
                )
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("持锁业务执行超时")));
                // timeout 不能抢占不让出执行权的 future；返回后仍须检查实际耗时。
                if execution_started.elapsed() >= config.execution_timeout {
                    Err(Status::deadline_exceeded("持锁业务执行超时"))
                } else {
                    result
                }
            };
            let release = timeout(config.release_timeout, lease.release()).await;
            if !matches!(release, Ok(Ok(()))) {
                tracing::error!(service_name = %config.namespace, "请求锁条件释放失败或超时，等待租期兜底");
                if result.as_ref().is_ok_and(|response| {
                    response.error.as_ref().is_none_or(|error| error.code == 0)
                }) {
                    return Err(Status::unavailable("请求锁释放结果不确定"));
                }
            }
            result
        })
    }
}
fn status(error: tok_core::lock::Error) -> Status {
    match error {
        tok_core::lock::Error::InvalidInput(_) => Status::invalid_argument("请求锁参数无效"),
        tok_core::lock::Error::WaitTimeout => Status::deadline_exceeded("请求锁获取超时"),
        tok_core::lock::Error::CapacityExceeded => Status::resource_exhausted("请求锁容量已满"),
        tok_core::lock::Error::InvalidConfig(_) => Status::failed_precondition("请求锁配置无效"),
        tok_core::lock::Error::Unavailable(_) => Status::unavailable("请求锁后端不可用"),
    }
}
