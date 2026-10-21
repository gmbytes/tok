//! 与业务输入和传输协议无关的 Tower 请求锁。
//!
//! 调用方显式提供取键函数及回应成功判断；中间件不理解 RPC、玩家或消息编码。
//! 获取、就绪等待、执行和释放受原 Request 上下文约束，不续租、不重试。
//! 返回 `execution::Error<lock::Error<E>>`；原调用停止统一位于外层 Stopped。
//! 外层取消或丢弃 future 后，未完成的获取和释放依靠后端有限 TTL 兜底。
use crate::Request;
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tok_core::{
    BoxFuture,
    lock::{Backend, Key},
};
use tokio::time::{Instant, timeout};
use tower::{Layer as _, ServiceExt};

/// 请求锁处理失败；业务错误与取键错误保留原类型。
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// 调用方无法从请求中提取合法锁键。
    #[error("请求锁取键失败：{0}")]
    Key(#[source] E),
    /// 后端获取失败；后端竞争超时与本地获取超时保持区分。
    #[error("请求锁获取失败：{0}")]
    Acquire(#[source] tok_core::lock::Error),
    /// 整个获取操作超时，结果可能不确定。
    #[error("请求锁获取超时")]
    AcquireTimeout,
    /// 从获取开始扣减后，剩余租期不足以容纳执行及释放预算。
    #[error("请求锁剩余租期不足")]
    InsufficientLease,
    /// 内层就绪等待或处理超时，不表示业务副作用已回滚。
    #[error("持锁业务执行超时")]
    ExecutionTimeout,
    /// 内层 Service 的原始业务错误。
    #[error("持锁业务处理失败：{0}")]
    Inner(#[source] E),
    /// 条件释放失败，后端有限 TTL 兜底。
    #[error("请求锁条件释放失败：{0}")]
    Release(#[source] tok_core::lock::Error),
    /// 条件释放超时，结果可能不确定。
    #[error("请求锁条件释放超时")]
    ReleaseTimeout,
}

/// 一次请求的获取、执行和释放预算；以获取开始时刻保守计算租约有效期。
#[derive(Clone, Debug)]
pub struct Config {
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
impl Default for Config {
    fn default() -> Self {
        Self {
            wait: Duration::from_millis(100),
            acquire_timeout: Duration::from_millis(500),
            ttl: Duration::from_secs(3),
            execution_timeout: Duration::from_secs(1),
            release_timeout: Duration::from_millis(100),
        }
    }
}
impl Config {
    /// 校验有限时间预算，不访问后端。
    pub fn validate(&self) -> Result<(), tok_core::lock::Error> {
        let positive = [
            self.acquire_timeout,
            self.ttl,
            self.execution_timeout,
            self.release_timeout,
        ];
        if positive
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
            || self.ttl < Duration::from_millis(1)
            || self.wait > self.acquire_timeout
            || self
                .execution_timeout
                .checked_add(self.release_timeout)
                .is_none_or(|budget| budget >= self.ttl)
        {
            return Err(tok_core::lock::Error::InvalidConfig(
                "请求锁时间预算无效".into(),
            ));
        }
        Ok(())
    }
}

/// 使用显式取键与结果判断策略的请求锁 Layer。
///
/// K 接收 Request 的借用并返回锁键或业务错误；P 判断 Ok 回应是否表示业务成功。
/// 释放失败只覆盖业务成功，保留原始业务错误和被 P 判为失败的回应。
/// 即使取键、等待或业务失败也不重试；克隆共享后端及策略。
/// 当前适配要求输入、回应及内层错误为自有 Send + static 类型，不创建后台任务。
pub struct Layer<K, P> {
    backend: Arc<dyn Backend>,
    config: Config,
    key: Arc<K>,
    success: Arc<P>,
}
impl<K, P> Clone for Layer<K, P> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            config: self.config.clone(),
            key: Arc::clone(&self.key),
            success: Arc::clone(&self.success),
        }
    }
}
impl<K, P> Layer<K, P> {
    /// 校验时间预算并注入策略；取键和结果判断函数应短小、不阻塞，且不执行外部副作用。
    pub fn new(
        backend: Arc<dyn Backend>,
        config: Config,
        key: K,
        success: P,
    ) -> Result<Self, tok_core::lock::Error> {
        config.validate()?;
        Ok(Self {
            backend,
            config,
            key: Arc::new(key),
            success: Arc::new(success),
        })
    }
}
impl<S, K, P> tower::Layer<S> for Layer<K, P> {
    type Service = Service<S, K, P>;
    fn layer(&self, inner: S) -> Self::Service {
        Service {
            inner,
            layer: self.clone(),
        }
    }
}

/// 持锁驱动内部 Service，就绪等待计入执行预算；外部始终就绪不预占租约。
pub struct Service<S, K, P> {
    inner: S,
    layer: Layer<K, P>,
}
impl<S: Clone, K, P> Clone for Service<S, K, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}
impl<S, T, K, P> tower::Service<Request<T>> for Service<S, K, P>
where
    S: tower::Service<Request<T>> + Clone + Send + 'static,
    S::Future: Send,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    T: Send + 'static,
    K: Fn(&Request<T>) -> Result<Key, S::Error> + Send + Sync + 'static,
    P: Fn(&S::Response) -> bool + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = crate::Error<Error<S::Error>>;
    type Future = BoxFuture<'static, Result<S::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let inner = self.inner.clone();
        let layer = self.layer.clone();
        // 复用共同执行层，使取键、获取与释放也遵守原取消、期限和追踪范围。
        let execution = crate::Layer.layer(tower::service_fn(move |request| {
            run(inner.clone(), layer.clone(), request)
        }));
        Box::pin(async move {
            execution
                .oneshot(request)
                .await
                .map_err(crate::Error::flatten)
        })
    }
}
async fn run<S, T, K, P>(
    inner: S,
    layer: Layer<K, P>,
    request: Request<T>,
) -> crate::Result<S::Response, Error<S::Error>>
where
    S: tower::Service<Request<T>>,
    K: Fn(&Request<T>) -> Result<Key, S::Error>,
    P: Fn(&S::Response) -> bool,
{
    let key = (layer.key)(&request).map_err(|error| crate::Error::Inner(Error::Key(error)))?;
    request.context.check()?;
    let namespace = key.namespace.clone();
    let config = &layer.config;
    let started = Instant::now();
    let lease = timeout(
        config.acquire_timeout,
        layer.backend.acquire(key, config.wait, config.ttl),
    )
    .await
    .map_err(|_| crate::Error::Inner(Error::AcquireTimeout))?
    .map_err(|error| crate::Error::Inner(Error::Acquire(error)))?;
    let elapsed = started.elapsed();
    let remaining = config.ttl.saturating_sub(elapsed);
    let result = if elapsed >= config.acquire_timeout {
        Err(crate::Error::Inner(Error::AcquireTimeout))
    } else if remaining <= config.execution_timeout + config.release_timeout {
        Err(crate::Error::Inner(Error::InsufficientLease))
    } else if let Err(reason) = request.context.check() {
        Err(crate::Error::Stopped(reason))
    } else {
        let execution_started = Instant::now();
        let mut request = request;
        request.context = request.context.with_budget_limit(config.execution_timeout);
        let result = timeout(config.execution_timeout, crate::Call::new(inner, request)).await;
        // timeout 无法抢占单次 poll 内的阻塞代码，接受结果前仍检查实际耗时。
        if execution_started.elapsed() >= config.execution_timeout {
            Err(crate::Error::Inner(Error::ExecutionTimeout))
        } else {
            match result {
                Ok(result) => result.map_err(|error| error.map_inner(Error::Inner)),
                Err(_) => Err(crate::Error::Inner(Error::ExecutionTimeout)),
            }
        }
    };
    let release_started = Instant::now();
    let release = timeout(config.release_timeout, lease.release()).await;
    let release_error = if release_started.elapsed() >= config.release_timeout {
        Some(Error::ReleaseTimeout)
    } else {
        match release {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Error::Release(error)),
            Err(_) => Some(Error::ReleaseTimeout),
        }
    };
    if let Some(error) = release_error {
        tracing::error!(%namespace, "请求锁条件释放失败或超时，等待租期兜底");
        if result
            .as_ref()
            .is_ok_and(|response| (layer.success)(response))
        {
            return Err(crate::Error::Inner(error));
        }
    }
    result
}
