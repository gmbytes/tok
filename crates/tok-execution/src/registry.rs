//! 根据调用方定义的键选择处理链，不限定消息 ID、输入类型或编码格式。
//!
//! Registry 只负责选链和在原 Context 下执行，编解码由协议适配层负责。
use crate::Request;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tok_core::BoxFuture;
use tower::{Layer as _, Service as TowerService, ServiceExt};

/// 注册配置错误；键的有效性规则由调用方或协议边界规定。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error<K> {
    /// 同一键只能注册一次；注册失败不替换已有处理链。
    #[error("分发键重复注册：{0:?}")]
    DuplicateKey(K),
}

/// 路由或执行失败，保留未知键和原始处理错误，不包含协议状态码。
#[derive(Debug, thiserror::Error)]
pub enum DispatchError<K, E> {
    /// 没有为指定键注册处理链。
    #[error("分发键尚未注册：{0:?}")]
    Unknown(K),
    /// 原调用范围已取消或期限耗尽。
    #[error(transparent)]
    Stopped(crate::StopReason),
    /// 处理链返回的原始错误。
    #[error("处理链执行失败：{0}")]
    Handler(#[source] E),
}

type Handler<T, R, E> =
    dyn Fn(Request<T>) -> BoxFuture<'static, Result<R, crate::Error<E>>> + Send + Sync;

/// 通过 K 选择处理链，T、R、E 分别是输入、结果和业务错误。
///
/// 键只需支持排序和克隆，不要求数值、字符串或 Display。克隆注册器共享已注册的链；
/// Service 克隆所需的并发或限流状态须由对应 Layer 共享。不入队、不重试、不重新分配预算。
pub struct Registry<K, T, R, E = std::convert::Infallible> {
    handlers: BTreeMap<K, Arc<Handler<T, R, E>>>,
}
impl<K, T, R, E> Default for Registry<K, T, R, E> {
    fn default() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }
}
impl<K: Clone, T, R, E> Clone for Registry<K, T, R, E> {
    fn clone(&self) -> Self {
        Self {
            handlers: self.handlers.clone(),
        }
    }
}
impl<K, T, R, E> Registry<K, T, R, E>
where
    K: Ord + Clone,
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    /// 创建空分发表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册异步处理函数；需要中间件时使用 register_service。
    pub fn register<F, Fut>(&mut self, key: K, handler: F) -> Result<(), Error<K>>
    where
        F: Fn(Request<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.register_service(
            key,
            tower::service_fn(move |request| {
                let handler = Arc::clone(&handler);
                async move { handler(request).await }
            }),
        )
    }

    /// 安装 Tower 处理链；就绪等待和执行共用传入 Context 的原期限与取消范围。
    pub fn register_service<S>(&mut self, key: K, service: S) -> Result<(), Error<K>>
    where
        S: TowerService<Request<T>, Response = R, Error = E> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
    {
        if self.handlers.contains_key(&key) {
            return Err(Error::DuplicateKey(key));
        }
        self.handlers.insert(
            key,
            Arc::new(move |request| {
                let service = service.clone();
                Box::pin(async move { crate::Layer.layer(service).oneshot(request).await })
            }),
        );
        Ok(())
    }

    /// 将组装好的分发表转换为标准 Tower Service，由取键函数读取完整请求。
    ///
    /// 可在分发表外安装公共 Layer，路由内的 Layer 仍由 register_service 配置。
    /// Service 克隆共享这份只读分发表与取键函数，不复制全部路由，不重新分配执行预算。
    pub fn into_service<F>(self, key: F) -> Service<K, T, R, E, F>
    where
        K: Send + Sync + 'static,
        F: Fn(&Request<T>) -> K + Send + Sync + 'static,
    {
        Service {
            registry: Arc::new(self),
            key: Arc::new(key),
        }
    }

    /// 按键分发，保留原操作名、追踪、扩展、期限及取消；键与输入独立传递。
    pub async fn dispatch(&self, key: &K, request: Request<T>) -> Result<R, DispatchError<K, E>> {
        request.context.check().map_err(DispatchError::Stopped)?;
        let handler = self
            .handlers
            .get(key)
            .ok_or_else(|| DispatchError::Unknown(key.clone()))?;
        handler(request).await.map_err(|error| match error {
            crate::Error::Inner(error) => DispatchError::Handler(error),
            crate::Error::Stopped(reason) => DispatchError::Stopped(reason),
        })
    }
}

/// 按请求取键并执行已注册处理链的路由 Service，不是包装单个处理器的 Layer。
///
/// poll_ready 只表示分发入口可接受调用，不表示目标路由已有容量；取键、路由查询、
/// 目标就绪等待与执行均受原 Context 的期限、取消与追踪约束。外层中间件的等待
/// 仍须由外层执行保护负责。克隆共享固定路由快照，路由自己的限额状态由该 Layer 共享。
pub struct Service<K, T, R, E, F> {
    registry: Arc<Registry<K, T, R, E>>,
    key: Arc<F>,
}
impl<K, T, R, E, F> Clone for Service<K, T, R, E, F> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            key: Arc::clone(&self.key),
        }
    }
}
impl<K, T, R, E, F> TowerService<Request<T>> for Service<K, T, R, E, F>
where
    K: Ord + Clone + Send + Sync + 'static,
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
    F: Fn(&Request<T>) -> K + Send + Sync + 'static,
{
    type Response = R;
    type Error = DispatchError<K, E>;
    type Future = BoxFuture<'static, Result<R, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let registry = Arc::clone(&self.registry);
        let key = Arc::clone(&self.key);
        Box::pin(async move {
            // 取键也可能执行用户逻辑，因此放在原执行保护与追踪范围之内。
            crate::Layer
                .layer(tower::service_fn(move |request: Request<T>| {
                    let registry = Arc::clone(&registry);
                    let key = key(&request);
                    async move { registry.dispatch(&key, request).await }
                }))
                .oneshot(request)
                .await
                .map_err(|error| match error {
                    crate::Error::Inner(error) => error,
                    crate::Error::Stopped(reason) => DispatchError::Stopped(reason),
                })
        })
    }
}
