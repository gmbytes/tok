//! 根据调用方定义的键选择处理链，不限定消息 ID、输入类型或编码格式。
//!
//! Registry 只负责选链和在原 Context 下执行，编解码由协议适配层负责。
use crate::Request;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tok_core::BoxFuture;
use tower::{Layer as _, Service, ServiceExt};

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
        S: Service<Request<T>, Response = R, Error = E> + Clone + Send + Sync + 'static,
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

    /// 按键分发，保留原操作名、追踪、扩展、期限及取消；键与输入独立传递。
    pub async fn dispatch(&self, key: &K, request: Request<T>) -> Result<R, DispatchError<K, E>> {
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
