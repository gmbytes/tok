//! 根据调用方定义的键选择处理链，不限定消息 ID、输入类型或编码格式。
//!
//! Registry 只负责选链和在原 Context 下执行，编解码由协议适配层负责。
use crate::{Request, Response};
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
    /// 处理链返回的原始错误。
    #[error("处理链执行失败：{0}")]
    Handler(#[source] E),
}

type HandlerFn<T, R, E> =
    dyn Fn(Request<T>) -> BoxFuture<'static, Result<Response<R>, crate::Error<E>>> + Send + Sync;

/// 可克隆的已擦除处理链；克隆共享链工厂，每次调用克隆内层 Service。
/// 就绪等待与执行始终处于请求原有的期限、取消和追踪范围内。
pub struct Handler<T, R, E> {
    call: Arc<HandlerFn<T, R, E>>,
}
impl<T, R, E> Clone for Handler<T, R, E> {
    fn clone(&self) -> Self {
        Self {
            call: Arc::clone(&self.call),
        }
    }
}
impl<T: Send + 'static, R: Send + 'static, E: Send + 'static> Handler<T, R, E> {
    /// 擦除具体 Tower 链类型，不启动任务或添加重试。
    pub fn new<S>(service: S) -> Self
    where
        S: TowerService<Request<T>, Response = Response<R>, Error = E>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        Self {
            call: Arc::new(move |request| Box::pin(crate::Call::new(service.clone(), request))),
        }
    }
    /// 执行此链；不会重建上下文或放大预算。
    pub fn call(
        &self,
        request: Request<T>,
    ) -> BoxFuture<'static, Result<Response<R>, crate::Error<E>>> {
        (self.call)(request)
    }
}

/// 处理链的注册与派发契约；存储、选择和动态更新策略由实现决定。
///
/// 实现可以委托、拦截或选择备用处理链；不要求 Key 支持排序或 Clone。
/// 必须保留 Request / Response 扩展、原期限、取消与追踪，不隐式重试。
/// 注册失败不得破坏已有注册；克隆后的更新可见性由具体实现说明。
/// 可作为 dyn Registry 使用；泛型注册便捷方法与 into_service 用于自有实现。
pub trait Registry: Send + Sync {
    /// 注册与查找使用的键。
    type Key;
    /// 处理链输入正文。
    type Input;
    /// 处理链成功结果正文。
    type Output;
    /// 注册处理链的原始错误。
    type HandlerError;
    /// 注册器自身的路由和处理错误；执行停止由外层 Error 统一表达。
    type DispatchError;
    /// 注册失败；由实现保留自己的错误类型。
    type RegistrationError;

    /// 注册已擦除的处理链，具体实现决定存储与重复键规则。
    fn register_handler(
        &mut self,
        key: Self::Key,
        handler: Handler<Self::Input, Self::Output, Self::HandlerError>,
    ) -> Result<(), Self::RegistrationError>;

    /// 在原调用范围中按键派发；执行停止使用 Error::Stopped，其余失败使用 Error::Inner。
    /// 可以借用当前注册器与键，不自行 spawn；必须接续原期限、取消与追踪。
    fn dispatch<'a>(
        &'a self,
        key: &'a Self::Key,
        request: Request<Self::Input>,
    ) -> BoxFuture<'a, crate::Result<Response<Self::Output>, Self::DispatchError>>;

    /// 注册异步回调；需要中间件时使用 register_service。
    fn register<F, Fut>(
        &mut self,
        key: Self::Key,
        handler: F,
    ) -> Result<(), Self::RegistrationError>
    where
        Self: Sized,
        Self::Input: Send + 'static,
        Self::Output: Send + 'static,
        Self::HandlerError: Send + 'static,
        F: Fn(Request<Self::Input>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Self::Output>, Self::HandlerError>> + Send + 'static,
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

    /// 安装具体 Tower 链，共同执行保护由 Handler 提供。
    fn register_service<S>(
        &mut self,
        key: Self::Key,
        service: S,
    ) -> Result<(), Self::RegistrationError>
    where
        Self: Sized,
        Self::Input: Send + 'static,
        Self::Output: Send + 'static,
        Self::HandlerError: Send + 'static,
        S: TowerService<
                Request<Self::Input>,
                Response = Response<Self::Output>,
                Error = Self::HandlerError,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.register_handler(key, Handler::new(service))
    }

    /// 转为按请求取键的 Tower Service，选择和派发均受原执行范围保护。
    fn into_service<F>(self, key: F) -> Service<Self, F>
    where
        Self: Sized,
        F: Fn(&Request<Self::Input>) -> Self::Key + Send + Sync + 'static,
    {
        Service::new(Arc::new(self), key)
    }
}

/// 基于 BTreeMap 的默认静态注册表，重复键拒绝注册；不入队、不重试。
/// 克隆复制键表并共享已有处理链，后续注册不改变旧克隆；链的并发状态由其 Layer 共享。
pub struct Table<K, T, R, E = std::convert::Infallible> {
    handlers: BTreeMap<K, Handler<T, R, E>>,
}
impl<K, T, R, E> Default for Table<K, T, R, E> {
    fn default() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }
}
impl<K: Clone, T, R, E> Clone for Table<K, T, R, E> {
    fn clone(&self) -> Self {
        Self {
            handlers: self.handlers.clone(),
        }
    }
}
impl<K, T, R, E> Table<K, T, R, E> {
    /// 创建空的静态注册表。
    pub fn new() -> Self {
        Self::default()
    }
}
impl<K, T, R, E> Registry for Table<K, T, R, E>
where
    K: Ord + Clone + Send + Sync + 'static,
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    type Key = K;
    type Input = T;
    type Output = R;
    type HandlerError = E;
    type DispatchError = DispatchError<K, E>;
    type RegistrationError = Error<K>;
    fn register_handler(&mut self, key: K, handler: Handler<T, R, E>) -> Result<(), Error<K>> {
        if self.handlers.contains_key(&key) {
            return Err(Error::DuplicateKey(key));
        }
        self.handlers.insert(key, handler);
        Ok(())
    }
    fn dispatch<'a>(
        &'a self,
        key: &'a K,
        request: Request<T>,
    ) -> BoxFuture<'a, Result<Response<R>, crate::Error<DispatchError<K, E>>>> {
        Box::pin(async move {
            request.context.check()?;
            let handler = self
                .handlers
                .get(key)
                .ok_or_else(|| crate::Error::Inner(DispatchError::Unknown(key.clone())))?;
            handler
                .call(request)
                .await
                .map_err(|error| error.map_inner(DispatchError::Handler))
        })
    }
}

/// Registry 的 Tower 路由适配；支持自有实现以及 `Arc<dyn Registry>`。
/// poll_ready 不预留目标容量；取键、派发与内层就绪等待共用原期限与取消范围。
/// 克隆共享注册器和取键函数，动态更新的可见性由注册器实现决定。
pub struct Service<G: ?Sized, F> {
    registry: Arc<G>,
    key: Arc<F>,
}
impl<G: Registry + ?Sized, F> Service<G, F> {
    /// 注入共享注册器与取键函数；可使用 trait 对象实现运行时选择。
    pub fn new(registry: Arc<G>, key: F) -> Self {
        Self {
            registry,
            key: Arc::new(key),
        }
    }
}
impl<G: ?Sized, F> Clone for Service<G, F> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            key: Arc::clone(&self.key),
        }
    }
}
impl<G, F> TowerService<Request<G::Input>> for Service<G, F>
where
    G: Registry + ?Sized + 'static,
    G::Key: Send + Sync + 'static,
    G::Input: Send + 'static,
    G::Output: Send + 'static,
    G::DispatchError: Send + 'static,
    F: Fn(&Request<G::Input>) -> G::Key + Send + Sync + 'static,
{
    type Response = Response<G::Output>;
    type Error = crate::Error<G::DispatchError>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<G::Input>) -> Self::Future {
        let registry = Arc::clone(&self.registry);
        let key = Arc::clone(&self.key);
        Box::pin(async move {
            crate::Layer
                .layer(tower::service_fn(move |request: Request<G::Input>| {
                    let registry = Arc::clone(&registry);
                    let key = key(&request);
                    async move { registry.dispatch(&key, request).await }
                }))
                .oneshot(request)
                .await
                .map_err(crate::Error::flatten)
        })
    }
}
