//! 将服务级事件订阅与服务初始化和任务监督组合。
use crate::{
    Error as EventError, Handler, Message, Outcome, Subscriber, Task, TopicFilter,
    interest::Transport,
};
use std::{sync::Arc, time::Duration};
use tok_execution::{Request, Response};
use tok_lifecycle::Init;

/// 服务级订阅的显式路由、处理类型及本地能力。
pub struct Config {
    /// 受监督任务名称。
    pub name: String,
    /// 订阅的事件类型；事件名由业务定义。
    pub event_types: Vec<String>,
    /// 传输主题过滤。
    pub topics: TopicFilter,
    /// 消费组；None 表示每个订阅实例独立接收。
    pub group: Option<String>,
    /// 允许传给此服务事件处理链的本地能力；不自动复制源端能力。
    pub extensions: tok_core::Extensions,
    /// 订阅建立的最大等待时间。
    pub timeout: Duration,
}

/// 服务级订阅的装配错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 事件订阅参数或后端失败。
    #[error(transparent)]
    Event(#[from] EventError),
    /// 无法预留或启动受监督任务。
    #[error(transparent)]
    Task(#[from] tok_lifecycle::task::SubmitError),
    /// 服务停止导致初始化取消。
    #[error("服务已停止，无法安装事件订阅")]
    Stopped,
    /// 订阅未在期限内建立。
    #[error("事件订阅建立超时")]
    Timeout,
}

type Chain = tower::util::BoxCloneSyncService<Request<Arc<Message>>, Response<Outcome>, EventError>;
struct Adapter {
    names: Vec<String>,
    chain: Chain,
}

// 初始化 future 被丢弃或提交失败时也通知传输释放资源。
struct CancelOnDrop(Option<tokio_util::sync::CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            stop.cancel();
        }
    }
}
impl Handler for Adapter {
    fn event_types(&self) -> Vec<&str> {
        self.names.iter().map(String::as_str).collect()
    }
    fn handle(&self, request: Request<Arc<Message>>) -> Task<'_> {
        let chain = self.chain.clone();
        Box::pin(async move {
            tok_execution::Call::new(chain, request)
                .await
                .map_err(|error| match error {
                    tok_execution::Error::Inner(error) => error,
                    tok_execution::Error::Stopped(reason) => EventError::Stopped(reason),
                })
        })
    }
}

/// 将服务级事件注册器接入 Tower，克隆共享同一注册快照。
#[derive(Clone)]
#[expect(
    clippy::module_name_repetitions,
    reason = "名称区分事件注册表与其 Tower 适配器。"
)]
pub struct RegistryService(Arc<crate::Registry>);
impl RegistryService {
    /// 构造远程事件处理链，不建立订阅。
    pub fn new(registry: Arc<crate::Registry>) -> Self {
        Self(registry)
    }
    /// 已启用远程解码的事件类型，可用于配置订阅。
    pub fn event_types(&self) -> Vec<String> {
        self.0
            .event_types()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
impl tower::Service<Request<Arc<Message>>> for RegistryService {
    type Response = Response<Outcome>;
    type Error = EventError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Arc<Message>>) -> Self::Future {
        let registry = Arc::clone(&self.0);
        Box::pin(async move {
            match registry.receive(&mut (), request).await {
                Ok(results) => Ok(results.map(Outcome::combine)),
                Err(crate::DispatchError::Unknown) => {
                    Err(EventError::InvalidInput("事件未注册或未安装解码器"))
                }
                Err(crate::DispatchError::Decode(_)) => {
                    Err(EventError::InvalidInput("事件正文解码失败"))
                }
                Err(
                    crate::DispatchError::Invalid(error) | crate::DispatchError::Handler(error),
                ) => Err(error),
                Err(crate::DispatchError::Stopped(reason)) => Err(EventError::Stopped(reason)),
            }
        })
    }
}

/// 使用注册表安装订阅，并从其解码器推导事件类型；可通过闭包组合外层 Tower 链。
///
/// `config.event_types` 为空时自动填充；非空时必须与注册表一致，避免订阅后静默忽略事件。
pub async fn install_registry<S>(
    init: &Init,
    transport: &dyn Transport,
    mut config: Config,
    registry: RegistryService,
    layer: impl FnOnce(RegistryService) -> S,
) -> Result<(), Error>
where
    S: tower::Service<Request<Arc<Message>>, Response = Response<Outcome>, Error = EventError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let types = registry.event_types();
    if !config.event_types.is_empty() {
        let mut configured = config.event_types.clone();
        configured.sort();
        let mut registered = types.clone();
        registered.sort();
        if configured != registered {
            return Err(EventError::InvalidInput("订阅类型与事件注册表不一致").into());
        }
    }
    config.event_types = types;
    install(init, transport, config, layer(registry)).await
}

/// 在初始化期间建立并确认订阅，再交由服务任务范围监督。
///
/// 先预留容量；失败、超时或服务停止不会提交消费任务。订阅本身的恢复和确认
/// 语义由传输决定；返回成功不代表事件业务已执行。服务停止时取消并等待传输清理。
pub async fn install<S>(
    init: &Init,
    transport: &dyn Transport,
    config: Config,
    service: S,
) -> Result<(), Error>
where
    S: tower::Service<Request<Arc<Message>>, Response = Response<Outcome>, Error = EventError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    if config.timeout.is_zero() {
        return Err(EventError::InvalidInput("订阅期限必须大于零").into());
    }
    if config.name.trim().is_empty() || config.name.len() > 256 {
        return Err(tok_lifecycle::task::SubmitError::InvalidName.into());
    }
    if config.event_types.is_empty() {
        return Err(EventError::InvalidInput("订阅事件类型不能为空").into());
    }
    let mut names = std::collections::BTreeSet::new();
    for name in &config.event_types {
        crate::validate_name(name)?;
        if !names.insert(name) {
            return Err(EventError::InvalidInput("订阅事件类型重复").into());
        }
    }
    let reservation = init.service.tasks().reserve()?;
    let adapter = Arc::new(Adapter {
        names: config.event_types,
        chain: Chain::new(service),
    });
    let subscriber = Subscriber::new(
        init.identity.clone(),
        adapter,
        config.topics,
        config.group.as_deref(),
    )?
    .with_extensions(config.extensions);
    let stop = init.service.cancellation().child_token();
    let mut cleanup = CancelOnDrop(Some(stop.clone()));
    let result = tokio::select! {
        biased;
        () = stop.cancelled() => return Err(Error::Stopped),
        result = tokio::time::timeout(config.timeout, transport.subscribe(subscriber, stop.clone())) => result,
    };
    let consumer = match result {
        Ok(Ok(consumer)) => consumer,
        Ok(Err(error)) => {
            stop.cancel();
            return Err(error.into());
        }
        Err(_) => {
            stop.cancel();
            return Err(Error::Timeout);
        }
    };
    let task_stop = stop.clone();
    let task: tok_lifecycle::task::Task = Box::pin(async move {
        let result = consumer.await;
        let result = if task_stop.is_cancelled() {
            result
        } else {
            result.and(Err(EventError::Unavailable("事件消费任务意外结束")))
        };
        result.map_err(|error| Box::new(error) as tok_lifecycle::task::BoxError)
    });
    if let Err(error) = reservation.launch(config.name, task).await {
        stop.cancel();
        return Err(error.into());
    }
    cleanup.0 = None;
    Ok(())
}
