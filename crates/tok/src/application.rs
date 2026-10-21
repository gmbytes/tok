//! 配置驱动的服务生命周期；具体服务由游戏注册。
mod rpc;

use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use tok_core::discovery::ServiceRegistry;
use tok_core::{Extensions, service::Identity};
use tok_core::{ServiceDescriptor, ServiceDirectory};
use tok_infra::discovery::{MemoryDirectory, redis::RedisDirectory};
use tok_lifecycle as lifecycle;
use tok_lifecycle::{Builder, Factory, Handle, task::BoxError};
use tok_rpc::{Listener, Readiness};
use tokio::task::{JoinError, JoinSet};
/// 服务协作退出的取消令牌；详细 API 见 tokio-util 上游文档。
pub use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{AppConfig, DiscoveryBackend};

/// Application 注入工厂扩展的端点与发现能力；不属于通用执行上下文。
#[derive(Clone)]
pub struct Binding {
    /// 本地监听地址，可能与发布地址不同。
    pub listen_address: std::net::SocketAddr,
    /// 本节点的服务发布描述。
    pub service: ServiceDescriptor,
    /// 业务调用使用的发现查询后端。
    pub directory: Arc<dyn ServiceDirectory>,
}

/// 服务与端点启动、运行监督及退出清理过程的错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 配置校验或本地目录操作失败。
    #[error(transparent)]
    Core(#[from] tok_core::Error),
    /// RPC 入口的组装错误。
    #[error(transparent)]
    Rpc(#[from] tok_rpc::Error),
    /// 配置选用的初始化工厂尚未注册。
    #[error("初始化工厂未注册：{0}")]
    MissingFactory(String),
    /// 同一名称重复注册初始化工厂。
    #[error("初始化工厂重复注册：{0}")]
    DuplicateFactory(String),
    /// 业务服务初始化或共享端点绑定失败。
    #[error("服务 {service_name} 启动失败：{source}")]
    Start {
        /// 初始化失败的业务服务或端点任务名称。
        service_name: String,
        /// 原始初始化错误。
        source: BoxError,
    },
    /// 初始化未在指定期限内完成，携带任务名称。
    #[error("服务 {0} 启动超时")]
    StartTimeout(String),
    /// 业务服务或共享端点运行失败。
    #[error("服务 {service_name} 运行失败：{source}")]
    Run {
        /// 失败的业务服务或端点任务名称。
        service_name: String,
        /// 原始运行错误。
        source: BoxError,
    },
    /// 任务在应用通知退出前正常结束，携带任务名称。
    #[error("服务 {0} 在退出通知前结束")]
    UnexpectedExit(String),
    /// 初始化或运行任务崩溃等 JoinError。
    #[error("服务任务异常：{0}")]
    Task(#[from] JoinError),
    /// 协作清理超时，剩余任务已中止。
    #[error("服务未在退出期限内完成清理，已中止")]
    ShutdownTimeout,
    /// 退出信号监听或调用方提供的退出 future 失败。
    #[error("退出信号监听失败：{0}")]
    Signal(#[from] std::io::Error),
}

/// 组装业务工厂与共享 RPC 端点，统一监督初始化、运行和退出。
pub struct Application {
    config: AppConfig,
    factories: BTreeMap<String, Arc<dyn Factory>>,
    directory: MemoryDirectory,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    max_service_tasks: usize,
    rpc_endpoints: BTreeMap<String, Listener>,
    rpc_registrations: Vec<rpc::Registration>,
    service_directory: Arc<dyn ServiceDirectory>,
    registry: Arc<dyn ServiceRegistry>,
    redis: Option<Arc<RedisDirectory>>,
}

// 即使调用者丢弃运行 future，也撤销目录并通知服务；JoinSet 随后中止任务。
struct Registrations {
    directory: MemoryDirectory,
    shutdown: CancellationToken,
    services: Vec<ServiceDescriptor>,
    running_services: Vec<String>,
    rpc_readiness: Vec<Readiness>,
    service_handles: Vec<Handle>,
}

impl Drop for Registrations {
    fn drop(&mut self) {
        for readiness in &self.rpc_readiness {
            readiness.clear();
        }
        for service in self.services.iter().rev() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "析构时尽力撤销本地注册，目录不可用时不能再次失败中断退出。"
            )]
            let _ = self
                .directory
                .unregister(&service.node_id, &service.service_name);
        }
        for service in &self.running_services {
            metrics::gauge!("tok_services_running", "service_name" => service.clone())
                .decrement(1.0);
        }
        self.shutdown.cancel();
    }
}

impl Application {
    /// 创建内存或 Redis 开发发现应用，不连接网络；Kubernetes 后端尚未实现。
    pub fn new(config: AppConfig) -> Result<Self, Error> {
        config.validate()?;
        if config.discovery == DiscoveryBackend::Kubernetes {
            return Err(tok_core::Error::Unsupported("Kubernetes 服务发现").into());
        }
        let directory = MemoryDirectory::default();
        let redis = if config.discovery == DiscoveryBackend::Redis {
            Some(RedisDirectory::new(
                config.redis_discovery.clone().ok_or_else(|| {
                    tok_core::Error::InvalidConfig("Redis 发现缺少 redis_discovery 配置".into())
                })?,
                &config.node_id,
            )?)
        } else {
            None
        };
        let service_directory: Arc<dyn ServiceDirectory> = redis
            .as_ref()
            .map(|r| Arc::clone(r) as Arc<dyn ServiceDirectory>)
            .unwrap_or_else(|| Arc::new(directory.clone()));
        let registry: Arc<dyn ServiceRegistry> = redis
            .as_ref()
            .map(|r| Arc::clone(r) as Arc<dyn ServiceRegistry>)
            .unwrap_or_else(|| Arc::new(directory.clone()));
        Ok(Self {
            config,
            factories: BTreeMap::new(),
            directory: directory.clone(),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            max_service_tasks: 64,
            rpc_endpoints: BTreeMap::new(),
            rpc_registrations: Vec::new(),
            service_directory,
            registry,
            redis,
        })
    }

    /// 按名称注册初始化工厂；服务默认按服务名查找，也可通过 factory 显式选择。
    pub fn register_factory(
        &mut self,
        name: impl Into<String>,
        factory: impl Factory + 'static,
    ) -> Result<(), Error> {
        let name = name.into();
        if self.factories.contains_key(&name) {
            return Err(Error::DuplicateFactory(name));
        }
        self.factories.insert(name, Arc::new(factory));
        Ok(())
    }

    /// 本进程的注册目录；服务调用应使用 service_directory 查询所选发现后端。
    pub fn directory(&self) -> MemoryDirectory {
        self.directory.clone()
    }

    /// 注入服务查询和变更订阅后端；本进程的注册清理仍由 Application 管理。
    pub fn with_service_directory(mut self, directory: Arc<dyn ServiceDirectory>) -> Self {
        self.service_directory = directory;
        self
    }

    /// 返回业务调用应使用的发现查询后端。
    pub fn service_directory(&self) -> Arc<dyn ServiceDirectory> {
        Arc::clone(&self.service_directory)
    }

    /// 将显式绑定的 protobuf 服务交给应用统一监听和监督。
    ///
    /// 端点必须是已配置的 grpc 端点，其业务绑定必须与配置完全一致。
    /// 每个业务服务选用的工厂仍需通过 register_factory 注册，完成初始化后才接受 RPC。
    pub fn register_rpc_endpoint(
        &mut self,
        endpoint_name: impl Into<String>,
        endpoint: Listener,
    ) -> Result<(), Error> {
        let endpoint_name = endpoint_name.into();
        let config = self
            .config
            .endpoints
            .iter()
            .find(|entry| entry.name == endpoint_name)
            .ok_or_else(|| {
                tok_core::Error::InvalidConfig(format!("RPC 端点未配置：{endpoint_name}"))
            })?;
        let expected: std::collections::BTreeSet<_> = self
            .config
            .services
            .iter()
            .filter(|service| service.endpoint.as_deref() == Some(endpoint_name.as_str()))
            .map(|service| service.service_name.as_str())
            .collect();
        let actual: std::collections::BTreeSet<_> = endpoint.service_names().collect();
        if config.protocol != "grpc"
            || expected.is_empty()
            || expected != actual
            || self.rpc_endpoints.contains_key(&endpoint_name)
            || self
                .rpc_registrations
                .iter()
                .any(|entry| entry.endpoint == endpoint_name)
        {
            return Err(tok_core::Error::InvalidConfig(format!(
                "RPC 端点协议、业务绑定无效或重复：{endpoint_name}"
            ))
            .into());
        }
        self.rpc_endpoints.insert(endpoint_name, endpoint);
        Ok(())
    }

    /// 设置每个服务待启动及执行中的后台任务上限，范围为 1 至 65536。
    pub fn with_task_capacity(mut self, max_tasks: usize) -> Result<Self, Error> {
        if max_tasks == 0 || max_tasks > 65_536 {
            return Err(
                tok_core::Error::InvalidConfig("服务后台任务容量须为 1 至 65536".into()).into(),
            );
        }
        self.max_service_tasks = max_tasks;
        Ok(self)
    }

    /// 每个服务的初始化期限，以及整个进程的协作退出期限。
    pub fn with_timeouts(mut self, startup: Duration, shutdown: Duration) -> Self {
        self.startup_timeout = startup;
        self.shutdown_timeout = shutdown;
        self
    }

    /// 按配置顺序初始化；失败或退出信号到达时撤销注册并清理已启动服务。
    pub async fn run_until(
        mut self,
        shutdown: impl Future<Output = std::io::Result<()>>,
    ) -> Result<(), Error> {
        for service in &self.config.services {
            if !self.factories.contains_key(service.factory_name()) {
                return Err(Error::MissingFactory(service.factory_name().into()));
            }
        }
        self.assemble_rpc()?;
        let token = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let mut registered = Registrations {
            directory: self.directory.clone(),
            shutdown: token.clone(),
            services: Vec::new(),
            running_services: Vec::new(),
            rpc_readiness: Vec::new(),
            service_handles: Vec::new(),
        };
        tokio::pin!(shutdown);
        let outcome = Box::pin(async {
            if let Some(redis) = &self.redis {
                tokio::select! {
                    biased;
                    signal = &mut shutdown => return signal.map_err(Error::Signal),
                    result = redis.initialize() => result?,
                }
                let redis = Arc::clone(redis);
                let discovery_stop = token.child_token();
                tasks.spawn(async move {
                    ("redis-discovery".into(), redis.run(discovery_stop).await.map_err(|error| Box::new(error) as BoxError))
                });
            }
            // 端点只监听一次；业务服务初始化前由就绪拦截器阻止调用。
            for (name, endpoint) in std::mem::take(&mut self.rpc_endpoints) {
                let readiness = endpoint.readiness();
                registered.rpc_readiness.push(readiness);
                let address = self.config.endpoints.iter().find(|entry| entry.name == name)
                    .ok_or_else(|| tok_core::Error::InvalidConfig(format!("RPC 端点未配置：{name}")))?.address;
                let endpoint_name = format!("rpc-endpoint:{name}");
                let task = tokio::select! {
                    biased;
                    signal = &mut shutdown => return signal.map_err(Error::Signal),
                    result = tasks.join_next(), if !tasks.is_empty() => return Err(unexpected(result)),
                    result = tokio::time::timeout(self.startup_timeout, endpoint.bind(address, token.child_token())) => {
                        result.map_err(|_| Error::StartTimeout(endpoint_name.clone()))?
                            .map_err(|source| Error::Start { service_name: endpoint_name.clone(), source: Box::new(source) })?
                    }
                };
                tasks.spawn(async move { (endpoint_name, task.await.map_err(|source| Box::new(source) as BoxError)) });
            }
            let descriptors: BTreeMap<_, _> = self.config.service_descriptors()?.into_iter().map(|service| (service.service_name.clone(), service)).collect();
            for configured in &self.config.services {
                let service_name = &configured.service_name;
                let network = descriptors.get(service_name);
                let builder = Builder::new(Identity { node_id: self.config.node_id.clone(), service_name: service_name.clone() }, self.max_service_tasks, token.child_token())?;
                registered.service_handles.push(builder.handle());
                let mut extensions = Extensions::default();
                if let Some(service) = network {
                    let listen_address = self.config.endpoints.iter().find(|endpoint| Some(endpoint.name.as_str()) == configured.endpoint.as_deref())
                        .ok_or_else(|| tok_core::Error::InvalidConfig(format!("服务端点未配置：{service_name}")))?.address;
                    extensions.insert(Binding { listen_address, service: service.clone(), directory: Arc::clone(&self.service_directory) });
                }
                let factory = Arc::clone(self.factories.get(configured.factory_name())
                    .ok_or_else(|| Error::MissingFactory(configured.factory_name().into()))?);
                let started = std::time::Instant::now();
                let span = tracing::info_span!("service_start", node_id = %self.config.node_id, service_name = %service_name);
                let prepared = tokio::select! {
                    biased;
                    signal = &mut shutdown => return signal.map_err(Error::Signal),
                    result = tasks.join_next(), if !tasks.is_empty() => return Err(unexpected(result)),
                    result = builder.build(factory, extensions, self.startup_timeout).instrument(span) => {
                        result.map_err(|error| service_error(service_name, error, true))?
                    }
                };
                let running = prepared.start();
                // 发布可能失败或被取消；先把主任务与运行范围交给监督，确保仍会执行退出清理。
                let task_name = service_name.clone();
                let shutdown_timeout = self.shutdown_timeout;
                tasks.spawn(async move {
                    let result = running.run(shutdown_timeout).await.map_err(|error| Box::new(error) as BoxError);
                    (task_name, result)
                });
                for readiness in &registered.rpc_readiness {
                    readiness.set(service_name, true);
                }
                if let Some(service) = network {
                    // 快照对外可见时对应入口必须已就绪，不能先发布再开启入口。
                    let publication = tokio::select! {
                        biased;
                        signal = &mut shutdown => return signal.map_err(Error::Signal),
                        result = tasks.join_next(), if !tasks.is_empty() => return Err(unexpected(result)),
                        result = self.registry.register(service.clone()) => result,
                    };
                    if let Err(error) = publication {
                        for readiness in &registered.rpc_readiness {
                            readiness.set(service_name, false);
                        }
                        return Err(error.into());
                    }
                    if self.redis.is_some() { self.directory.register(service.clone())?; }
                    registered.services.push(service.clone());
                }
                metrics::histogram!("tok_service_start_duration_seconds", "service_name" => service_name.clone()).record(started.elapsed().as_secs_f64());
                metrics::counter!("tok_service_starts_total", "service_name" => service_name.clone()).increment(1);
                metrics::gauge!("tok_services_running", "service_name" => service_name.clone()).increment(1.0);
                registered.running_services.push(service_name.clone());
            }
            tokio::select! {
                biased;
                signal = &mut shutdown => signal.map_err(Error::Signal),
                result = tasks.join_next() => Err(unexpected(result)),
            }
        }).await;

        if let Err(error) = &outcome {
            metrics::counter!("tok_application_failures_total").increment(1);
            tracing::error!(node_id = %self.config.node_id, error = %error, "服务运行失败，开始清理");
        }

        let mut cleanup_error = None;
        for readiness in &registered.rpc_readiness {
            readiness.clear();
        }
        for service in registered.running_services.drain(..) {
            metrics::gauge!("tok_services_running", "service_name" => service).decrement(1.0);
        }
        for service in registered.services.drain(..).rev() {
            if let Err(error) = self
                .directory
                .unregister(&service.node_id, &service.service_name)
            {
                cleanup_error.get_or_insert(Error::Core(error));
            }
        }
        token.cancel();
        let drain = async {
            // 停止续租后先撤销远端可见性，再等待业务任务收束；全程受退出期限约束。
            if let Some(redis) = &self.redis
                && let Err(error) = redis.withdraw().await
            {
                cleanup_error.get_or_insert(Error::Core(error));
            }
            while let Some(result) = tasks.join_next().await {
                let error = match result {
                    Ok((service_name, Err(source))) => Some(task_error(service_name, source)),
                    Err(error) => Some(Error::Task(error)),
                    _ => None,
                };
                if let Some(error) = error {
                    cleanup_error.get_or_insert(error);
                }
            }
            for handle in &registered.service_handles {
                handle.wait_finished().await;
            }
        };
        if tokio::time::timeout(self.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tasks.shutdown().await;
            for handle in &registered.service_handles {
                handle.wait_finished().await;
            }
            tracing::error!("服务退出超时，中止剩余任务");
            cleanup_error.get_or_insert(Error::ShutdownTimeout);
        }
        outcome?;
        cleanup_error.map_or(Ok(()), Err)
    }
}

type ServiceCompletion = Result<(String, Result<(), BoxError>), JoinError>;

fn unexpected(result: Option<ServiceCompletion>) -> Error {
    match result {
        Some(Ok((service_name, Ok(())))) => Error::UnexpectedExit(service_name),
        Some(Ok((service_name, Err(source)))) => task_error(service_name, source),
        Some(Err(error)) => Error::Task(error),
        None => Error::UnexpectedExit("运行时任务集合".into()),
    }
}

fn task_error(service_name: String, source: BoxError) -> Error {
    match source.downcast::<tok_lifecycle::Error>() {
        Ok(error) => service_error(&service_name, *error, false),
        Err(source) => Error::Run {
            service_name,
            source,
        },
    }
}

fn service_error(name: &str, error: tok_lifecycle::Error, starting: bool) -> Error {
    match error {
        lifecycle::Error::InitializationTimeout => Error::StartTimeout(name.into()),
        lifecycle::Error::Join(error) => Error::Task(error),
        lifecycle::Error::ShutdownTimeout => Error::ShutdownTimeout,
        lifecycle::Error::UnexpectedExit => Error::UnexpectedExit(name.into()),
        error => {
            let source: BoxError = match error {
                lifecycle::Error::Initialize(source) | lifecycle::Error::Run { source, .. } => {
                    source
                }
                error => Box::new(error),
            };
            if starting {
                Error::Start {
                    service_name: name.into(),
                    source,
                }
            } else {
                Error::Run {
                    service_name: name.into(),
                    source,
                }
            }
        }
    }
}

/// Windows 监听 Ctrl+C；Unix 同时监听 SIGTERM，适配容器退出。
pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
