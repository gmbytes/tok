//! 配置驱动的服务生命周期；具体服务由游戏注册。

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use tok_core::{ServiceDescriptor, ServiceDirectory};
use tok_infra::discovery::MemoryDirectory;
use tok_rpc::{RpcEndpoint, RpcReadiness};
use tokio::task::{JoinError, JoinSet};
pub use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{AppConfig, DiscoveryBackend};

/// 服务初始化和运行错误，可由游戏定义具体错误类型。
pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;
/// 持续运行至取消通知的任务，负责清理自身资源。
pub type ServiceTask = Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send>>;
/// 可取消的初始化任务，成功后返回持续运行任务。
pub type StartTask = Pin<Box<dyn Future<Output = Result<ServiceTask, ServiceError>> + Send>>;

/// 服务初始化时提供的身份、发现后端及退出通知。
#[derive(Clone)]
pub struct ServiceContext {
    /// 本节点服务身份与端点描述。
    pub service: ServiceDescriptor,
    /// 可注入的服务快照和变更订阅来源。
    pub directory: Arc<dyn ServiceDirectory>,
    /// 应用退出时取消；服务应停止新工作并协作清理。
    pub shutdown: CancellationToken,
}

/// start 返回前完成初始化；返回的任务负责持续运行及收到取消后的清理。
/// 启动 future 必须可取消，持有资源应通过 RAII 释放，不得遗留脱管任务。
pub trait ServiceFactory: Send + Sync {
    /// 准备业务资源，成功返回后才发布目录并开启对应 RPC 入口。
    fn start(&self, context: ServiceContext) -> StartTask;
}

/// 服务与端点启动、运行监督及退出清理过程的错误。
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// 配置校验或本地目录操作失败。
    #[error(transparent)]
    Core(#[from] tok_core::Error),
    /// 配置启用的服务没有注册工厂。
    #[error("服务未注册：{0}")]
    MissingService(String),
    /// 同一业务服务名重复注册工厂。
    #[error("服务重复注册：{0}")]
    DuplicateService(String),
    /// 业务服务初始化或共享端点绑定失败。
    #[error("服务 {service_name} 启动失败：{source}")]
    Start {
        /// 初始化失败的业务服务或端点任务名称。
        service_name: String,
        /// 原始初始化错误。
        source: ServiceError,
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
        source: ServiceError,
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
    factories: BTreeMap<String, Arc<dyn ServiceFactory>>,
    directory: MemoryDirectory,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    rpc_endpoints: BTreeMap<String, RpcEndpoint>,
    service_directory: Arc<dyn ServiceDirectory>,
}

// 即使调用者丢弃运行 future，也撤销目录并通知服务；JoinSet 随后中止任务。
struct Registrations {
    directory: MemoryDirectory,
    shutdown: CancellationToken,
    services: Vec<ServiceDescriptor>,
    rpc_readiness: Vec<RpcReadiness>,
}

impl Drop for Registrations {
    fn drop(&mut self) {
        for readiness in &self.rpc_readiness {
            readiness.clear();
        }
        for service in self.services.iter().rev() {
            let _ = self
                .directory
                .unregister(&service.node_id, &service.service_name);
            metrics::gauge!("tok_services_running", "service_name" => service.service_name.clone())
                .decrement(1.0);
        }
        self.shutdown.cancel();
    }
}

impl Application {
    /// 创建内存发现应用；Kubernetes 后端尚未实现。
    pub fn new(config: AppConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        if config.discovery != DiscoveryBackend::Memory {
            return Err(tok_core::Error::NotImplemented("Kubernetes 服务发现").into());
        }
        let directory = MemoryDirectory::default();
        Ok(Self {
            config,
            factories: BTreeMap::new(),
            directory: directory.clone(),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            rpc_endpoints: BTreeMap::new(),
            service_directory: Arc::new(directory),
        })
    }

    /// 按业务服务名注册初始化工厂；不从服务名推导执行或加锁策略。
    pub fn register(
        &mut self,
        service_name: impl Into<String>,
        factory: impl ServiceFactory + 'static,
    ) -> Result<(), RuntimeError> {
        let service_name = service_name.into();
        if self.factories.contains_key(&service_name) {
            return Err(RuntimeError::DuplicateService(service_name));
        }
        self.factories.insert(service_name, Arc::new(factory));
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
        self.service_directory.clone()
    }

    /// 将显式绑定的 protobuf 服务交给应用统一监听和监督。
    ///
    /// 端点必须是已配置的 grpc 端点，其业务绑定必须与配置完全一致。
    /// 每个业务服务仍需通过 register 注册初始化工厂，完成初始化后才接受 RPC。
    pub fn register_rpc_endpoint(
        &mut self,
        endpoint_name: impl Into<String>,
        endpoint: RpcEndpoint,
    ) -> Result<(), RuntimeError> {
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
            .filter(|service| service.endpoint == endpoint_name)
            .map(|service| service.service_name.as_str())
            .collect();
        let actual: std::collections::BTreeSet<_> = endpoint.service_names().collect();
        if config.protocol != "grpc"
            || expected.is_empty()
            || expected != actual
            || self.rpc_endpoints.contains_key(&endpoint_name)
        {
            return Err(tok_core::Error::InvalidConfig(format!(
                "RPC 端点协议、业务绑定无效或重复：{endpoint_name}"
            ))
            .into());
        }
        self.rpc_endpoints.insert(endpoint_name, endpoint);
        Ok(())
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
    ) -> Result<(), RuntimeError> {
        for service in &self.config.services {
            if !self.factories.contains_key(&service.service_name) {
                return Err(RuntimeError::MissingService(service.service_name.clone()));
            }
        }
        let token = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let mut registered = Registrations {
            directory: self.directory.clone(),
            shutdown: token.clone(),
            services: Vec::new(),
            rpc_readiness: Vec::new(),
        };
        tokio::pin!(shutdown);
        let outcome = async {
            // 端点只监听一次；业务服务初始化前由就绪拦截器阻止调用。
            for (name, endpoint) in std::mem::take(&mut self.rpc_endpoints) {
                let readiness = endpoint.readiness();
                registered.rpc_readiness.push(readiness);
                let address = self.config.endpoints.iter().find(|entry| entry.name == name).expect("注册时已校验端点").address;
                let endpoint_name = format!("rpc-endpoint:{name}");
                let task = tokio::select! {
                    biased;
                    signal = &mut shutdown => return signal.map_err(RuntimeError::Signal),
                    result = tasks.join_next(), if !tasks.is_empty() => return Err(unexpected(result.expect("服务任务集合非空"))),
                    result = tokio::time::timeout(self.startup_timeout, endpoint.bind(address, token.child_token())) => {
                        result.map_err(|_| RuntimeError::StartTimeout(endpoint_name.clone()))?
                            .map_err(|source| RuntimeError::Start { service_name: endpoint_name.clone(), source: Box::new(source) })?
                    }
                };
                tasks.spawn(async move { (endpoint_name, task.await.map_err(|source| Box::new(source) as ServiceError)) });
            }
            for service in self.config.service_descriptors()? {
                let context = ServiceContext {
                    service: service.clone(),
                    directory: self.service_directory.clone(),
                    shutdown: token.child_token(),
                };
                let factory = self.factories[&service.service_name].clone();
                let mut starting = JoinSet::new();
                let started = std::time::Instant::now();
                let span = tracing::info_span!("service_start", node_id = %self.config.node_id, service_name = %service.service_name);
                starting.spawn(async move { factory.start(context).await }.instrument(span));
                let task = tokio::select! {
                    biased;
                    signal = &mut shutdown => return signal.map_err(RuntimeError::Signal),
                    result = tasks.join_next(), if !tasks.is_empty() => return Err(unexpected(result.expect("服务任务集合非空"))),
                    result = tokio::time::timeout(self.startup_timeout, starting.join_next()) => {
                        result.map_err(|_| RuntimeError::StartTimeout(service.service_name.clone()))?
                            .expect("存在一个初始化任务")
                            .map_err(RuntimeError::Task)?
                            .map_err(|source| RuntimeError::Start { service_name: service.service_name.clone(), source })?
                    }
                };
                for readiness in &registered.rpc_readiness {
                    readiness.set(&service.service_name, true);
                }
                // 快照对外可见时对应入口必须已就绪，不能先发布再开启入口。
                if let Err(error) = self.directory.register(service.clone()) {
                    for readiness in &registered.rpc_readiness {
                        readiness.set(&service.service_name, false);
                    }
                    return Err(error.into());
                }
                metrics::histogram!("tok_service_start_duration_seconds", "service_name" => service.service_name.clone()).record(started.elapsed().as_secs_f64());
                metrics::counter!("tok_service_starts_total", "service_name" => service.service_name.clone()).increment(1);
                metrics::gauge!("tok_services_running", "service_name" => service.service_name.clone()).increment(1.0);
                registered.services.push(service.clone());
                tasks.spawn(async move { (service.service_name, task.await) });
            }
            tokio::select! {
                biased;
                signal = &mut shutdown => signal.map_err(RuntimeError::Signal),
                result = tasks.join_next() => Err(unexpected(result.expect("至少配置一个服务"))),
            }
        }.await;

        if let Err(error) = &outcome {
            metrics::counter!("tok_runtime_failures_total").increment(1);
            tracing::error!(node_id = %self.config.node_id, error = %error, "服务运行失败，开始清理");
        }

        let mut cleanup_error = None;
        for readiness in &registered.rpc_readiness {
            readiness.clear();
        }
        for service in registered.services.drain(..).rev() {
            metrics::gauge!("tok_services_running", "service_name" => service.service_name.clone())
                .decrement(1.0);
            if let Err(error) = self
                .directory
                .unregister(&service.node_id, &service.service_name)
            {
                cleanup_error.get_or_insert(RuntimeError::Core(error));
            }
        }
        token.cancel();
        let drain = async {
            while let Some(result) = tasks.join_next().await {
                let error = match result {
                    Ok((service_name, Err(source))) => Some(RuntimeError::Run {
                        service_name,
                        source,
                    }),
                    Err(error) => Some(RuntimeError::Task(error)),
                    _ => None,
                };
                if let Some(error) = error {
                    cleanup_error.get_or_insert(error);
                }
            }
        };
        if tokio::time::timeout(self.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tasks.shutdown().await;
            tracing::error!("服务退出超时，中止剩余任务");
            cleanup_error.get_or_insert(RuntimeError::ShutdownTimeout);
        }
        outcome?;
        cleanup_error.map_or(Ok(()), Err)
    }
}

fn unexpected(result: Result<(String, Result<(), ServiceError>), JoinError>) -> RuntimeError {
    match result {
        Ok((service_name, Ok(()))) => RuntimeError::UnexpectedExit(service_name),
        Ok((service_name, Err(source))) => RuntimeError::Run {
            service_name,
            source,
        },
        Err(error) => RuntimeError::Task(error),
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
