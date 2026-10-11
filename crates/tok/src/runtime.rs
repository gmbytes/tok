//! 配置驱动的服务生命周期；具体服务由游戏注册。

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use tok_core::ServiceDescriptor;
use tok_infra::discovery::MemoryDirectory;
use tokio::task::{JoinError, JoinSet};
pub use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{AppConfig, DiscoveryBackend};

pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;
pub type ServiceTask = Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send>>;
pub type StartTask = Pin<Box<dyn Future<Output = Result<ServiceTask, ServiceError>> + Send>>;

#[derive(Clone)]
pub struct ServiceContext {
    pub service: ServiceDescriptor,
    pub directory: MemoryDirectory,
    pub shutdown: CancellationToken,
}

/// start 返回前完成初始化；返回的任务负责持续运行及收到取消后的清理。
/// 启动 future 必须可取消，持有资源应通过 RAII 释放，不得遗留脱管任务。
pub trait ServiceFactory: Send + Sync {
    fn start(&self, context: ServiceContext) -> StartTask;
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Core(#[from] tok_core::Error),
    #[error("服务未注册：{0}")]
    MissingService(String),
    #[error("服务重复注册：{0}")]
    DuplicateService(String),
    #[error("服务 {service_name} 启动失败：{source}")]
    Start {
        service_name: String,
        source: ServiceError,
    },
    #[error("服务 {0} 启动超时")]
    StartTimeout(String),
    #[error("服务 {service_name} 运行失败：{source}")]
    Run {
        service_name: String,
        source: ServiceError,
    },
    #[error("服务 {0} 在退出通知前结束")]
    UnexpectedExit(String),
    #[error("服务任务异常：{0}")]
    Task(#[from] JoinError),
    #[error("服务未在退出期限内完成清理，已中止")]
    ShutdownTimeout,
    #[error("退出信号监听失败：{0}")]
    Signal(#[from] std::io::Error),
}

pub struct Application {
    config: AppConfig,
    factories: BTreeMap<String, Arc<dyn ServiceFactory>>,
    directory: MemoryDirectory,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

// 即使调用者丢弃运行 future，也撤销目录并通知服务；JoinSet 随后中止任务。
struct Registrations {
    directory: MemoryDirectory,
    shutdown: CancellationToken,
    services: Vec<ServiceDescriptor>,
}

impl Drop for Registrations {
    fn drop(&mut self) {
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
    pub fn new(config: AppConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        if config.discovery != DiscoveryBackend::Memory {
            return Err(tok_core::Error::NotImplemented("Kubernetes 服务发现").into());
        }
        Ok(Self {
            config,
            factories: BTreeMap::new(),
            directory: MemoryDirectory::default(),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
        })
    }

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

    pub fn directory(&self) -> MemoryDirectory {
        self.directory.clone()
    }

    /// 每个服务的初始化期限，以及整个进程的协作退出期限。
    pub fn with_timeouts(mut self, startup: Duration, shutdown: Duration) -> Self {
        self.startup_timeout = startup;
        self.shutdown_timeout = shutdown;
        self
    }

    /// 按配置顺序初始化；失败或退出信号到达时撤销注册并清理已启动服务。
    pub async fn run_until(
        self,
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
        };
        tokio::pin!(shutdown);
        let outcome = async {
            for service in self.config.service_descriptors()? {
                let context = ServiceContext {
                    service: service.clone(),
                    directory: self.directory.clone(),
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
                self.directory.register(service.clone())?;
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
