use super::{Factory, Init};
use crate::task::{self, BoxError, Task};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tok_core::{Extensions, service::Identity};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument::WithSubscriber};

/// 服务阶段；初始化与任务范围的开放状态分别表达。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// 正在初始化或等待组装方启动入口。
    Starting,
    /// 初始化完成，主任务已交给监督，组装方可发布发现。
    Ready,
    /// 已收到停止通知，正在收束。
    Stopping,
    /// 服务任务范围及被跟踪工作已释放。
    Stopped,
}

/// 服务阶段、停止与结束等待；任务提交和容量操作通过 tasks() 取得通用句柄。
#[derive(Clone)]
pub struct Handle {
    tasks: task::Handle,
    ready: Arc<AtomicBool>,
}
impl Handle {
    /// 查询服务阶段；业务操作仍须重新校验。
    pub fn phase(&self) -> Phase {
        match self.tasks.state() {
            task::State::Open if self.ready.load(Ordering::SeqCst) => Phase::Ready,
            task::State::Open => Phase::Starting,
            task::State::Stopping => Phase::Stopping,
            task::State::Stopped => Phase::Stopped,
        }
    }
    /// 获取任务范围能力，供内置组件建立子范围。
    pub fn tasks(&self) -> task::Handle {
        self.tasks.clone()
    }
    /// 请求停止，不等待清理。
    pub fn request_stop(&self) {
        self.tasks.request_stop();
    }
    /// 获取服务停止信号；取消不等于结束。
    pub fn cancellation(&self) -> CancellationToken {
        self.tasks.cancellation()
    }
    /// 等待服务范围结束，不能由本服务任务等待自身。
    pub async fn wait_finished(&self) {
        self.tasks.wait_finished().await;
    }
}

/// 服务构建器创建失败，不包含工厂初始化或运行错误。
#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    /// 节点或服务名为空白。
    #[error("服务身份不能为空")]
    InvalidIdentity,
    /// 无法创建服务所需的任务范围，保留原始错误。
    #[error("服务任务范围创建失败：{0}")]
    Tasks(#[from] task::CreateError),
}

/// 服务初始化、主任务及退出错误；任务执行错误保留原始来源。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 工厂初始化失败。
    #[error("初始化失败：{0}")]
    Initialize(#[source] BoxError),
    /// 初始化期限耗尽。
    #[error("初始化超时")]
    InitializationTimeout,
    /// 初始化期间收到停止通知。
    #[error("服务初始化已取消")]
    Cancelled,
    /// 主任务或后台任务失败。
    #[error("任务 {name} 失败：{source}")]
    Run {
        /// 任务名称。
        name: String,
        /// 原始任务错误。
        source: BoxError,
    },
    /// 初始化或任务异常结束，包括 panic 或调度取消。
    #[error("服务任务异常：{0}")]
    Join(#[from] JoinError),
    /// 主任务在停止通知前结束。
    #[error("服务主任务在停止通知前结束")]
    UnexpectedExit,
    /// 退出期限耗尽，已中止任务；不表示异步清理完成。
    #[error("服务退出超时，已中止剩余任务")]
    ShutdownTimeout,
}
impl From<task::Error> for Error {
    fn from(error: task::Error) -> Self {
        match error {
            task::Error::Run { name, source } => Self::Run { name, source },
            task::Error::Join(error) => Self::Join(error),
            task::Error::ShutdownTimeout => Self::ShutdownTimeout,
        }
    }
}

/// 尚未初始化的服务构建器；build 消费自身，不能重复初始化。
///
/// ```compile_fail
/// use std::{sync::Arc, time::Duration};
/// use tok_core::Extensions;
/// use tok_lifecycle::Factory;
/// use tok_lifecycle::Builder;
/// async fn build_twice(builder: Builder, factory: Arc<dyn Factory>) {
///     let first = builder.build(Arc::clone(&factory), Extensions::default(), Duration::from_secs(1)).await;
///     let second = builder.build(factory, Extensions::default(), Duration::from_secs(1)).await;
/// }
/// ```
pub struct Builder {
    identity: Identity,
    supervisor: task::Supervisor,
    service: Handle,
}
impl Builder {
    /// 创建服务范围，不初始化业务，不发布就绪。
    pub fn new(
        identity: Identity,
        max_tasks: usize,
        cancellation: CancellationToken,
    ) -> Result<Self, CreateError> {
        if identity.node_id.trim().is_empty() || identity.service_name.trim().is_empty() {
            return Err(CreateError::InvalidIdentity);
        }
        let supervisor = task::Supervisor::new(max_tasks, cancellation)?;
        let service = Handle {
            tasks: supervisor.handle(),
            ready: Arc::new(AtomicBool::new(false)),
        };
        Ok(Self {
            identity,
            supervisor,
            service,
        })
    }
    /// 获取初始化阶段的服务句柄。
    pub fn handle(&self) -> Handle {
        self.service.clone()
    }
    /// 初始化状态及入口，并监督启动期后台任务；成功返回的 Prepared 尚未就绪。
    ///
    /// 工厂创建的子范围必须由本服务任务持有或清理，不得脱离服务生命周期。
    /// 丢弃此 future 会取消并中止初始化及其后台工作。
    pub async fn build(
        mut self,
        factory: Arc<dyn Factory>,
        extensions: Extensions,
        timeout: Duration,
    ) -> Result<Prepared, Error> {
        let init = Init {
            identity: self.identity.clone(),
            service: self.service.clone(),
            extensions,
        };
        let mut starting = JoinSet::new();
        starting.spawn(
            self.supervisor
                .track(async move { factory.initialize(init).await })
                .in_current_span()
                .with_current_subscriber(),
        );
        let cancellation = self.service.cancellation();
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let outcome = loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Err(Error::Cancelled),
                _ = &mut deadline => break Err(Error::InitializationTimeout),
                completion = self.supervisor.next() => {
                    if let Err(error) = completion.result { break Err(error.into()); }
                }
                result = starting.join_next() => break match result {
                    Some(Ok(Ok(task))) => Ok(task),
                    Some(Ok(Err(error))) => Err(Error::Initialize(error)),
                    Some(Err(error)) => Err(Error::Join(error)),
                    None => Err(Error::UnexpectedExit),
                },
            }
        };
        match outcome {
            Ok(main) => Ok(Prepared {
                main,
                supervisor: self.supervisor,
                service: self.service,
            }),
            Err(error) => {
                self.service.request_stop();
                starting.shutdown().await;
                // 初始化失败直接回收，保留原始初始化错误。
                drop(self.supervisor.shutdown(Some(Duration::ZERO)).await);
                self.service.wait_finished().await;
                Err(error)
            }
        }
    }
}

/// 初始化成功的服务；只能消费一次以启动，不能重新初始化或提前发布就绪。
///
/// ```compile_fail
/// use tok_lifecycle::Prepared;
/// fn start_twice(prepared: Prepared) {
///     let first = prepared.start();
///     let second = prepared.start();
/// }
/// ```
pub struct Prepared {
    // 主任务先于任务范围析构，释放未启动的业务资源及其子范围。
    main: Task,
    supervisor: task::Supervisor,
    service: Handle,
}
impl Prepared {
    /// 获取服务句柄；此时仍为 Starting，停止通知则表现为 Stopping。
    pub fn handle(&self) -> Handle {
        self.service.clone()
    }
    /// 将主任务交给监督并唯一地推进就绪；收到过停止通知时仍启动主任务以执行清理。
    /// 返回后组装方可开启入口、发布发现，并须驱动 Running::run。
    pub fn start(self) -> Running {
        let mut main = JoinSet::new();
        main.spawn(
            self.supervisor
                .track(self.main)
                .in_current_span()
                .with_current_subscriber(),
        );
        if self.service.tasks.state() == task::State::Open {
            self.service.ready.store(true, Ordering::SeqCst);
        }
        Running {
            main,
            supervisor: self.supervisor,
            service: self.service,
        }
    }
}

/// 主任务已启动的服务；run 负责监督、失败策略与完整退出，不能再初始化或启动。
pub struct Running {
    main: JoinSet<Result<(), BoxError>>,
    supervisor: task::Supervisor,
    service: Handle,
}
impl Running {
    /// 获取服务运行句柄。
    pub fn handle(&self) -> Handle {
        self.service.clone()
    }
    /// 驱动服务监督；退出成功表示主任务、后台任务及子范围均已释放。
    /// 超时会中止任务并等待本服务持有的子范围回收，不保证异步清理完成。
    pub async fn run(self, shutdown_timeout: Duration) -> Result<(), Error> {
        let Self {
            mut main,
            mut supervisor,
            service,
        } = self;
        let cancellation = service.cancellation();
        let outcome = loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                completion = supervisor.next() => { if let Err(error) = completion.result { break Err(error.into()); } }
                result = main.join_next() => break match result {
                    Some(Ok(Err(source))) => Err(Error::Run { name: "main".into(), source }),
                    Some(Err(error)) => Err(Error::Join(error)),
                    _ => Err(Error::UnexpectedExit),
                },
            }
        };
        supervisor.stop_accepting();
        let cleanup = async {
            let mut error = None;
            while let Some(result) = main.join_next().await {
                if let Err(next) = result.map_err(Error::Join).and_then(|r| {
                    r.map_err(|source| Error::Run {
                        name: "main".into(),
                        source,
                    })
                }) {
                    error.get_or_insert(next);
                }
            }
            let background = supervisor.shutdown(None).await.map_err(Error::from);
            error.map_or(background, Err)
        };
        let cleanup = match tokio::time::timeout(shutdown_timeout, cleanup).await {
            Ok(result) => result,
            Err(_) => {
                main.shutdown().await;
                service.wait_finished().await;
                Err(Error::ShutdownTimeout)
            }
        };
        outcome.and(cleanup)
    }
}
