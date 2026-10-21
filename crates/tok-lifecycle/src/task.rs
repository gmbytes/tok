//! 通用有界任务范围：提交、监督、取消与结束等待，不包含服务初始化或就绪。
use std::{collections::HashMap, future::Future};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::{Id, JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument::WithSubscriber};

/// 保留任务原始错误，不依赖具体传输。
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// 受监督异步任务，取消后应协作清理资源。
pub type Task = tok_core::BoxFuture<'static, Result<(), BoxError>>;

/// 通用任务范围状态；与服务初始化、就绪阶段无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// 接受任务和容量预留。
    Open,
    /// 已停止接收新任务，正在收束。
    Stopping,
    /// 任务范围已关闭且被跟踪的工作已释放，不保证强制中止完成异步清理。
    Stopped,
}

// 同一任务范围共享的状态与活动计数；容量名额由 Semaphore 独立维护。
struct ScopeState {
    state: watch::Sender<State>,
    activity_count: AtomicUsize,
    owner_closed: AtomicBool,
    // 子范围持有父范围的活动守卫，直到自身工作全部释放才解除父范围的结束等待。
    parent_activity: Mutex<Option<ActivityGuard>>,
}
impl ScopeState {
    fn try_finish(&self) {
        if self.owner_closed.load(Ordering::SeqCst)
            && self.activity_count.load(Ordering::SeqCst) == 0
        {
            self.state.send_replace(State::Stopped);
            // 锁内只取出守卫；释放后再递归更新父范围，避免嵌套持锁。
            let parent = self
                .parent_activity
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            drop(parent);
        }
    }
}
// 每个守卫代表一个未释放的任务、外部 future 或子范围；普通 Handle 克隆不计入活动数。
struct ActivityGuard {
    scope: Arc<ScopeState>,
}
impl ActivityGuard {
    fn new(scope: &Arc<ScopeState>) -> Self {
        scope.activity_count.fetch_add(1, Ordering::SeqCst);
        Self {
            scope: Arc::clone(scope),
        }
    }
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.scope.activity_count.fetch_sub(1, Ordering::SeqCst);
        self.scope.try_finish();
    }
}

/// 有界任务提交和生命周期句柄；不持有监督循环本身。
#[derive(Clone)]
pub struct Handle {
    cancellation: CancellationToken,
    launches: mpsc::Sender<Submission>,
    capacity: Arc<Semaphore>,
    scope: Arc<ScopeState>,
}
impl Handle {
    /// 查询任务范围状态；后续操作仍需重新校验。
    pub fn state(&self) -> State {
        let state = *self.scope.state.borrow();
        if self.cancellation.is_cancelled() && matches!(state, State::Open) {
            State::Stopping
        } else {
            state
        }
    }
    /// 包含待提交、已排队及执行中任务的已占用后台名额。
    pub fn used_capacity(&self) -> usize {
        self.launches.max_capacity() - self.capacity.available_permits()
    }
    /// 请求停止，等待释放任务需另行调用 wait_finished。
    pub fn request_stop(&self) {
        self.scope.state.send_if_modified(|state| {
            if matches!(state, State::Open) {
                *state = State::Stopping;
                true
            } else {
                false
            }
        });
        self.cancellation.cancel();
    }
    /// 当前运行范围的取消令牌；取消不代表任务已经释放。
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    /// 等待范围关闭、任务 future 及子范围释放；不能在本范围任务内等待自身结束。
    pub async fn wait_finished(&self) {
        let mut states = self.scope.state.subscribe();
        loop {
            if *states.borrow_and_update() == State::Stopped {
                return;
            }
            if states.changed().await.is_err() {
                return;
            }
        }
    }
    /// 预留待启动及执行中的共同容量；停止后拒绝预留。
    pub fn reserve(&self) -> Result<Reservation, SubmitError> {
        if self.cancellation.is_cancelled()
            || matches!(self.state(), State::Stopping | State::Stopped)
        {
            return Err(SubmitError::Stopping);
        }
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| SubmitError::CapacityExceeded)?;
        Ok(Reservation {
            handle: self.clone(),
            permit,
        })
    }
    /// 提交有名称的受监督任务；满时立即失败，不启动任务。
    pub async fn launch(&self, name: impl Into<String>, task: Task) -> Result<(), SubmitError> {
        self.reserve()?.launch(name, task).await
    }
}

/// 任务容量预留或提交失败；不表示任务执行失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    /// 任务范围已收到停止通知。
    #[error("任务范围正在退出")]
    Stopping,
    /// 待提交、排队和执行中的任务已占满容量。
    #[error("后台任务容量已满")]
    CapacityExceeded,
    /// 监督队列已关闭。
    #[error("任务提交入口已关闭")]
    Closed,
    /// 名称全为空白或 UTF-8 编码超过 256 字节。
    #[error("任务名称不能为空白，且不得超过 256 字节")]
    InvalidName,
}

/// 已预留的后台任务名额；放弃提交时自动归还。
pub struct Reservation {
    handle: Handle,
    permit: OwnedSemaphorePermit,
}
impl Reservation {
    /// 提交前重新检查停止状态；成功只表示进入监督队列。
    pub async fn launch(self, name: impl Into<String>, task: Task) -> Result<(), SubmitError> {
        let name = name.into();
        if name.trim().is_empty() || name.len() > 256 {
            return Err(SubmitError::InvalidName);
        }
        let submission = Submission {
            name,
            task: Box::pin(task.in_current_span().with_current_subscriber()),
            _capacity_permit: self.permit,
            _activity: ActivityGuard::new(&self.handle.scope),
        };
        tokio::select! {
            biased;
            _ = self.handle.cancellation.cancelled() => Err(SubmitError::Stopping),
            result = self.handle.launches.send(submission) => result.map_err(|_| SubmitError::Closed),
        }
    }
}
struct Submission {
    name: String,
    task: Task,
    // 只需持有：覆盖排队与执行期间的后台容量，Drop 自动归还名额。
    _capacity_permit: OwnedSemaphorePermit,
    // 只需持有：任务 future 释放前阻止范围结束，Drop 自动减少活动数。
    _activity: ActivityGuard,
}

/// 一次后台任务完成；正常结束可以是合法的有限工作，由运行入口决定策略。
pub struct Completion {
    /// 组装方声明的任务名称。
    pub name: String,
    /// 原任务错误或任务异常结束。
    pub result: Result<(), Error>,
}

/// 任务范围构造失败，不包含已启动任务的执行错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CreateError {
    /// 容量不在 1 至 65536 范围内。
    #[error("后台任务容量须为 1 至 65536")]
    InvalidCapacity,
    /// 创建子范围时父范围已开始退出。
    #[error("父任务范围正在退出")]
    ParentStopping,
}

/// 通用任务执行与退出错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 被监督任务返回错误。
    #[error("任务 {name} 失败：{source}")]
    Run {
        /// 任务名称。
        name: String,
        /// 原始任务错误。
        source: BoxError,
    },
    /// 被监督任务异常结束，包括 panic 或调度取消。
    #[error("任务异常：{0}")]
    Join(#[from] JoinError),
    /// 协作退出期限耗尽，本监督者拥有的任务已中止；外部跟踪工作仍由其所有者回收。
    #[error("任务范围退出超时")]
    ShutdownTimeout,
}

/// 任务所有者；克隆的 Handle 不会延长监督者生命周期。
///
/// shutdown 成功表示范围结束；Drop 只发起取消与中止，实际释放由 wait_finished 观察。
/// 由所有者持续驱动 next；提交工作本身不创建脱管监督循环。
pub struct Supervisor {
    handle: Handle,
    launches: mpsc::Receiver<Submission>,
    tasks: JoinSet<Completion>,
    names: HashMap<Id, String>,
}
impl Supervisor {
    /// 创建有界任务范围；容量须为 1 至 65536，外部跟踪工作使用调用方自己的容量约束。
    pub fn new(max_tasks: usize, cancellation: CancellationToken) -> Result<Self, CreateError> {
        if max_tasks == 0 || max_tasks > 65_536 {
            return Err(CreateError::InvalidCapacity);
        }
        let (launches, receiver) = mpsc::channel(max_tasks);
        let (state, _) = watch::channel(State::Open);
        Ok(Self {
            handle: Handle {
                cancellation,
                launches,
                capacity: Arc::new(Semaphore::new(max_tasks)),
                scope: Arc::new(ScopeState {
                    state,
                    activity_count: AtomicUsize::new(0),
                    owner_closed: AtomicBool::new(false),
                    parent_activity: Mutex::new(None),
                }),
            },
            launches: receiver,
            tasks: JoinSet::new(),
            names: HashMap::new(),
        })
    }
    /// 创建独立容量的子运行范围；父取消向子传播，父结束等待包含子范围的全部任务。
    /// 子监督者仍由组件拥有并驱动，子停止不取消父范围。
    pub fn child(max_tasks: usize, parent: &Handle) -> Result<Self, CreateError> {
        let activity = ActivityGuard::new(&parent.scope);
        if parent.cancellation.is_cancelled() || parent.scope.owner_closed.load(Ordering::SeqCst) {
            return Err(CreateError::ParentStopping);
        }
        let child = Self::new(max_tasks, parent.cancellation.child_token())?;
        *child
            .handle
            .scope
            .parent_activity
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(activity);
        Ok(child)
    }
    /// 获取任务入口及生命周期查询能力。
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    fn spawn(&mut self, submission: Submission) {
        let name = submission.name.clone();
        let task = self.tasks.spawn(async move {
            let Submission {
                name,
                task,
                _capacity_permit,
                _activity,
            } = submission;
            let result = task.await.map_err(|source| Error::Run {
                name: name.clone(),
                source,
            });
            Completion { name, result }
        });
        self.names.insert(task.id(), name);
    }
    fn completion(&mut self, result: Result<(Id, Completion), JoinError>) -> Completion {
        match result {
            Ok((id, completion)) => {
                self.names.remove(&id);
                completion
            }
            Err(error) => {
                // 所有 spawn 都登记名称；防御性回退仍保留 Tokio 任务 ID 便于定位。
                let name = self
                    .names
                    .remove(&error.id())
                    .unwrap_or_else(|| format!("task-{}", error.id()));
                Completion {
                    name,
                    result: Err(Error::Join(error)),
                }
            }
        }
    }
    /// 将外部调度器拥有的 future 纳入结束等待，不启动任务、不占后台名额。
    ///
    /// 用于另有容量约束的连接任务等；调用方仍须拥有、中止并回收这些 future。
    /// 返回值释放前 wait_finished 不会完成，其容量与取消仍由外部调度器控制。
    pub fn track<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
        let activity = ActivityGuard::new(&self.handle.scope);
        async move {
            let _activity = activity;
            future.await
        }
    }
    /// 等待下一个任务结束，同时启动队列中的任务；空运行范围会等待新工作。
    /// 此方法可在 select 中取消，不丢弃已接收任务。
    pub async fn next(&mut self) -> Completion {
        loop {
            tokio::select! {
                biased;
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(result) = result {
                        return self.completion(result);
                    }
                }
                submission = self.launches.recv(), if !self.launches.is_closed() || !self.launches.is_empty() => {
                    if let Some(submission) = submission { self.spawn(submission); }
                }
                else => return std::future::pending().await,
            }
        }
    }
    /// 关闭新任务准入、发出取消并启动已排队任务以便清理；不等待退出。
    pub fn stop_accepting(&mut self) {
        self.handle.request_stop();
        self.launches.close();
        while let Ok(submission) = self.launches.try_recv() {
            self.spawn(submission);
        }
    }
    async fn drain(&mut self) -> Result<(), Error> {
        let mut error = None;
        while let Some(result) = self.tasks.join_next_with_id().await {
            let result = self.completion(result).result;
            if let Err(next) = result {
                error.get_or_insert(next);
            }
        }
        error.map_or(Ok(()), Err)
    }
    async fn abort_owned(&mut self) {
        self.launches.close();
        while self.launches.try_recv().is_ok() {}
        self.tasks.shutdown().await;
        self.names.clear();
    }
    /// 消费任务范围并收束全部工作；成功返回时 wait_finished 已完成。
    ///
    /// None 表示整体期限由外层控制。期限耗尽后中止并回收本监督者拥有的任务；
    /// 外部跟踪 future 或子范围仍由其所有者回收，未释放时状态保留 Stopping。
    pub async fn shutdown(mut self, timeout: Option<Duration>) -> Result<(), Error> {
        self.stop_accepting();
        let handle = self.handle.clone();
        let cleanup = async {
            let result = self.drain().await;
            self.close_owner();
            handle.wait_finished().await;
            result
        };
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, cleanup).await,
            None => Ok(cleanup.await),
        };
        match result {
            Ok(result) => result,
            Err(_) => {
                self.abort_owned().await;
                self.close_owner();
                Err(Error::ShutdownTimeout)
            }
        }
    }
    fn close_owner(&self) {
        self.handle.scope.owner_closed.store(true, Ordering::SeqCst);
        self.handle.scope.try_finish();
    }
}
impl Drop for Supervisor {
    fn drop(&mut self) {
        self.handle.request_stop();
        self.launches.close();
        while self.launches.try_recv().is_ok() {}
        self.tasks.abort_all();
        self.handle.scope.owner_closed.store(true, Ordering::SeqCst);
        self.handle.scope.try_finish();
    }
}
