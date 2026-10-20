//! 按玩家串行执行、重复登录收束及条件续租释放。

use crate::{
    ErrorCode, GatewayContext, Result,
    player::{
        PlayerConfig, PlayerExecutor, PlayerReply,
        channel::{self, ConnectionBinding, OutboundSender, SessionChannel},
        replay::ReplayBatch,
        resume_token::ResumeTokens,
        timer::{TimerScheduler, TimerTicket},
    },
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok_core::{
    LogicalMessage, MessageKind, SessionIdentity,
    coordination::{OwnershipStore, SessionOwner, SessionOwnership},
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub(crate) type SessionTask = Pin<Box<dyn Future<Output = ()> + Send>>;

/// 单次排队请求的回应，不创建额外业务任务；队列关闭时返回失败回应。
pub type PendingResponse = Pin<Box<dyn Future<Output = LogicalMessage> + Send>>;

pub(super) enum Job {
    Request(RequestJob),
    Timer(TimerTicket),
}
pub(super) struct RequestJob {
    request: LogicalMessage,
    // Some 将回应交给等待方；None 通过会话通信通道发布给游戏客户端。
    response_sender: Option<oneshot::Sender<LogicalMessage>>,
}
struct Completion {
    done: AtomicBool,
    notify: Notify,
}
impl Completion {
    async fn wait(&self) {
        loop {
            let waiting = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            waiting.await;
        }
    }
}
struct SessionState {
    ownership: SessionOwnership,
    cancel: CancellationToken,
    completion: Completion,
    job_sender: mpsc::Sender<Job>,
    channel: Arc<SessionChannel>,
    resume_token: String,
}
struct SessionGuard(Arc<SessionState>);
impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.0.cancel.cancel();
        self.0.channel.close();
        self.0.completion.done.store(true, Ordering::Release);
        self.0.completion.notify.notify_waiters();
    }
}

/// 一个玩家当前逻辑会话的有界提交入口，可用于后续转发事件。
#[derive(Clone)]
pub struct SessionHandle(Arc<SessionState>);

#[derive(Clone)]
// 绑定特定连接代次的 session 操作视图；异于不绑定连接的 SessionHandle。
pub(super) struct BoundSession {
    session: SessionHandle,
    binding: ConnectionBinding,
}

impl BoundSession {
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.binding.cancellation.clone()
    }
    pub(super) fn detach(&self) {
        self.session.0.channel.detach(&self.binding);
    }
    pub(super) fn close_session(&self) {
        self.session.0.channel.close_bound_session(&self.binding);
    }
    pub(super) fn last_request_id(&self) -> Result<u64> {
        self.session.0.channel.last_request_id()
    }
    pub(super) fn submit(&self, request: LogicalMessage) -> Result<()> {
        self.session
            .0
            .channel
            .admit_request(&self.binding, request.request_id, || {
                self.session.enqueue_request(request, None)
            })
    }
    pub(super) fn acknowledge(&self, seq: u64) -> Result<()> {
        self.session.0.channel.acknowledge(&self.binding, seq)
    }
    pub(super) fn publish_response(
        &self,
        response: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        let result =
            self.session
                .0
                .channel
                .publish_from(Some(&self.binding), response, request_message_id);
        if let Err(error) = &result
            && matches!(error.code, ErrorCode::SessionClosed | ErrorCode::Internal)
        {
            self.close_session();
        }
        result
    }
}

impl SessionHandle {
    /// 读取当前可信身份，不能据此绕过所有权与队列直接调用 player。
    pub fn identity(&self) -> &SessionIdentity {
        &self.0.ownership.identity
    }
    /// 会话被替换、关闭或退出时取消。
    pub fn cancellation(&self) -> CancellationToken {
        self.0.cancel.clone()
    }
    /// 停止新业务，在途调用由执行任务收束。
    pub fn close(&self) {
        self.0.cancel.cancel();
        self.0.channel.close();
    }

    pub(super) fn complete_login(&self, login: crate::protocol::Frame) -> Result<BoundSession> {
        let binding = self.0.channel.complete_login(login)?;
        Ok(BoundSession {
            session: self.clone(),
            binding,
        })
    }

    pub(super) fn resume_token(&self) -> &str {
        &self.0.resume_token
    }

    /// 查询确认位置之后的完整缓存快照；存在缺口或位置越界时返回 None。
    ///
    /// 不发送、不确认消息，也不恢复连接；会话关闭后返回 SessionClosed。
    pub fn replay_after(&self, ack_seq: u64) -> Result<Option<ReplayBatch>> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.replay_after(ack_seq)
    }

    pub(super) fn publish_response(
        &self,
        response: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if let Err(error) = self.0.channel.publish(response, request_message_id) {
            self.close();
            return Err(error);
        }
        Ok(())
    }

    fn enqueue(
        &self,
        request: LogicalMessage,
        response_sender: Option<oneshot::Sender<LogicalMessage>>,
    ) -> Result<()> {
        self.0.channel.ensure_logged_in()?;
        self.enqueue_request(request, response_sender)
    }

    fn enqueue_request(
        &self,
        request: LogicalMessage,
        response_sender: Option<oneshot::Sender<LogicalMessage>>,
    ) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if request.kind != MessageKind::Request
            || request.request_id == 0
            || request.message_id == 0
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        self.0
            .job_sender
            .try_send(Job::Request(RequestJob {
                request,
                response_sender,
            }))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ErrorCode::QueueFull.into(),
                mpsc::error::TrySendError::Closed(_) => ErrorCode::SessionClosed.into(),
            })
    }
    /// 非阻塞提交业务，回应按执行完成顺序进入会话下行序列，断线宽限期内继续缓存。
    pub fn submit(&self, request: LogicalMessage) -> Result<()> {
        self.enqueue(request, None)
    }
    /// 非阻塞提交并取得独立回应 future，满队列立即失败，不自动重试。
    ///
    /// 回应只交给调用方，不自动发给游戏客户端，不分配下行序号或写入缓存。
    pub fn try_request(&self, request: LogicalMessage) -> Result<PendingResponse> {
        let fallback = channel::error_response(&request, ErrorCode::SessionClosed);
        let (sender, receiver) = oneshot::channel();
        self.enqueue(request, Some(sender))?;
        Ok(Box::pin(async move { receiver.await.unwrap_or(fallback) }))
    }
    /// 主动通知进入下行入口，不调用 player；断线或慢连接时在宽限期内继续缓存。
    /// 超大通知返回 FrameTooLarge，不消耗序号；登录回应入队前返回 InvalidState。
    pub fn notify(&self, message: LogicalMessage) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if message.kind != MessageKind::Notify || message.message_id == 0 {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if let Err(error) = self.0.channel.publish(message, 0) {
            if !matches!(
                error.code,
                ErrorCode::FrameTooLarge | ErrorCode::InvalidState
            ) {
                self.close();
            }
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Default)]
struct Slot {
    // 串行化同一玩家的新建会话与恢复绑定。
    transition_gate: tokio::sync::Mutex<()>,
    // 当前登记的会话，可能正在退出；使用前仍需检查取消或完成状态。
    current_session: Mutex<Option<Arc<SessionState>>>,
}
struct Registry {
    slots: Mutex<BTreeMap<String, Arc<Slot>>>,
    session_counter: AtomicU64,
    // 会话目录建立时生成的实例标记，用于区分重启；不是操作系统 PID。
    process_instance_id: u128,
}

/// 有界的玩家会话目录，由 PlayerIngress 构造并通过 sessions 获取。
///
/// 持有者不能直接创建未认证的 session；不同玩家的登录协调锁互相独立。
#[derive(Clone)]
pub struct SessionManager {
    registry: Arc<Registry>,
    config: Arc<PlayerConfig>,
    owner: SessionOwner,
    ownership_store: Arc<dyn OwnershipStore>,
    executor: Arc<dyn PlayerExecutor>,
    runtime: GatewayContext,
    shutdown: CancellationToken,
    resume_tokens: Arc<ResumeTokens>,
}

impl SessionManager {
    pub(crate) fn new(
        config: Arc<PlayerConfig>,
        owner: SessionOwner,
        ownership_store: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
        runtime: GatewayContext,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            resume_tokens: Arc::new(ResumeTokens::new(&config)),
            registry: Arc::new(Registry {
                slots: Mutex::new(BTreeMap::new()),
                session_counter: AtomicU64::new(0),
                process_instance_id: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            }),
            config,
            owner,
            ownership_store,
            executor,
            runtime,
            shutdown,
        }
    }

    pub(super) fn authenticate_resume_token(&self, token: &str) -> Result<String> {
        Ok(self.resume_tokens.validate(token)?.player_id)
    }

    pub(super) async fn resume_session(
        &self,
        token: &str,
        ack_seq: u64,
        request_id: u64,
        sender: OutboundSender,
    ) -> Result<BoundSession> {
        let claims = self.resume_tokens.validate(token)?;
        let slot = self
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(&claims.player_id)
            .cloned()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        let _gate = tokio::select! { biased; _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), gate = slot.transition_gate.lock() => gate };
        let session_state = slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .clone()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        if session_state.cancel.is_cancelled()
            || session_state.ownership.identity.session_id != claims.session_id
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        if !renew_ownership(
            &session_state,
            self.ownership_store.as_ref(),
            Duration::from_millis(self.config.rpc_timeout_ms),
            Duration::from_millis(self.config.ownership_ttl_ms),
        )
        .await
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let identity = &session_state.ownership.identity;
        let response = crate::protocol::ResumeResponse {
            request_id,
            error_code: 0,
            player_id: identity.player_id.clone(),
            session_id: identity.session_id.clone(),
            epoch: identity.epoch,
            last_request_id: 0,
        };
        let binding = session_state
            .channel
            .rebind_connection(sender, ack_seq, response)?;
        Ok(BoundSession {
            session: SessionHandle(session_state),
            binding,
        })
    }

    /// 查询活动 session，仅返回尚未失效的有界提交入口。
    pub fn session(&self, player_id: &str) -> Result<Option<SessionHandle>> {
        let slots = self
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        let Some(slot) = slots.get(player_id) else {
            return Ok(None);
        };
        let session_state = slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        Ok(session_state
            .as_ref()
            .filter(|session_state| !session_state.cancel.is_cancelled())
            .cloned()
            .map(SessionHandle))
    }

    fn slot(&self, player_id: &str) -> Result<Arc<Slot>> {
        let mut slots = self
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        if let Some(slot) = slots.get(player_id) {
            return Ok(slot.clone());
        }
        slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot
                    .current_session
                    .lock()
                    .map(|session_state| {
                        session_state.as_ref().is_some_and(|session_state| {
                            !session_state.completion.done.load(Ordering::Acquire)
                        })
                    })
                    .unwrap_or(true)
        });
        if slots.len() >= self.config.max_sessions {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        let slot = Arc::new(Slot::default());
        slots.insert(player_id.into(), slot.clone());
        Ok(slot)
    }

    pub(crate) async fn create_session(
        &self,
        player_id: String,
        sender: OutboundSender,
    ) -> Result<SessionHandle> {
        if player_id.trim().is_empty() || self.shutdown.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let slot = self.slot(&player_id)?;
        let _gate = tokio::select! { biased; _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), gate = slot.transition_gate.lock() => gate };
        let old = slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .clone();
        if let Some(old) = &old {
            old.cancel.cancel();
            old.channel.close();
            tokio::select! { biased; _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), _ = old.completion.wait() => {} }
        }
        let timeout = Duration::from_millis(self.config.rpc_timeout_ms);
        let existing = tokio::time::timeout(timeout, self.ownership_store.lookup(&player_id))
            .await
            .map_err(|_| ErrorCode::Timeout)?
            .map_err(|_| ErrorCode::Unavailable)?;
        if let Some(existing) = &existing
            && !old.as_ref().is_some_and(|old| old.ownership == *existing)
        {
            return Err(ErrorCode::Unavailable.into());
        }
        let counter = self
            .registry
            .session_counter
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ErrorCode::Internal)?;
        let session_id = format!(
            "{}:{}:{}:{counter}",
            self.owner.node_id, self.owner.service_name, self.registry.process_instance_id
        );
        let ownership = tokio::time::timeout(
            timeout,
            self.ownership_store.acquire(
                player_id,
                session_id,
                self.owner.clone(),
                existing.map(|record| record.identity.epoch),
                Duration::from_millis(self.config.ownership_ttl_ms),
            ),
        )
        .await
        .map_err(|_| ErrorCode::Timeout)?
        .map_err(|error| match error {
            tok_core::Error::OwnershipConflict => ErrorCode::SessionReplaced,
            _ => ErrorCode::Unavailable,
        })?;
        let resume_token = match self.resume_tokens.issue(&ownership.identity) {
            Ok(token) => token,
            Err(error) => {
                let _ =
                    tokio::time::timeout(timeout, self.ownership_store.release(ownership)).await;
                return Err(error);
            }
        };
        let (job_sender, receiver) = mpsc::channel(self.config.queue_capacity);
        let cancel = self.shutdown.child_token();
        let session_state = Arc::new(SessionState {
            ownership,
            channel: Arc::new(SessionChannel::new(sender, &self.config, cancel.clone())),
            cancel,
            resume_token,
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            job_sender,
        });
        // 登录 future 在运行时接收任务后被取消时，不能遗留无人绑定却持续续租的 session。
        let opening = session_state.cancel.clone().drop_guard();
        // 在下一次 await 前登记已确认的租约，取消登录时退出流程仍能条件释放。
        *slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)? = Some(session_state.clone());
        let task = session_task(
            session_state.clone(),
            receiver,
            self.config.clone(),
            self.ownership_store.clone(),
            self.executor.clone(),
        );
        let task = Box::pin(async move {
            task.await;
            Ok(())
        });
        let launched = tokio::select! { biased; _ = self.shutdown.cancelled() => false, result = self.runtime.launch(task) => result.is_ok() };
        if !launched {
            let _ = tokio::time::timeout(
                timeout,
                self.ownership_store
                    .release(session_state.ownership.clone()),
            )
            .await;
            return Err(ErrorCode::SessionClosed.into());
        }
        if session_state.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let _ = opening.disarm();
        Ok(SessionHandle(session_state))
    }

    pub(crate) fn close_all(&self) {
        self.shutdown.cancel();
    }

    pub(crate) async fn release_all_ownership(&self) {
        let records = self
            .registry
            .slots
            .lock()
            .ok()
            .map(|slots| {
                slots
                    .values()
                    .filter_map(|slot| {
                        slot.current_session
                            .lock()
                            .ok()?
                            .as_ref()
                            .map(|session_state| session_state.ownership.clone())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for record in records {
            let _ = tokio::time::timeout(
                Duration::from_millis(self.config.rpc_timeout_ms),
                self.ownership_store.release(record),
            )
            .await;
        }
    }
}

fn session_task(
    session_state: Arc<SessionState>,
    mut job_receiver: mpsc::Receiver<Job>,
    config: Arc<PlayerConfig>,
    ownership_store: Arc<dyn OwnershipStore>,
    executor: Arc<dyn PlayerExecutor>,
) -> SessionTask {
    let guard = SessionGuard(session_state.clone());
    Box::pin(async move {
        // 即使任务尚未首次 poll 就被丢弃，也失效入口并唤醒等待的重复登录。
        let _guard = guard;
        let timeout = Duration::from_millis(config.rpc_timeout_ms);
        let ttl = Duration::from_millis(config.ownership_ttl_ms);
        let mut renewal = tokio::time::interval(Duration::from_millis(config.renew_interval_ms));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let timers = TimerScheduler::new(config.max_timers, config.max_timer_bytes);
        let work = async {
            let mut last_request = 0;
            let mut timer_request = 0u64;
            loop {
                tokio::select! {
                    biased;
                    _ = session_state.cancel.cancelled() => break,
                    _ = renewal.tick() => {
                        if !matches!(tokio::time::timeout(timeout, ownership_store.renew(session_state.ownership.clone(), ttl)).await, Ok(Ok(()))) { break; }
                    }
                    job = job_receiver.recv() => {
                        let Some(job) = job else { break; };
                        let job = match job {
                            Job::Request(job) => job,
                            Job::Timer(ticket) => {
                                if !timers.is_current(&ticket) { continue; }
                                if !renew_ownership(&session_state, ownership_store.as_ref(), timeout, ttl).await { break; }
                                let Some((spec, trigger)) = timers.prepare(&ticket) else { continue; };
                                let Some(next) = timer_request.checked_add(1) else { session_state.cancel.cancel(); break; };
                                timer_request = next;
                                let span = tracing::info_span!("player_timer", player_id = %session_state.ownership.identity.player_id, timer_id = %trigger.timer_id, scheduled_at_ms = trigger.scheduled_at_ms, fired_at_ms = trigger.fired_at_ms);
                                let result = tokio::time::timeout(timeout, executor.execute_timer(session_state.ownership.identity.clone(), timer_request, spec, trigger).instrument(span)).await;
                                let success = match result {
                                    Ok(Ok(reply)) => {
                                        let mut success = reply.response.as_ref().is_none_or(|response| response.error_code == 0);
                                        if !success { tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家 timer 返回业务错误，未自动重试"); }
                                        if let Some(update) = reply.timers && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            session_state.cancel.cancel();
                                            success = false;
                                        }
                                        success
                                    }
                                    Ok(Err(error)) => { tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "玩家 timer 调用失败，未自动重试"); false }
                                    Err(_) => { tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家 timer 调用超时，未自动重试"); false }
                                };
                                metrics::counter!("tok_session_timers_total", "result" => if success { "ok" } else { "error" }).increment(1);
                                timers.complete_tick(&ticket);
                                continue;
                            }
                        };
                        let mut request = job.request;
                        let response = if session_state.cancel.is_cancelled() {
                            channel::error_response(&request, ErrorCode::SessionClosed)
                        } else if request.request_id <= last_request {
                            channel::error_response(&request, ErrorCode::InvalidRequest)
                        } else {
                            last_request = request.request_id;
                            if !renew_ownership(&session_state, ownership_store.as_ref(), timeout, ttl).await {
                                channel::error_response(&request, ErrorCode::SessionClosed)
                            } else {
                                let span = tracing::info_span!("client_request", player_id = %session_state.ownership.identity.player_id, request_id = request.request_id, message_id = request.message_id);
                                let result = tokio::time::timeout(timeout, executor.execute_request(session_state.ownership.identity.clone(), request.clone()).instrument(span)).await;
                                match result {
                                    Ok(Ok(PlayerReply { response: Some(mut response), timers: timer_update })) if response.kind == MessageKind::Response => {
                                        if let Some(update) = timer_update && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            session_state.cancel.cancel();
                                            channel::error_response(&request, ErrorCode::Internal)
                                        } else {
                                            response.request_id = request.request_id; response.server_seq = None; response
                                        }
                                    }
                                    Ok(Ok(_)) => channel::error_response(&request, ErrorCode::Internal),
                                    Ok(Err(error)) => channel::error_response(&request, error.code),
                                    Err(_) => {
                                        tracing::error!(player_id = %session_state.ownership.identity.player_id, request_id = request.request_id, "玩家调用超时，未自动重试");
                                        channel::error_response(&request, ErrorCode::Timeout)
                                    }
                                }
                            }
                        };
                        // 尽早释放请求正文，慢下行不会持有两份业务缓冲。
                        request.body.clear();
                        metrics::counter!("tok_session_requests_total", "result" => if response.error_code == 0 { "ok" } else { "error" }).increment(1);
                        if let Some(response_sender) = job.response_sender { let _ = response_sender.send(response); }
                        else { let _ = SessionHandle(session_state.clone()).publish_response(response, request.message_id); }
                    }
                }
            }
            session_state.cancel.cancel();
            // 宽限期结束、重复登录或关闭后，未开始的任务只失效，不重新执行。
            while let Ok(job) = job_receiver.try_recv() {
                if let Job::Request(job) = job
                    && let Some(response_sender) = job.response_sender
                {
                    let _ = response_sender.send(channel::error_response(
                        &job.request,
                        ErrorCode::SessionClosed,
                    ));
                }
            }
            if !matches!(
                tokio::time::timeout(
                    timeout,
                    ownership_store.release(session_state.ownership.clone())
                )
                .await,
                Ok(Ok(_))
            ) {
                tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家会话条件释放失败");
            }
        };
        tokio::join!(
            work,
            timers.run(
                session_state.job_sender.clone(),
                session_state.cancel.clone()
            ),
            session_state.channel.run_grace_timer()
        );
    })
}

async fn renew_ownership(
    session_state: &SessionState,
    ownership_store: &dyn OwnershipStore,
    timeout: Duration,
    ttl: Duration,
) -> bool {
    let started = tokio::time::Instant::now();
    let renewed = matches!(
        tokio::time::timeout(
            timeout,
            ownership_store.renew(session_state.ownership.clone(), ttl)
        )
        .await,
        Ok(Ok(()))
    );
    // 本地保守预算检查仍不能排除检查后的进程暂停，不承诺严格 fencing。
    if !renewed || started.elapsed().saturating_add(timeout) >= ttl {
        session_state.cancel.cancel();
    }
    !session_state.cancel.is_cancelled()
}

#[cfg(test)]
mod tests;
