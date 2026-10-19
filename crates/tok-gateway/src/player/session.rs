//! 按玩家串行执行、重复登录收束及条件续租释放。

use crate::{
    ErrorCode, GatewayContext, Result,
    player::{
        PlayerConfig, PlayerExecutor,
        outbound::{self, Downlink, Sender},
        replay::ReplayBatch,
        timer::{Ticket, Timers},
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
    Timer(Ticket),
}
pub(super) struct RequestJob {
    request: LogicalMessage,
    reply: Option<oneshot::Sender<LogicalMessage>>,
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
struct Active {
    ownership: SessionOwnership,
    cancel: CancellationToken,
    completion: Completion,
    queue: mpsc::Sender<Job>,
    outbound: Downlink,
}
struct Finished(Arc<Active>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.cancel.cancel();
        self.0.outbound.close();
        self.0.completion.done.store(true, Ordering::Release);
        self.0.completion.notify.notify_waiters();
    }
}

/// 一个玩家当前逻辑会话的有界提交入口，可用于后续转发事件。
#[derive(Clone)]
pub struct SessionHandle(Arc<Active>);

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
        self.0.outbound.close();
    }

    pub(super) fn start(&self, login: crate::protocol::Frame) -> Result<()> {
        self.0.outbound.start(login)
    }

    pub(super) fn acknowledge(&self, seq: u64) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.outbound.acknowledge(seq)
    }

    /// 查询确认位置之后的完整缓存快照；存在缺口或位置越界时返回 None。
    ///
    /// 不发送、不确认消息，也不恢复连接；会话关闭后返回 SessionClosed。
    pub fn replay_after(&self, ack_seq: u64) -> Result<Option<ReplayBatch>> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.outbound.replay_after(ack_seq)
    }

    pub(super) fn respond(&self, response: LogicalMessage, request_message_id: u32) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if let Err(error) = self.0.outbound.publish(response, request_message_id) {
            self.close();
            return Err(error);
        }
        Ok(())
    }

    fn enqueue(
        &self,
        request: LogicalMessage,
        reply: Option<oneshot::Sender<LogicalMessage>>,
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
        self.0.outbound.check_ready()?;
        self.0
            .queue
            .try_send(Job::Request(RequestJob { request, reply }))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ErrorCode::QueueFull.into(),
                mpsc::error::TrySendError::Closed(_) => ErrorCode::SessionClosed.into(),
            })
    }
    /// 非阻塞提交业务，回应按执行完成顺序进入当前连接的有界下行入口。
    pub fn submit(&self, request: LogicalMessage) -> Result<()> {
        self.enqueue(request, None)
    }
    /// 非阻塞提交并取得独立回应 future，满队列立即失败，不自动重试。
    ///
    /// 回应只交给调用方，不自动发给游戏客户端，不分配下行序号或写入缓存。
    pub fn try_request(&self, request: LogicalMessage) -> Result<PendingResponse> {
        let fallback = outbound::failed(&request, ErrorCode::SessionClosed);
        let (sender, receiver) = oneshot::channel();
        self.enqueue(request, Some(sender))?;
        Ok(Box::pin(async move { receiver.await.unwrap_or(fallback) }))
    }
    /// 主动通知进入下行入口，不调用 player；慢连接的缓冲满时关闭会话。
    /// 超大通知返回 FrameTooLarge，不消耗序号；登录回应入队前返回 InvalidState。
    pub fn notify(&self, message: LogicalMessage) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if message.kind != MessageKind::Notify || message.message_id == 0 {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if let Err(error) = self.0.outbound.publish(message, 0) {
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
    gate: tokio::sync::Mutex<()>,
    active: Mutex<Option<Arc<Active>>>,
}
struct Registry {
    slots: Mutex<BTreeMap<String, Arc<Slot>>>,
    counter: AtomicU64,
    process_id: u128,
}

/// 有界的玩家会话目录，由 PlayerIngress 构造并通过 sessions 获取。
///
/// 持有者不能直接创建未认证的 session；不同玩家的登录协调锁互相独立。
#[derive(Clone)]
pub struct SessionManager {
    registry: Arc<Registry>,
    config: Arc<PlayerConfig>,
    owner: SessionOwner,
    ownership: Arc<dyn OwnershipStore>,
    executor: Arc<dyn PlayerExecutor>,
    runtime: GatewayContext,
    shutdown: CancellationToken,
}

impl SessionManager {
    pub(crate) fn new(
        config: Arc<PlayerConfig>,
        owner: SessionOwner,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
        runtime: GatewayContext,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            registry: Arc::new(Registry {
                slots: Mutex::new(BTreeMap::new()),
                counter: AtomicU64::new(0),
                process_id: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            }),
            config,
            owner,
            ownership,
            executor,
            runtime,
            shutdown,
        }
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
        let active = slot.active.lock().map_err(|_| ErrorCode::Internal)?;
        Ok(active
            .as_ref()
            .filter(|active| !active.cancel.is_cancelled())
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
                    .active
                    .lock()
                    .map(|active| {
                        active
                            .as_ref()
                            .is_some_and(|active| !active.completion.done.load(Ordering::Acquire))
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

    pub(crate) async fn open(&self, player_id: String, outbound: Sender) -> Result<SessionHandle> {
        if player_id.trim().is_empty() || self.shutdown.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let slot = self.slot(&player_id)?;
        let _gate = tokio::select! { biased; _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), gate = slot.gate.lock() => gate };
        let old = slot.active.lock().map_err(|_| ErrorCode::Internal)?.clone();
        if let Some(old) = &old {
            old.cancel.cancel();
            tokio::select! { biased; _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), _ = old.completion.wait() => {} }
        }
        let timeout = Duration::from_millis(self.config.rpc_timeout_ms);
        let existing = tokio::time::timeout(timeout, self.ownership.lookup(&player_id))
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
            .counter
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ErrorCode::Internal)?;
        let session_id = format!(
            "{}:{}:{}:{counter}",
            self.owner.node_id, self.owner.service_name, self.registry.process_id
        );
        let ownership = tokio::time::timeout(
            timeout,
            self.ownership.acquire(
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
        let (queue, receiver) = mpsc::channel(self.config.queue_capacity);
        let active = Arc::new(Active {
            ownership,
            cancel: self.shutdown.child_token(),
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            queue,
            outbound: Downlink::new(outbound, &self.config),
        });
        // 在下一次 await 前登记已确认的租约，取消登录时退出流程仍能条件释放。
        *slot.active.lock().map_err(|_| ErrorCode::Internal)? = Some(active.clone());
        let task = actor(
            active.clone(),
            receiver,
            self.config.clone(),
            self.ownership.clone(),
            self.executor.clone(),
        );
        let task = Box::pin(async move {
            task.await;
            Ok(())
        });
        let launched = tokio::select! { biased; _ = self.shutdown.cancelled() => false, result = self.runtime.launch(task) => result.is_ok() };
        if !launched {
            let _ = tokio::time::timeout(timeout, self.ownership.release(active.ownership.clone()))
                .await;
            return Err(ErrorCode::SessionClosed.into());
        }
        if active.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        Ok(SessionHandle(active))
    }

    pub(crate) fn close_all(&self) {
        self.shutdown.cancel();
    }

    pub(crate) async fn release_all(&self) {
        let records = self
            .registry
            .slots
            .lock()
            .ok()
            .map(|slots| {
                slots
                    .values()
                    .filter_map(|slot| {
                        slot.active
                            .lock()
                            .ok()?
                            .as_ref()
                            .map(|active| active.ownership.clone())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for record in records {
            let _ = tokio::time::timeout(
                Duration::from_millis(self.config.rpc_timeout_ms),
                self.ownership.release(record),
            )
            .await;
        }
    }
}

fn actor(
    active: Arc<Active>,
    mut queue: mpsc::Receiver<Job>,
    config: Arc<PlayerConfig>,
    ownership: Arc<dyn OwnershipStore>,
    executor: Arc<dyn PlayerExecutor>,
) -> SessionTask {
    let finished = Finished(active.clone());
    Box::pin(async move {
        // 即使任务尚未首次 poll 就被丢弃，也失效入口并唤醒等待的重复登录。
        let _finished = finished;
        let timeout = Duration::from_millis(config.rpc_timeout_ms);
        let ttl = Duration::from_millis(config.ownership_ttl_ms);
        let mut renewal = tokio::time::interval(Duration::from_millis(config.renew_interval_ms));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let timers = Timers::new(config.max_timers, config.max_timer_bytes);
        let work = async {
            let mut last_request = 0;
            let mut timer_request = 0u64;
            loop {
                tokio::select! {
                    biased;
                    _ = active.cancel.cancelled() => break,
                    _ = renewal.tick() => {
                        if !matches!(tokio::time::timeout(timeout, ownership.renew(active.ownership.clone(), ttl)).await, Ok(Ok(()))) { break; }
                    }
                    job = queue.recv() => {
                        let Some(job) = job else { break; };
                        let job = match job {
                            Job::Request(job) => job,
                            Job::Timer(ticket) => {
                                if !timers.is_current(&ticket) { continue; }
                                if !renew_for_call(&active, ownership.as_ref(), timeout, ttl).await { break; }
                                let Some((spec, trigger)) = timers.prepare(&ticket) else { continue; };
                                let Some(next) = timer_request.checked_add(1) else { active.cancel.cancel(); break; };
                                timer_request = next;
                                let span = tracing::info_span!("player_timer", player_id = %active.ownership.identity.player_id, timer_id = %trigger.timer_id, scheduled_at_ms = trigger.scheduled_at_ms, fired_at_ms = trigger.fired_at_ms);
                                let result = tokio::time::timeout(timeout, executor.timer(active.ownership.identity.clone(), timer_request, spec, trigger).instrument(span)).await;
                                let success = match result {
                                    Ok(Ok(reply)) => {
                                        let mut success = reply.response.as_ref().is_none_or(|response| response.error_code == 0);
                                        if !success { tracing::error!(player_id = %active.ownership.identity.player_id, "玩家 timer 返回业务错误，未自动重试"); }
                                        if let Some(update) = reply.timers && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %active.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            active.cancel.cancel();
                                            success = false;
                                        }
                                        success
                                    }
                                    Ok(Err(error)) => { tracing::error!(player_id = %active.ownership.identity.player_id, code = ?error.code, "玩家 timer 调用失败，未自动重试"); false }
                                    Err(_) => { tracing::error!(player_id = %active.ownership.identity.player_id, "玩家 timer 调用超时，未自动重试"); false }
                                };
                                metrics::counter!("tok_session_timers_total", "result" => if success { "ok" } else { "error" }).increment(1);
                                timers.finish(&ticket);
                                continue;
                            }
                        };
                        let mut request = job.request;
                        let response = if active.cancel.is_cancelled() {
                            outbound::failed(&request, ErrorCode::SessionClosed)
                        } else if request.request_id <= last_request {
                            outbound::failed(&request, ErrorCode::InvalidRequest)
                        } else {
                            last_request = request.request_id;
                            if !renew_for_call(&active, ownership.as_ref(), timeout, ttl).await {
                                outbound::failed(&request, ErrorCode::SessionClosed)
                            } else {
                                let span = tracing::info_span!("client_request", player_id = %active.ownership.identity.player_id, request_id = request.request_id, message_id = request.message_id);
                                let result = tokio::time::timeout(timeout, executor.execute(active.ownership.identity.clone(), request.clone()).instrument(span)).await;
                                match result {
                                    Ok(Ok(reply)) if reply.response.as_ref().is_some_and(|response| response.kind == MessageKind::Response) => {
                                        if let Some(update) = reply.timers && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %active.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            active.cancel.cancel();
                                            outbound::failed(&request, ErrorCode::Internal)
                                        } else {
                                            let mut response = reply.response.expect("已检查回应类型");
                                            response.request_id = request.request_id; response.server_seq = None; response
                                        }
                                    }
                                    Ok(Ok(_)) => outbound::failed(&request, ErrorCode::Internal),
                                    Ok(Err(error)) => outbound::failed(&request, error.code),
                                    Err(_) => {
                                        tracing::error!(player_id = %active.ownership.identity.player_id, request_id = request.request_id, "玩家调用超时，未自动重试");
                                        outbound::failed(&request, ErrorCode::Timeout)
                                    }
                                }
                            }
                        };
                        // 尽早释放请求正文，慢下行不会持有两份业务缓冲。
                        request.body.clear();
                        metrics::counter!("tok_session_requests_total", "result" => if response.error_code == 0 { "ok" } else { "error" }).increment(1);
                        if let Some(reply) = job.reply { let _ = reply.send(response); }
                        else { let _ = SessionHandle(active.clone()).respond(response, request.message_id); }
                    }
                }
            }
            active.cancel.cancel();
            // 本批没有断线宽限期；队列中未开始的任务只失效，不重新执行。
            while let Ok(job) = queue.try_recv() {
                if let Job::Request(job) = job
                    && let Some(reply) = job.reply
                {
                    let _ = reply.send(outbound::failed(&job.request, ErrorCode::SessionClosed));
                }
            }
            if !matches!(
                tokio::time::timeout(timeout, ownership.release(active.ownership.clone())).await,
                Ok(Ok(_))
            ) {
                tracing::error!(player_id = %active.ownership.identity.player_id, "玩家会话条件释放失败");
            }
        };
        tokio::join!(
            work,
            timers.run(active.queue.clone(), active.cancel.clone())
        );
    })
}

async fn renew_for_call(
    active: &Active,
    ownership: &dyn OwnershipStore,
    timeout: Duration,
    ttl: Duration,
) -> bool {
    let started = tokio::time::Instant::now();
    let renewed = matches!(
        tokio::time::timeout(timeout, ownership.renew(active.ownership.clone(), ttl)).await,
        Ok(Ok(()))
    );
    // 本地保守预算检查仍不能排除检查后的进程暂停，不承诺严格 fencing。
    if !renewed || started.elapsed().saturating_add(timeout) >= ttl {
        active.cancel.cancel();
    }
    !active.cancel.is_cancelled()
}

#[cfg(test)]
mod tests;
