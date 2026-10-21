//! 有界会话目录、登录替换、凭证刷新与恢复绑定的协调。
use super::{
    channel::{self, OutboundSender},
    handle::{BoundSession, SessionHandle},
    handoff,
    state::{Completion, SessionState},
    subscriptions, timer,
    worker::{self, renew_ownership},
};
use crate::{
    ErrorCode, Result,
    player::{Config, Executor, resume_token::SignedResumeTokens},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok_lifecycle::task;
use tok_player::ownership::{Owner as SessionOwner, Store as OwnershipStore};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct Slot {
    // 串行化同一玩家的新建会话与恢复绑定。
    pub(super) transition_gate: Arc<tokio::sync::Mutex<()>>,
    // 当前登记的会话，可能正在退出；使用前仍需检查取消或完成状态。
    pub(super) current_session: Mutex<Option<Arc<SessionState>>>,
}
struct Registry {
    slots: Mutex<BTreeMap<String, Arc<Slot>>>,
    session_counter: AtomicU64,
    // 会话目录建立时生成的实例标记，用于区分重启；不是操作系统 PID。
    process_instance_id: u128,
}

type TopicPolicy = dyn Fn(tok_execution::Request<tok_player::SessionIdentity>) -> Result<Vec<String>>
    + Send
    + Sync;

/// 有界的玩家会话目录，由 Ingress 构造并通过 sessions 获取。
///
/// 持有者不能直接创建未认证的 session；不同玩家的登录协调锁互相独立。
#[derive(Clone)]
pub struct SessionManager {
    pub(super) inner: Arc<ManagerState>,
}

#[derive(Clone)]
pub(super) struct ManagerState {
    topic_policy: Option<Arc<TopicPolicy>>,
    pub(super) topic_interests: Arc<std::sync::OnceLock<super::topics::Bindings>>,
    pub(super) extension: Arc<dyn super::extension::Factory>,
    pub(super) activities: Arc<super::state::Activities>,
    pub(super) handoffs: Arc<handoff::Handoffs>,
    pub(super) gateway_directory: Option<Arc<dyn tok_core::ServiceDirectory>>,
    registry: Arc<Registry>,
    pub(super) subscriptions: Arc<subscriptions::Index>,
    pub(super) config: Arc<Config>,
    pub(super) owner: SessionOwner,
    pub(super) ownership_store: Arc<dyn OwnershipStore>,
    pub(super) pipeline: Arc<crate::player::Pipeline>,
    pub(super) extensions: tok_core::Extensions,
    pub(super) tasks: task::Handle,
    pub(super) gateway: crate::Handle,
    pub(super) shutdown: CancellationToken,
    resume_token_codec: Arc<dyn tok_player::credentials::ResumeTokenCodec>,
}

impl SessionManager {
    pub(crate) fn new(
        config: Arc<Config>,
        owner: SessionOwner,
        ownership_store: Arc<dyn OwnershipStore>,
        executor: Arc<dyn Executor>,
        gateway: crate::Handle,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(ManagerState {
                topic_policy: None,
                topic_interests: Arc::default(),
                extension: Arc::new(super::extension::Empty),
                activities: Arc::default(),
                handoffs: Arc::new(handoff::Handoffs::new(config.max_handoffs)),
                gateway_directory: None,
                resume_token_codec: Arc::new(
                    SignedResumeTokens::ephemeral().map_err(|_| ErrorCode::Internal)?,
                ),
                subscriptions: Arc::new(subscriptions::Index::default()),
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
                pipeline: Arc::new(crate::player::Pipeline::new(executor)),
                extensions: gateway.extensions().clone(),
                tasks: gateway.tasks(),
                gateway,
                shutdown,
            }),
        })
    }

    pub(in crate::player) fn with_topic_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(tok_execution::Request<tok_player::SessionIdentity>) -> Result<Vec<String>>
            + Send
            + Sync
            + 'static,
    {
        Arc::make_mut(&mut self.inner).topic_policy = Some(Arc::new(policy));
        self
    }
    pub(in crate::player) fn bind_topic_interests(
        &self,
        pool: &tok_events::interest::Pool,
        components: Vec<String>,
    ) -> Result<()> {
        let bindings = super::topics::Bindings::new(pool, components)?;
        let slots = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        if !slots.is_empty() {
            return Err(ErrorCode::InvalidState.into());
        }
        self.inner
            .topic_interests
            .set(bindings)
            .map_err(|_| ErrorCode::InvalidState.into())
    }

    pub(in crate::player) fn authenticate_resume_token(
        &self,
        token: &str,
    ) -> Result<tok_player::credentials::ResumeClaims> {
        self.verify_resume_token(token)
    }

    fn verify_resume_token(&self, token: &str) -> Result<tok_player::credentials::ResumeClaims> {
        self.inner
            .resume_token_codec
            .verify(token)
            .map_err(|_| ErrorCode::Internal)?
            .ok_or(ErrorCode::AuthenticationFailed.into())
    }

    /// 注入纯本地签发与验证组件，启动接入前完成组装。
    pub fn with_resume_token_codec(
        mut self,
        codec: Arc<dyn tok_player::credentials::ResumeTokenCodec>,
    ) -> Self {
        Arc::make_mut(&mut self.inner).resume_token_codec = codec;
        self
    }

    /// 注入网关内部 RPC 的发现目录，由组装层配置。
    pub fn with_gateway_directory(
        mut self,
        directory: Arc<dyn tok_core::ServiceDirectory>,
    ) -> Self {
        Arc::make_mut(&mut self.inner).gateway_directory = Some(directory);
        self
    }

    pub(super) fn activity(&self) -> Result<super::state::ActivityGuard> {
        let guard = self.inner.activities.guard();
        if self.inner.shutdown.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        Ok(guard)
    }
    /// 玩家接入阶段及容量快照，不替代登录或业务准入检查。
    pub fn snapshot(&self) -> Result<super::ManagerSnapshot> {
        let sessions = self.sessions()?;
        Ok(super::ManagerSnapshot {
            owner: self.inner.owner.clone(),
            phase: if !self.inner.shutdown.is_cancelled() {
                super::ManagerPhase::Open
            } else if self.inner.activities.count() == 0 {
                super::ManagerPhase::Stopped
            } else {
                super::ManagerPhase::Stopping
            },
            sessions: sessions.len(),
            max_sessions: self.inner.config.max_sessions,
            queue_capacity: self.inner.config.queue_capacity,
        })
    }
    /// 等待停止以及已准入的协调工作和会话 future 释放，不得在自身会话中等待。
    /// 网关 finish 的额外清理由 gateway().wait_finished() 观察。
    pub async fn wait_finished(&self) {
        self.inner.shutdown.cancelled().await;
        self.inner.activities.wait().await;
    }
    pub(in crate::player) fn with_session_extension(
        mut self,
        extension: Arc<dyn super::extension::Factory>,
    ) -> Result<Self> {
        tok_events::validate_name(extension.kind()).map_err(|_| ErrorCode::InvalidRequest)?;
        Arc::make_mut(&mut self.inner).extension = extension;
        Ok(self)
    }
    pub(super) fn extension_context(&self, operation: &str) -> Result<tok_execution::Context> {
        let mut context = tok_execution::Context::new(
            self.inner.gateway.identity().clone(),
            operation,
            Duration::from_millis(self.inner.config.rpc_timeout_ms),
            self.inner.shutdown.child_token(),
        )
        .map_err(|_| ErrorCode::Internal)?;
        context.extensions_mut().extend(&self.inner.extensions);
        context.extensions_mut().insert(self.inner.gateway.clone());
        context.extensions_mut().insert(self.clone());
        Ok(context)
    }
    pub(in crate::player) fn with_pipeline(mut self, pipeline: crate::player::Pipeline) -> Self {
        Arc::make_mut(&mut self.inner).pipeline = Arc::new(pipeline);
        self
    }
    /// 玩家管理器作用域的能力，与消息及事件的执行上下文共用。
    pub fn extensions(&self) -> tok_core::Extensions {
        let mut extensions = self.inner.extensions.clone();
        extensions.insert(self.inner.gateway.clone());
        extensions.insert(self.clone());
        extensions
    }
    pub(in crate::player) fn with_extensions(mut self, extensions: tok_core::Extensions) -> Self {
        Arc::make_mut(&mut self.inner)
            .extensions
            .extend(&extensions);
        self
    }
    /// 当前玩家接入所属网关，提供身份、容量与完整退出观察。
    pub fn gateway(&self) -> crate::Handle {
        self.inner.gateway.clone()
    }
    /// 当前可信所有者身份，不等于 player 业务服务的 RPC 地址。
    pub fn owner(&self) -> &SessionOwner {
        &self.inner.owner
    }
    /// 请求停止所有玩家会话及新登录，不等待清理。
    pub fn request_stop(&self) {
        self.inner.shutdown.cancel();
    }
    /// 玩家接入范围的取消信号。
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }
    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub(crate) fn service_name(&self) -> &str {
        &self.inner.owner.service_name
    }

    // 凭证刷新复用已认证连接，不要求旧 token 仍有效；与顶号和交接串行协调。
    pub(in crate::player) async fn refresh_resume_token(
        &self,
        bound: &BoundSession,
        request_id: u64,
    ) -> Result<()> {
        let _activity = self.activity()?;
        let state = &bound.session.0;
        let slot = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(&state.ownership.identity.player_id)
            .cloned()
            .ok_or(ErrorCode::SessionClosed)?;
        let _gate = tokio::select! {
            biased;
            _ = self.inner.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()),
            _ = bound.binding.cancellation.cancelled() => return Err(ErrorCode::SessionReplaced.into()),
            gate = slot.transition_gate.lock() => gate,
        };
        let current = slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, state));
        if !current || state.cancel.is_cancelled() {
            return Err(ErrorCode::SessionReplaced.into());
        }
        if !renew_ownership(
            state,
            self.inner.ownership_store.as_ref(),
            Duration::from_millis(self.inner.config.rpc_timeout_ms),
            Duration::from_millis(self.inner.config.ownership_ttl_ms),
        )
        .await
        {
            return Err(ErrorCode::SessionClosed.into());
        }
        state.channel.send_bound_frame(&bound.binding, || {
            if state.cancel.is_cancelled() {
                return Err(ErrorCode::SessionClosed.into());
            }
            let token = self
                .inner
                .resume_token_codec
                .issue(
                    &state.ownership.identity,
                    Duration::from_millis(self.inner.config.resume_token_ttl_ms),
                )
                .map_err(|_| ErrorCode::Internal)?;
            Ok(crate::protocol::Frame {
                version: crate::codec::VERSION,
                content: Some(crate::protocol::frame::Content::ResumeTokenRefreshed(
                    crate::protocol::RefreshResumeTokenResponse {
                        request_id,
                        error_code: 0,
                        resume_token: token,
                        resume_token_ttl_ms: self.inner.config.resume_token_ttl_ms,
                    },
                )),
            })
        })
    }

    pub(in crate::player) async fn resume_session(
        &self,
        token: &str,
        ack_seq: u64,
        request_id: u64,
        sender: OutboundSender,
    ) -> Result<BoundSession> {
        let _activity = self.activity()?;
        let claims = self.verify_resume_token(token)?;
        if self.inner.config.handoff_enabled {
            let ownership = self
                .inner
                .ownership_store
                .lookup(&claims.player_id)
                .await
                .map_err(|_| ErrorCode::Unavailable)?
                .ok_or(ErrorCode::ResumeUnavailable)?;
            if ownership.identity.session_id != claims.session_id {
                return Err(ErrorCode::ResumeUnavailable.into());
            }
            if ownership.owner != self.inner.owner {
                return handoff::receive(self, token, ownership, ack_seq, request_id, sender).await;
            }
        }
        let slot = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(&claims.player_id)
            .cloned()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        let _gate = tokio::select! { biased; _ = self.inner.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), gate = slot.transition_gate.lock() => gate };
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
            self.inner.ownership_store.as_ref(),
            Duration::from_millis(self.inner.config.rpc_timeout_ms),
            Duration::from_millis(self.inner.config.ownership_ttl_ms),
        )
        .await
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        session_state.channel.check_interests()?;
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
        let slot = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(player_id)
            .cloned();
        let Some(slot) = slot else {
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

    /// 取得本地尚未关闭的会话快照；事件准入仍需在会话锁下重新校验在线和冻结状态。
    pub fn sessions(&self) -> Result<Vec<SessionHandle>> {
        // 目录锁只用于复制 slot 引用，不在持有目录锁时等待各个 slot。
        let slots: Vec<_> = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .values()
            .cloned()
            .collect();
        let mut sessions = Vec::new();
        for slot in slots {
            let current = slot
                .current_session
                .lock()
                .map_err(|_| ErrorCode::Internal)?;
            if let Some(state) = current
                .as_ref()
                .filter(|state| !state.cancel.is_cancelled())
            {
                sessions.push(SessionHandle(Arc::clone(state)));
            }
        }
        Ok(sessions)
    }
    pub(super) fn slot(&self, player_id: &str) -> Result<Arc<Slot>> {
        let mut slots = self
            .inner
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?;
        if let Some(slot) = slots.get(player_id) {
            return Ok(Arc::clone(slot));
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
        if slots.len() >= self.inner.config.max_sessions {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        let slot = Arc::new(Slot::default());
        slots.insert(player_id.into(), Arc::clone(&slot));
        Ok(slot)
    }

    pub(crate) async fn create_session(
        &self,
        player_id: String,
        sender: OutboundSender,
        previous: Option<tok_player::credentials::ResumeClaims>,
    ) -> Result<SessionHandle> {
        let _activity = self.activity()?;
        if player_id.trim().is_empty() || self.inner.shutdown.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let slot = self.slot(&player_id)?;
        let _gate = tokio::select! { biased; _ = self.inner.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), gate = slot.transition_gate.lock() => gate };
        let old = slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .clone();
        if previous.as_ref().is_some_and(|claims| {
            old.as_ref().is_some_and(|old| {
                !old.cancel.is_cancelled() && old.ownership.identity.session_id != claims.session_id
            })
        }) {
            return Err(ErrorCode::SessionReplaced.into());
        }
        if let Some(old) = &old {
            old.cancel.cancel();
            old.channel.close();
            tokio::select! { biased; _ = self.inner.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()), _ = old.completion.wait() => {} }
        }
        let timeout = Duration::from_millis(self.inner.config.rpc_timeout_ms);
        let existing = tokio::time::timeout(timeout, self.inner.ownership_store.lookup(&player_id))
            .await
            .map_err(|_| ErrorCode::Timeout)?
            .map_err(|_| ErrorCode::Unavailable)?;
        if let Some(existing) = &existing
            && !old.as_ref().is_some_and(|old| old.ownership == *existing)
        {
            return Err(ErrorCode::Unavailable.into());
        }
        let counter = self
            .inner
            .registry
            .session_counter
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ErrorCode::Internal)?;
        let session_id = format!(
            "{}:{}:{}:{counter}",
            self.inner.owner.node_id,
            self.inner.owner.service_name,
            self.inner.registry.process_instance_id
        );
        let ttl = Duration::from_millis(self.inner.config.ownership_ttl_ms);
        let expected_epoch = existing.map(|record| record.identity.epoch);
        let acquire = match previous {
            Some(previous) => self.inner.ownership_store.acquire_for_relogin(
                previous,
                session_id,
                self.inner.owner.clone(),
                expected_epoch,
                ttl,
            ),
            None => self.inner.ownership_store.acquire(
                player_id,
                session_id,
                self.inner.owner.clone(),
                expected_epoch,
                ttl,
            ),
        };
        let ownership = tokio::time::timeout(timeout, acquire)
            .await
            .map_err(|_| ErrorCode::Timeout)?
            .map_err(|error| match error {
                tok_player::Error::OwnershipConflict => ErrorCode::SessionReplaced,
                _ => ErrorCode::Unavailable,
            })?;
        let resume_token = match self.inner.resume_token_codec.issue(
            &ownership.identity,
            Duration::from_millis(self.inner.config.resume_token_ttl_ms),
        ) {
            Ok(token) => token,
            Err(_) => {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "签发失败后尽力释放，有限租期兜底，不覆盖签发错误。"
                )]
                let _ =
                    tokio::time::timeout(timeout, self.inner.ownership_store.release(ownership))
                        .await;
                return Err(ErrorCode::Internal.into());
            }
        };
        let extensions = match self.inner.extension.initialize(tok_execution::Request::new(
            self.extension_context("player/session/initialize")?,
            ownership.identity.clone(),
        )) {
            Ok(extensions) => extensions,
            Err(error) => {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "扩展初始化失败后尽力条件释放；保留原始初始化错误，有限租期兜底。"
                )]
                let _ =
                    tokio::time::timeout(timeout, self.inner.ownership_store.release(ownership))
                        .await;
                return Err(error);
            }
        };
        let (job_sender, receiver) = mpsc::channel(self.inner.config.queue_capacity);
        let cancel = self.inner.shutdown.child_token();
        let session_state = Arc::new(SessionState {
            manager: Arc::downgrade(&self.inner),
            extension: Arc::clone(&self.inner.extension),
            closing_notified: AtomicBool::new(false),
            extensions,
            pipeline: Arc::clone(&self.inner.pipeline),
            execution_budget: timeout,
            downlink_gate: tokio::sync::Mutex::new(()),
            topic_gate: tokio::sync::Mutex::new(()),
            timers: Arc::new(timer::Scheduler::new(
                self.inner.config.max_timers,
                self.inner.config.max_timer_bytes,
            )),
            initial_execution: None,
            ownership,
            channel: Arc::new(channel::Channel::new(
                sender,
                &self.inner.config,
                cancel.clone(),
            )),
            cancel,
            resume_token,
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            job_sender,
        });
        session_state
            .channel
            .bind_topic_membership(Arc::clone(&self.inner.subscriptions), &session_state)?;
        SessionHandle(Arc::clone(&session_state)).changed(super::extension::Change::Initialized);
        // 登录 future 在运行时接收任务后被取消时，不能遗留无人绑定却持续续租的 session。
        let opening = session_state.cancel.clone().drop_guard();
        // 在下一次 await 前登记已确认的租约，取消登录时退出流程仍能条件释放。
        *slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)? = Some(Arc::clone(&session_state));
        let task = worker::task(
            Arc::clone(&session_state),
            receiver,
            Arc::clone(&self.inner.config),
            Arc::clone(&self.inner.ownership_store),
            Arc::clone(&self.inner.pipeline) as Arc<dyn Executor>,
        );
        let task = Box::pin(async move {
            task.await;
            Ok(())
        });
        let launched = tokio::select! { biased; _ = self.inner.shutdown.cancelled() => false, result = self.inner.tasks.launch("player-session", task) => result.is_ok() };
        if !launched {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "任务启动失败后尽力释放，有限租期兜底，不覆盖关闭错误。"
            )]
            let _ = tokio::time::timeout(
                timeout,
                self.inner
                    .ownership_store
                    .release(session_state.ownership.clone()),
            )
            .await;
            return Err(ErrorCode::SessionClosed.into());
        }
        if session_state.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let handle = SessionHandle(Arc::clone(&session_state));
        if let Some(policy) = &self.inner.topic_policy {
            let result = tokio::time::timeout(timeout, async {
                let topics = policy(tok_execution::Request::new(
                    handle.context("player/session/topics".into())?,
                    handle.identity().clone(),
                ))?;
                if topics.len() > self.inner.config.max_topics {
                    return Err(ErrorCode::CapacityExceeded.into());
                }
                for topic in topics {
                    handle.join_topic(&topic).await?;
                }
                Ok(())
            })
            .await
            .unwrap_or(Err(ErrorCode::Timeout.into()));
            if let Err(error) = result {
                handle.close();
                return Err(error);
            }
        }
        let _ = opening.disarm();
        Ok(handle)
    }

    pub(crate) fn close_all(&self) {
        self.inner.shutdown.cancel();
    }

    pub(crate) async fn release_all_ownership(&self) {
        let records = self
            .inner
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
            #[expect(
                clippy::let_underscore_must_use,
                reason = "退出阶段尽力条件释放，后端失败由有限租期兜底。"
            )]
            let _ = tokio::time::timeout(
                Duration::from_millis(self.inner.config.rpc_timeout_ms),
                self.inner.ownership_store.release(record),
            )
            .await;
        }
    }
}
