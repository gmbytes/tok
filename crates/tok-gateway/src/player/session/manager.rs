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
use tok_core::coordination::{OwnershipStore, SessionOwner};
use tok_service::task;
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

/// 有界的玩家会话目录，由 Ingress 构造并通过 sessions 获取。
///
/// 持有者不能直接创建未认证的 session；不同玩家的登录协调锁互相独立。
#[derive(Clone)]
pub struct SessionManager {
    pub(super) handoffs: Arc<handoff::Handoffs>,
    pub(super) gateway_directory: Option<Arc<dyn tok_core::ServiceDirectory>>,
    registry: Arc<Registry>,
    pub(super) subscriptions: Arc<subscriptions::Index>,
    pub(super) config: Arc<Config>,
    pub(super) owner: SessionOwner,
    pub(super) ownership_store: Arc<dyn OwnershipStore>,
    pub(super) executor: Arc<dyn Executor>,
    pub(super) tasks: task::Handle,
    pub(super) shutdown: CancellationToken,
    resume_token_codec: Arc<dyn tok_core::credentials::ResumeTokenCodec>,
}

impl SessionManager {
    pub(crate) fn new(
        config: Arc<Config>,
        owner: SessionOwner,
        ownership_store: Arc<dyn OwnershipStore>,
        executor: Arc<dyn Executor>,
        tasks: task::Handle,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
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
            executor,
            tasks,
            shutdown,
        })
    }

    pub(in crate::player) fn authenticate_resume_token(
        &self,
        token: &str,
    ) -> Result<tok_core::credentials::ResumeClaims> {
        self.verify_resume_token(token)
    }

    fn verify_resume_token(&self, token: &str) -> Result<tok_core::credentials::ResumeClaims> {
        self.resume_token_codec
            .verify(token)
            .map_err(|_| ErrorCode::Internal)?
            .ok_or(ErrorCode::AuthenticationFailed.into())
    }

    /// 注入纯本地签发与验证组件，启动接入前完成组装。
    pub fn with_resume_token_codec(
        mut self,
        codec: Arc<dyn tok_core::credentials::ResumeTokenCodec>,
    ) -> Self {
        self.resume_token_codec = codec;
        self
    }

    /// 注入网关内部 RPC 的发现目录，由组装层配置。
    pub fn with_gateway_directory(
        mut self,
        directory: Arc<dyn tok_core::ServiceDirectory>,
    ) -> Self {
        self.gateway_directory = Some(directory);
        self
    }

    pub(crate) fn service_name(&self) -> &str {
        &self.owner.service_name
    }

    // 凭证刷新复用已认证连接，不要求旧 token 仍有效；与顶号和交接串行协调。
    pub(in crate::player) async fn refresh_resume_token(
        &self,
        bound: &BoundSession,
        request_id: u64,
    ) -> Result<()> {
        let state = &bound.session.0;
        let slot = self
            .registry
            .slots
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(&state.ownership.identity.player_id)
            .cloned()
            .ok_or(ErrorCode::SessionClosed)?;
        let _gate = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()),
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
            self.ownership_store.as_ref(),
            Duration::from_millis(self.config.rpc_timeout_ms),
            Duration::from_millis(self.config.ownership_ttl_ms),
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
                .resume_token_codec
                .issue(
                    &state.ownership.identity,
                    Duration::from_millis(self.config.resume_token_ttl_ms),
                )
                .map_err(|_| ErrorCode::Internal)?;
            Ok(crate::protocol::Frame {
                version: crate::codec::VERSION,
                content: Some(crate::protocol::frame::Content::ResumeTokenRefreshed(
                    crate::protocol::RefreshResumeTokenResponse {
                        request_id,
                        error_code: 0,
                        resume_token: token,
                        resume_token_ttl_ms: self.config.resume_token_ttl_ms,
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
        let claims = self.verify_resume_token(token)?;
        if self.config.handoff_enabled {
            let ownership = self
                .ownership_store
                .lookup(&claims.player_id)
                .await
                .map_err(|_| ErrorCode::Unavailable)?
                .ok_or(ErrorCode::ResumeUnavailable)?;
            if ownership.identity.session_id != claims.session_id {
                return Err(ErrorCode::ResumeUnavailable.into());
            }
            if ownership.owner != self.owner {
                return handoff::receive(self, token, ownership, ack_seq, request_id, sender).await;
            }
        }
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
        let slot = self
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
        if slots.len() >= self.config.max_sessions {
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
        previous: Option<tok_core::credentials::ResumeClaims>,
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
        let ttl = Duration::from_millis(self.config.ownership_ttl_ms);
        let expected_epoch = existing.map(|record| record.identity.epoch);
        let acquire = match previous {
            Some(previous) => self.ownership_store.acquire_for_relogin(
                previous,
                session_id,
                self.owner.clone(),
                expected_epoch,
                ttl,
            ),
            None => self.ownership_store.acquire(
                player_id,
                session_id,
                self.owner.clone(),
                expected_epoch,
                ttl,
            ),
        };
        let ownership = tokio::time::timeout(timeout, acquire)
            .await
            .map_err(|_| ErrorCode::Timeout)?
            .map_err(|error| match error {
                tok_core::Error::OwnershipConflict => ErrorCode::SessionReplaced,
                _ => ErrorCode::Unavailable,
            })?;
        let resume_token = match self.resume_token_codec.issue(
            &ownership.identity,
            Duration::from_millis(self.config.resume_token_ttl_ms),
        ) {
            Ok(token) => token,
            Err(_) => {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "签发失败后尽力释放，有限租期兜底，不覆盖签发错误。"
                )]
                let _ =
                    tokio::time::timeout(timeout, self.ownership_store.release(ownership)).await;
                return Err(ErrorCode::Internal.into());
            }
        };
        let (job_sender, receiver) = mpsc::channel(self.config.queue_capacity);
        let cancel = self.shutdown.child_token();
        let session_state = Arc::new(SessionState {
            timers: Arc::new(timer::Scheduler::new(
                self.config.max_timers,
                self.config.max_timer_bytes,
            )),
            initial_execution: None,
            ownership,
            channel: Arc::new(channel::Channel::new(sender, &self.config, cancel.clone())),
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
            .bind_topic_membership(Arc::clone(&self.subscriptions), &session_state)?;
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
            Arc::clone(&self.config),
            Arc::clone(&self.ownership_store),
            Arc::clone(&self.executor),
        );
        let task = Box::pin(async move {
            task.await;
            Ok(())
        });
        let launched = tokio::select! { biased; _ = self.shutdown.cancelled() => false, result = self.tasks.launch("player-session", task) => result.is_ok() };
        if !launched {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "任务启动失败后尽力释放，有限租期兜底，不覆盖关闭错误。"
            )]
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
            #[expect(
                clippy::let_underscore_must_use,
                reason = "退出阶段尽力条件释放，后端失败由有限租期兜底。"
            )]
            let _ = tokio::time::timeout(
                Duration::from_millis(self.config.rpc_timeout_ms),
                self.ownership_store.release(record),
            )
            .await;
        }
    }
}
