//! 源网关冻结和提交协调、目标预留与接管；全部运行任务由网关监督。

use super::{
    channel::{self, OutboundSender},
    handle::{BoundSession, SessionHandle},
    manager::SessionManager,
    state::{Completion, SessionState},
    timer,
    worker::{self, Job, renew_ownership},
};
use crate::{ErrorCode, Result};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};
use tok_core::{
    SessionIdentity,
    coordination::{SessionOwner, SessionOwnership},
};
use tok_rpc::{
    protocol::internal::v1 as proto,
    tonic::{self, Status},
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedMutexGuard},
    time::{Instant, timeout_at},
};

pub(super) struct Handoffs {
    records: Mutex<BTreeMap<String, Arc<AsyncMutex<Record>>>>,
    capacity: Arc<tokio::sync::Semaphore>,
}
impl Handoffs {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            capacity: Arc::new(tokio::sync::Semaphore::new(capacity)),
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Preparing,
    Prepared,
    Committing,
    Committed,
    Cancelled,
}
pub(crate) enum HandoffAction {
    Commit,
    Cancel,
    Complete,
}
struct Record {
    _permit: tokio::sync::OwnedSemaphorePermit,
    request: proto::HandoffRequest,
    session: Arc<SessionState>,
    gate: Option<OwnedMutexGuard<()>>,
    deadline: Instant,
    phase: Phase,
    snapshot: Option<proto::HandoffSnapshot>,
    ownership: Option<SessionOwnership>,
}
pub(crate) fn encode_owner(owner: &SessionOwner) -> proto::GatewayOwner {
    proto::GatewayOwner {
        node_id: owner.node_id.clone(),
        service_name: owner.service_name.clone(),
    }
}
pub(crate) fn encode_ownership(value: &SessionOwnership) -> proto::GatewayOwnership {
    proto::GatewayOwnership {
        identity: Some(proto::SessionIdentity {
            player_id: value.identity.player_id.clone(),
            session_id: value.identity.session_id.clone(),
            epoch: value.identity.epoch,
        }),
        owner: Some(encode_owner(&value.owner)),
    }
}
fn owner(value: Option<&proto::GatewayOwner>) -> Result<SessionOwner> {
    let value = value.ok_or(ErrorCode::InvalidRequest)?;
    if [&value.node_id, &value.service_name]
        .iter()
        .any(|value| value.trim().is_empty() || value.len() > 256)
    {
        return Err(ErrorCode::InvalidRequest.into());
    }
    Ok(SessionOwner {
        node_id: value.node_id.clone(),
        service_name: value.service_name.clone(),
    })
}
fn ownership(value: Option<&proto::GatewayOwnership>) -> Result<SessionOwnership> {
    let value = value.ok_or(ErrorCode::InvalidRequest)?;
    let identity = value.identity.as_ref().ok_or(ErrorCode::InvalidRequest)?;
    if identity.player_id.is_empty()
        || identity.player_id.len() > 256
        || identity.session_id.is_empty()
        || identity.session_id.len() > 1024
        || identity.epoch == 0
    {
        return Err(ErrorCode::InvalidRequest.into());
    }
    Ok(SessionOwnership {
        identity: SessionIdentity {
            player_id: identity.player_id.clone(),
            session_id: identity.session_id.clone(),
            epoch: identity.epoch,
        },
        owner: owner(value.owner.as_ref())?,
    })
}
fn same_request(a: &proto::HandoffRequest, b: &proto::HandoffRequest) -> bool {
    a.expected == b.expected
        && a.target == b.target
        && a.ack_seq == b.ack_seq
        && a.source_service_name == b.source_service_name
}
fn outcome(record: &Record) -> proto::HandoffResponse {
    proto::HandoffResponse {
        snapshot: None,
        ownership: record.ownership.as_ref().map(encode_ownership),
    }
}
fn response(record: &Record) -> proto::HandoffResponse {
    proto::HandoffResponse {
        snapshot: record.snapshot.clone(),
        ownership: record.ownership.as_ref().map(encode_ownership),
    }
}
async fn abort(manager: &SessionManager, record: &mut Record) {
    let can_resume = matches!(record.phase, Phase::Preparing | Phase::Prepared);
    record.phase = Phase::Cancelled;
    if can_resume
        && renew_ownership(
            &record.session,
            manager.inner.ownership_store.as_ref(),
            Duration::from_millis(manager.inner.config.rpc_timeout_ms),
            Duration::from_millis(manager.inner.config.ownership_ttl_ms),
        )
        .await
        && record.session.channel.cancel_handoff().is_ok()
    {
        record.session.timers.resume();
        SessionHandle(Arc::clone(&record.session))
            .changed(super::extension::Change::HandoffCancelled);
    } else {
        SessionHandle(Arc::clone(&record.session)).close();
    }
    record.gate.take();
}

impl SessionManager {
    pub(crate) async fn prepare_handoff(
        &self,
        request: proto::HandoffRequest,
    ) -> Result<proto::HandoffResponse> {
        let _activity = self.activity()?;
        if !self.inner.config.handoff_enabled
            || uuid::Uuid::parse_str(&request.handoff_id).is_err()
            || request.budget_ms == 0
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let expected = ownership(request.expected.as_ref())?;
        let target = owner(request.target.as_ref())?;
        if expected.owner != self.inner.owner || target == self.inner.owner {
            return Err(ErrorCode::SessionReplaced.into());
        }
        let existing = {
            self.inner
                .handoffs
                .records
                .lock()
                .map_err(|_| ErrorCode::Internal)?
                .get(&request.handoff_id)
                .cloned()
        };
        if let Some(existing) = existing {
            let record = existing.lock().await;
            if !same_request(&record.request, &request)
                || record.phase != Phase::Prepared
                || record.deadline <= Instant::now()
            {
                return Err(ErrorCode::InvalidState.into());
            }
            return Ok(response(&record));
        }
        let slot = self.slot(&expected.identity.player_id)?;
        let permit = Arc::clone(&self.inner.handoffs.capacity)
            .try_acquire_owned()
            .map_err(|_| ErrorCode::CapacityExceeded)?;
        let gate = Arc::clone(&slot.transition_gate)
            .try_lock_owned()
            .map_err(|_| ErrorCode::InvalidState)?;
        let session = {
            slot.current_session
                .lock()
                .map_err(|_| ErrorCode::Internal)?
                .clone()
                .ok_or(ErrorCode::ResumeUnavailable)?
        };
        if session.ownership != expected || session.cancel.is_cancelled() {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        session.channel.check_resume_position(request.ack_seq)?;
        let reservation = self
            .inner
            .tasks
            .reserve()
            .map_err(|_| ErrorCode::CapacityExceeded)?;
        let deadline = Instant::now()
            + Duration::from_millis(request.budget_ms.min(self.inner.config.handoff_timeout_ms));
        let record = Arc::new(AsyncMutex::new(Record {
            _permit: permit,
            request: request.clone(),
            session: Arc::clone(&session),
            gate: Some(gate),
            deadline,
            phase: Phase::Preparing,
            snapshot: None,
            ownership: None,
        }));
        {
            let mut records = self
                .inner
                .handoffs
                .records
                .lock()
                .map_err(|_| ErrorCode::Internal)?;
            if records.len() >= self.inner.config.max_handoffs
                || records.contains_key(&request.handoff_id)
            {
                return Err(ErrorCode::CapacityExceeded.into());
            }
            records.insert(request.handoff_id.clone(), Arc::clone(&record));
        }
        let manager = self.clone();
        let activity = self.inner.activities.guard();
        let tracked = Arc::clone(&record);
        let id = request.handoff_id.clone();
        // 由网关监督的截止任务在 RPC 被取消时仍负责解冻或停止旧入口。
        if reservation.launch("handoff-deadline", Box::pin(async move {
            let _activity=activity;
            tokio::select! { _ = manager.inner.shutdown.cancelled() => {}, _ = tokio::time::sleep_until(deadline) => {} }
            {
                let mut record = tracked.lock().await;
                if record.phase != Phase::Committed && record.phase != Phase::Cancelled { abort(&manager, &mut record).await; }
            }
            tokio::select! { _ = manager.inner.shutdown.cancelled() => {}, _ = tokio::time::sleep(Duration::from_millis(manager.inner.config.handoff_timeout_ms)) => {} }
            if let Ok(mut records) = manager.inner.handoffs.records.lock() { records.remove(&id); }
            Ok(())
        })).await.is_err() {
            self.inner.handoffs.records.lock().map_err(|_| ErrorCode::Internal)?.remove(&request.handoff_id);
            return Err(ErrorCode::SessionClosed.into());
        }
        let mut record = record.lock().await;
        let result = timeout_at(deadline, async {
            if !renew_ownership(
                &session,
                self.inner.ownership_store.as_ref(),
                Duration::from_millis(self.inner.config.rpc_timeout_ms),
                Duration::from_millis(self.inner.config.ownership_ttl_ms),
            )
            .await
            {
                return Err(ErrorCode::ResumeUnavailable.into());
            }
            session.channel.begin_handoff()?;
            SessionHandle(Arc::clone(&session)).changed(super::extension::Change::HandoffPreparing);
            session.timers.pause();
            let (sender, receiver) = oneshot::channel();
            session
                .job_sender
                .send(Job::Snapshot(sender))
                .await
                .map_err(|_| ErrorCode::SessionClosed)?;
            let execution = receiver.await.map_err(|_| ErrorCode::SessionClosed)??;
            let mut channel = session.channel.snapshot(request.ack_seq)?;
            channel.last_request_id = channel.last_request_id.max(execution.last_request_id);
            let snapshot = proto::HandoffSnapshot {
                version: 1,
                snapshot_id: uuid::Uuid::new_v4().to_string(),
                source: Some(encode_ownership(&expected)),
                channel: Some(channel),
                execution: Some(execution),
                extension: Some(proto::ExtensionSnapshot {
                    kind: session.extension.kind().to_owned(),
                    body: session.extension.snapshot(tok_service::Request::new(
                        SessionHandle(Arc::clone(&session)).context(
                            "player/session/snapshot".into(),
                            tok_core::execution::Source::Internal,
                        )?,
                        SessionHandle(Arc::clone(&session)),
                    ))?,
                }),
            };
            if Instant::now() >= deadline {
                return Err(ErrorCode::Timeout.into());
            }
            if snapshot.encoded_len() > self.inner.config.max_handoff_bytes {
                return Err(ErrorCode::CapacityExceeded.into());
            }
            Ok(snapshot)
        })
        .await
        .unwrap_or(Err(ErrorCode::Timeout.into()));
        match result {
            Ok(snapshot) => {
                record.snapshot = Some(snapshot);
                record.phase = Phase::Prepared;
                Ok(response(&record))
            }
            Err(error) => {
                abort(self, &mut record).await;
                Err(error)
            }
        }
    }

    pub(crate) async fn finish_handoff(
        &self,
        request: proto::HandoffRequest,
        action: HandoffAction,
    ) -> Result<proto::HandoffResponse> {
        let _activity = self.activity()?;
        let shared = {
            self.inner
                .handoffs
                .records
                .lock()
                .map_err(|_| ErrorCode::Internal)?
                .get(&request.handoff_id)
                .cloned()
                .ok_or(ErrorCode::ResumeUnavailable)?
        };
        let mut record = shared.lock().await;
        if !same_request(&record.request, &request) {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if !matches!(action, HandoffAction::Cancel)
            && record
                .snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.snapshot_id != request.snapshot_id)
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if matches!(action, HandoffAction::Cancel) {
            if record.phase == Phase::Preparing || record.phase == Phase::Prepared {
                abort(self, &mut record).await;
            }
            return Ok(outcome(&record));
        }
        if record.phase == Phase::Committed {
            return Ok(outcome(&record));
        }
        if matches!(action, HandoffAction::Complete)
            || record.phase != Phase::Prepared
            || record.deadline <= Instant::now()
            || record
                .snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.snapshot_id != request.snapshot_id)
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        record.phase = Phase::Committing;
        let target = owner(request.target.as_ref())?;
        let expected = record.session.ownership.clone();
        let transferred = timeout_at(
            record.deadline,
            self.inner.ownership_store.transfer(
                expected.clone(),
                target.clone(),
                Duration::from_millis(self.inner.config.ownership_ttl_ms),
            ),
        )
        .await;
        let result = match transferred {
            Ok(Ok(value)) => Some(value),
            _ => tokio::time::timeout(
                Duration::from_millis(self.inner.config.rpc_timeout_ms),
                self.inner
                    .ownership_store
                    .lookup(&expected.identity.player_id),
            )
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .flatten()
            .filter(|value| {
                value.owner == target
                    && value.identity.session_id == expected.identity.session_id
                    && value.identity.epoch > expected.identity.epoch
            }),
        };
        // 从提交开始后不再恢复 A；未知结果也只允许查询确认或等待租约到期。
        if result.is_some() {
            SessionHandle(Arc::clone(&record.session))
                .changed(super::extension::Change::HandoffCommitted);
        }
        SessionHandle(Arc::clone(&record.session)).close();
        record.gate.take();
        match result {
            Some(value) => {
                record.ownership = Some(value);
                record.phase = Phase::Committed;
                Ok(outcome(&record))
            }
            None => {
                record.phase = Phase::Cancelled;
                Err(ErrorCode::ResumeUnavailable.into())
            }
        }
    }
}

fn rpc_error(status: Status) -> crate::GatewayError {
    match status.code() {
        tonic::Code::ResourceExhausted => ErrorCode::CapacityExceeded,
        tonic::Code::InvalidArgument => ErrorCode::InvalidRequest,
        tonic::Code::DeadlineExceeded => ErrorCode::Timeout,
        _ => ErrorCode::ResumeUnavailable,
    }
    .into()
}

pub(super) async fn receive(
    manager: &SessionManager,
    token: &str,
    expected: SessionOwnership,
    ack: u64,
    request_id: u64,
    sender: OutboundSender,
) -> Result<BoundSession> {
    let _activity = manager.activity()?;
    let _permit = Arc::clone(&manager.inner.handoffs.capacity)
        .try_acquire_owned()
        .map_err(|_| ErrorCode::CapacityExceeded)?;
    let started = Instant::now();
    let deadline = started + Duration::from_millis(manager.inner.config.handoff_timeout_ms);
    let slot = manager.slot(&expected.identity.player_id)?;
    let _gate = timeout_at(deadline, slot.transition_gate.lock())
        .await
        .map_err(|_| ErrorCode::Timeout)?;
    let old = {
        slot.current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .clone()
    };
    if let Some(old) = old {
        SessionHandle(Arc::clone(&old)).close();
        timeout_at(deadline, old.completion.wait())
            .await
            .map_err(|_| ErrorCode::Timeout)?;
    }
    let reservation = manager
        .inner
        .tasks
        .reserve()
        .map_err(|_| ErrorCode::CapacityExceeded)?;
    let directory = manager
        .inner
        .gateway_directory
        .as_ref()
        .ok_or(ErrorCode::Unavailable)?;
    let target = directory
        .snapshot(&manager.inner.config.gateway_rpc_service_name)
        .map_err(|_| ErrorCode::Unavailable)?
        .into_iter()
        .find(|entry| entry.node_id == expected.owner.node_id && entry.protocol == "grpc")
        .ok_or(ErrorCode::ResumeUnavailable)?;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", target.address))
        .map_err(|_| ErrorCode::Unavailable)?
        .connect_timeout(Duration::from_millis(manager.inner.config.rpc_timeout_ms));
    let channel = endpoint.connect_lazy();
    let mut client = proto::gateway_rpc_client::GatewayRpcClient::new(channel)
        .max_decoding_message_size(manager.inner.config.max_handoff_bytes + 65536)
        .max_encoding_message_size(manager.inner.config.max_handoff_bytes + 65536);
    let mut request = proto::HandoffRequest {
        handoff_id: uuid::Uuid::new_v4().to_string(),
        expected: Some(encode_ownership(&expected)),
        target: Some(encode_owner(&manager.inner.owner)),
        ack_seq: ack,
        snapshot_id: String::new(),
        budget_ms: deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u64,
        source_service_name: expected.owner.service_name.clone(),
    };
    let prepared = match timeout_at(deadline, client.prepare_handoff(request.clone())).await {
        Ok(Ok(response)) => response.into_inner(),
        result => {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "取消通知尽力发送，源端监督期限兜底清理，不覆盖原始失败。"
            )]
            let _ = tokio::time::timeout(
                Duration::from_millis(manager.inner.config.rpc_timeout_ms),
                client.cancel_handoff(request.clone()),
            )
            .await;
            return Err(match result {
                Err(_) => ErrorCode::Timeout.into(),
                Ok(Err(status)) => rpc_error(status),
                _ => ErrorCode::Internal.into(),
            });
        }
    };
    let snapshot = prepared.snapshot.ok_or(ErrorCode::ResumeUnavailable)?;
    let result = async {
        if snapshot.version != 1
            || snapshot.source != Some(encode_ownership(&expected))
            || snapshot.encoded_len() > manager.inner.config.max_handoff_bytes
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let extension = snapshot
            .extension
            .as_ref()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        if extension.kind != manager.inner.extension.kind() {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        manager.inner.extension.validate(&extension.body)?;
        let channel_snapshot = snapshot
            .channel
            .as_ref()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        let execution = snapshot
            .execution
            .as_ref()
            .ok_or(ErrorCode::ResumeUnavailable)?;
        if execution.last_request_id > channel_snapshot.last_request_id {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let cancel = manager.inner.shutdown.child_token();
        let channel = Arc::new(channel::Channel::restore(
            channel_snapshot,
            started.elapsed(),
            &manager.inner.config,
            cancel.clone(),
            sender.clone(),
        )?);
        if channel.replay_after(ack)?.is_none() {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let timers = Arc::new(timer::Scheduler::new(
            manager.inner.config.max_timers,
            manager.inner.config.max_timer_bytes,
        ));
        timers.restore(execution)?;
        // 取得所有权之前建立传输兴趣；尚未登记目标会话，不允许向它投递。
        let mut interests = std::collections::BTreeMap::new();
        if let Some(bindings) = manager.inner.topic_interests.get() {
            for topic in &channel_snapshot.topics {
                let leases = tokio::select! {
                    biased;
                    () = manager.inner.shutdown.cancelled() => return Err(ErrorCode::SessionClosed.into()),
                    result = timeout_at(deadline, bindings.acquire(topic)) => result.map_err(|_| ErrorCode::Timeout)??,
                };
                interests.insert(topic.clone(), leases);
            }
        }
        channel.install_interests(interests)?;
        channel.check_interests()?;
        request.snapshot_id = snapshot.snapshot_id.clone();
        let committed = timeout_at(deadline, client.commit_handoff(request.clone())).await;
        let acquired = match committed {
            Ok(Ok(reply)) => ownership(reply.into_inner().ownership.as_ref())?,
            _ => tokio::time::timeout(
                Duration::from_millis(manager.inner.config.rpc_timeout_ms),
                manager
                    .inner
                    .ownership_store
                    .lookup(&expected.identity.player_id),
            )
            .await
            .map_err(|_| ErrorCode::Timeout)?
            .map_err(|_| ErrorCode::Unavailable)?
            .ok_or(ErrorCode::ResumeUnavailable)?,
        };
        if acquired.owner != manager.inner.owner
            || acquired.identity.session_id != expected.identity.session_id
            || acquired.identity.player_id != expected.identity.player_id
            || acquired.identity.epoch <= expected.identity.epoch
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        if Instant::now() >= deadline || manager.inner.shutdown.is_cancelled() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "失败路径尽力条件释放，租约到期兜底，不覆盖原始错误。"
            )]
            let _ = tokio::time::timeout(
                Duration::from_millis(manager.inner.config.rpc_timeout_ms),
                manager.inner.ownership_store.release(acquired),
            )
            .await;
            return Err(ErrorCode::Timeout.into());
        }
        let extensions = match manager.inner.extension.restore(tok_service::Request::new(
            manager.extension_context("player/session/restore")?,
            (acquired.identity.clone(), extension.body.as_slice()),
        )) {
            Ok(extensions) => extensions,
            Err(error) => {
                release_failed_restore(manager, acquired).await;
                return Err(error);
            }
        };
        if Instant::now() >= deadline || manager.inner.shutdown.is_cancelled() {
            release_failed_restore(manager, acquired).await;
            return Err(ErrorCode::Timeout.into());
        }
        let (job_sender, receiver) = mpsc::channel(manager.inner.config.queue_capacity);
        let state = Arc::new(SessionState {
            manager: Arc::downgrade(&manager.inner),
            extension: Arc::clone(&manager.inner.extension),
            closing_notified: AtomicBool::new(false),
            extensions,
            pipeline: Arc::clone(&manager.inner.pipeline),
            execution_budget: Duration::from_millis(manager.inner.config.rpc_timeout_ms),
            downlink_gate: tokio::sync::Mutex::new(()),
            topic_gate: tokio::sync::Mutex::new(()),
            timers,
            initial_execution: Some(execution.clone()),
            ownership: acquired,
            cancel,
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            job_sender,
            channel,
            resume_token: token.to_owned(),
        });
        state
            .channel
            .bind_topic_membership(Arc::clone(&manager.inner.subscriptions), &state)?;
        SessionHandle(Arc::clone(&state)).changed(super::extension::Change::Restored);
        let opening = state.cancel.clone().drop_guard();
        *slot
            .current_session
            .lock()
            .map_err(|_| ErrorCode::Internal)? = Some(Arc::clone(&state));
        let task = worker::task(
            Arc::clone(&state),
            receiver,
            Arc::clone(&manager.inner.config),
            Arc::clone(&manager.inner.ownership_store),
            Arc::clone(&manager.inner.pipeline) as Arc<dyn crate::player::Executor>,
        );
        if reservation
            .launch(
                "player-session",
                Box::pin(async move {
                    task.await;
                    Ok(())
                }),
            )
            .await
            .is_err()
        {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "启动失败后尽力条件释放，有限租期兜底，不覆盖原始错误。"
            )]
            let _ = tokio::time::timeout(
                Duration::from_millis(manager.inner.config.rpc_timeout_ms),
                manager
                    .inner
                    .ownership_store
                    .release(state.ownership.clone()),
            )
            .await;
            return Err(ErrorCode::SessionClosed.into());
        }
        // 旧端已在提交时失效；完成通知失败不会回滚 B。
        #[expect(
            clippy::let_underscore_must_use,
            reason = "所有权已提交，完成通知失败由源端监督收束，不回滚接管。"
        )]
        let _ = tokio::time::timeout(
            Duration::from_millis(manager.inner.config.rpc_timeout_ms)
                .min(deadline.saturating_duration_since(Instant::now())),
            client.complete_handoff(request.clone()),
        )
        .await;
        if Instant::now() >= deadline || state.cancel.is_cancelled() {
            SessionHandle(Arc::clone(&state)).close();
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let identity = &state.ownership.identity;
        let binding = state.channel.rebind_connection(
            sender,
            ack,
            crate::protocol::ResumeResponse {
                request_id,
                error_code: 0,
                player_id: identity.player_id.clone(),
                session_id: identity.session_id.clone(),
                epoch: identity.epoch,
                last_request_id: 0,
            },
        )?;
        let _ = opening.disarm();
        Ok(BoundSession {
            session: SessionHandle(state),
            binding,
        })
    }
    .await;
    if result.is_err() {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "取消通知尽力发送，源端监督期限兜底清理，不覆盖原始失败。"
        )]
        let _ = tokio::time::timeout(
            Duration::from_millis(manager.inner.config.rpc_timeout_ms),
            client.cancel_handoff(request),
        )
        .await;
    }
    result
}

async fn release_failed_restore(manager: &SessionManager, ownership: SessionOwnership) {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "目标端恢复失败后尽力条件释放，有限租期兜底，不回滚已提交的旧端。"
    )]
    let _ = tokio::time::timeout(
        Duration::from_millis(manager.inner.config.rpc_timeout_ms),
        manager.inner.ownership_store.release(ownership),
    )
    .await;
}
