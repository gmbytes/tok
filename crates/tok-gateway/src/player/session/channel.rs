//! 会话通信通道：连接绑定、请求准入、下行发布、确认缓存及断线恢复。

use super::{
    replay::{ReplayBatch, ReplayCache},
    state::SessionState,
    subscriptions::{Index, TopicMembership},
};
use crate::player::Config;
use crate::{
    ErrorCode, Result,
    codec::{FrameCodec, VERSION},
    protocol::{self, Frame, frame::Content},
};
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tok_core::{LogicalMessage, MessageKind};
use tokio::{
    io::AsyncWrite,
    sync::{Notify, mpsc, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

// 连接写循环的工作项；实际 socket 写入只由写循环执行。
pub(crate) enum Outbound {
    Frame(Frame, Option<oneshot::Sender<()>>),
    Encoded(Arc<[u8]>),
    Replay(ReplayPlan),
}
pub(crate) type OutboundSender = mpsc::Sender<Outbound>;

#[derive(Clone)]
// 一次连接绑定的代次和取消令牌；不是纯数值 ID，也不是恢复凭证。
pub(super) struct Binding {
    generation: u64,
    pub(super) cancellation: CancellationToken,
}

impl Binding {
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

struct AttachedConnection {
    binding: Binding,
    outbound_sender: OutboundSender,
    replaying: bool,
}

// 需要在同一锁下协调的通道可变状态，不包含固定配置及自身支持并发访问的同步工具。
struct ChannelState {
    changes: Vec<super::extension::Change>,
    topics: BTreeSet<String>,
    topic_interests: BTreeMap<String, Vec<tok_events::interest::Lease>>,
    pending_topics: BTreeMap<String, (u64, CancellationToken)>,
    topic_revision: u64,
    handoff: bool,
    logged_in: bool,
    closed: bool,
    generation: u64,
    connection: Option<AttachedConnection>,
    grace_deadline: Option<Instant>,
    last_request_id: u64,
    replay_cache: ReplayCache,
}

pub(super) struct TopicJoin {
    pub(super) version: u64,
    generation: u64,
    pub(super) cancel: CancellationToken,
    pub(super) binding: CancellationToken,
}

// 管理逻辑 session 与可替换连接之间的通信状态，不执行业务或调度业务 timer。
pub(super) struct Channel {
    max_frame_size: usize,
    max_topics: usize,
    topic_operations: tokio::sync::Semaphore,
    disconnect_grace: Duration,
    session_cancel: CancellationToken,
    binding_changed: Notify,
    state: Mutex<ChannelState>,
    topic_membership: OnceLock<TopicMembership>,
    session: OnceLock<std::sync::Weak<SessionState>>,
}

// 后声明的状态锁先释放，再由此守卫取出变化并通知；不在锁内执行游戏回调。
struct Observations<'a>(&'a Channel);
impl Drop for Observations<'_> {
    fn drop(&mut self) {
        let changes = match self.0.state.lock() {
            Ok(mut state) => std::mem::take(&mut state.changes),
            Err(_) => return,
        };
        if let Some(state) = self.0.session.get().and_then(std::sync::Weak::upgrade) {
            let handle = super::SessionHandle(state);
            for change in changes {
                if change == super::extension::Change::Closing {
                    handle.closing();
                } else {
                    handle.changed(change);
                }
            }
        }
    }
}

impl Channel {
    pub(super) fn bind_topic_membership(
        &self,
        index: Arc<Index>,
        session: &Arc<SessionState>,
    ) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        self.session
            .set(Arc::downgrade(session))
            .map_err(|_| ErrorCode::Internal)?;
        let topic_membership = TopicMembership::new(index, session);
        self.topic_membership
            .set(topic_membership)
            .map_err(|_| ErrorCode::Internal)?;
        if let Some(topic_membership) = self.topic_membership.get() {
            for name in &state.topics {
                topic_membership.join(name)?;
            }
        }
        Ok(())
    }

    pub(super) fn status(&self) -> Result<super::Snapshot> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        let phase = if state.closed || self.session_cancel.is_cancelled() {
            super::Phase::Stopping
        } else if state.handoff {
            super::Phase::Handoff
        } else if !state.logged_in {
            super::Phase::Opening
        } else if state
            .connection
            .as_ref()
            .is_some_and(|connection| connection.replaying)
        {
            super::Phase::Resuming
        } else if state
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.binding.cancellation.is_cancelled())
        {
            super::Phase::Online
        } else {
            super::Phase::Detached
        };
        Ok(super::Snapshot {
            phase,
            connection_generation: state.generation,
            connected: state.connection.is_some(),
            topics: state.topics.iter().cloned().collect(),
            last_request_id: state.last_request_id,
            last_server_seq: state.replay_cache.last_published_seq(),
            queue_len: 0,
            queue_capacity: 0,
        })
    }
    pub(super) fn max_event_bytes(&self) -> usize {
        self.max_frame_size.min(1_048_576)
    }
    pub(super) fn check_resume_position(&self, ack: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed || state.handoff {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        if ack > state.replay_cache.last_published_seq() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        state
            .replay_cache
            .replay_after(ack)
            .ok_or(ErrorCode::ResumeUnavailable)?;
        Ok(())
    }
    fn ensure_event_admissible(state: &ChannelState, topic: Option<&str>) -> Result<()> {
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if !state.logged_in
            || state.handoff
            || state.connection.as_ref().is_none_or(|connection| {
                connection.replaying || connection.binding.cancellation.is_cancelled()
            })
            || topic.is_some_and(|topic| !state.topics.contains(topic))
        {
            return Err(ErrorCode::InvalidState.into());
        }
        Ok(())
    }
    pub(super) fn reserve_topic_operation(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        self.topic_operations
            .try_acquire()
            .map_err(|_| ErrorCode::CapacityExceeded.into())
    }
    pub(super) fn begin_topic(&self, name: &str) -> Result<Option<TopicJoin>> {
        tok_events::Topic::named(name).map_err(|_| ErrorCode::InvalidRequest)?;
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if state.handoff {
            return Err(ErrorCode::InvalidState.into());
        }
        if state.topics.contains(name) {
            if state
                .topic_interests
                .get(name)
                .is_some_and(|leases| leases.iter().any(|lease| !lease.is_ready()))
            {
                return Err(ErrorCode::Unavailable.into());
            }
            return Ok(None);
        }
        if state.topics.len() + state.pending_topics.len() >= self.max_topics {
            return Err(ErrorCode::CapacityExceeded.into());
        }
        state.topic_revision = state
            .topic_revision
            .checked_add(1)
            .ok_or(ErrorCode::Internal)?;
        let version = state.topic_revision;
        let cancel = self.session_cancel.child_token();
        state
            .pending_topics
            .insert(name.into(), (version, cancel.clone()));
        Ok(Some(TopicJoin {
            version,
            generation: state.generation,
            cancel,
            binding: state
                .connection
                .as_ref()
                .map(|connection| connection.binding.cancellation.clone())
                .unwrap_or_else(|| self.session_cancel.child_token()),
        }))
    }
    pub(super) fn abort_topic(&self, name: &str, version: u64) {
        if let Ok(mut state) = self.state.lock()
            && state
                .pending_topics
                .get(name)
                .is_some_and(|(current, _)| *current == version)
            && let Some((_, cancel)) = state.pending_topics.remove(name)
        {
            cancel.cancel();
        }
    }
    pub(super) fn activate_topic(
        &self,
        name: &str,
        ticket: &TopicJoin,
        leases: Vec<tok_events::interest::Lease>,
    ) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed || self.session_cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if state.handoff
            || state.generation != ticket.generation
            || ticket.binding.is_cancelled()
            || !state
                .pending_topics
                .get(name)
                .is_some_and(|(version, _)| *version == ticket.version)
        {
            return Err(ErrorCode::InvalidState.into());
        }
        if let Some(membership) = self.topic_membership.get() {
            membership.join(name)?;
        }
        state.pending_topics.remove(name);
        state.topic_interests.insert(name.into(), leases);
        state.topics.insert(name.into());
        Ok(())
    }
    pub(super) fn check_interests(&self) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state
            .topic_interests
            .values()
            .flatten()
            .any(|lease| !lease.is_ready())
        {
            return Err(ErrorCode::Unavailable.into());
        }
        Ok(())
    }
    pub(super) fn install_interests(
        &self,
        interests: BTreeMap<String, Vec<tok_events::interest::Lease>>,
    ) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .topic_interests = interests;
        Ok(())
    }
    pub(super) fn remove_topic(&self, name: &str) -> Result<Vec<tok_events::interest::Lease>> {
        tok_events::Topic::named(name).map_err(|_| ErrorCode::InvalidRequest)?;
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if state.handoff {
            return Err(ErrorCode::InvalidState.into());
        }
        if let Some(membership) = self.topic_membership.get() {
            membership.leave(name)?;
        }
        state.topics.remove(name);
        if let Some((_, cancel)) = state.pending_topics.remove(name) {
            cancel.cancel();
        }
        Ok(state.topic_interests.remove(name).unwrap_or_default())
    }
    pub(super) fn admit_event<T>(
        &self,
        topic: Option<&str>,
        enqueue: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::ensure_event_admissible(&state, topic)?;
        enqueue()
    }
    pub(super) fn check_event(&self, topic: Option<&str>) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::ensure_event_admissible(&state, topic)
    }
    pub(super) fn publish_event(&self, message: LogicalMessage, topic: Option<&str>) -> Result<()> {
        let _observations = Observations(self);
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::ensure_event_admissible(&state, topic)?;
        self.publish_locked(&mut state, None, message, 0)
    }
    pub(super) fn admit_bound<T>(
        &self,
        binding: &Binding,
        enqueue: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.handoff || Self::require_current_binding(&state, binding)?.replaying {
            return Err(ErrorCode::InvalidState.into());
        }
        enqueue()
    }
    pub(super) fn admit_internal<T>(&self, enqueue: impl FnOnce() -> Result<T>) -> Result<T> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if !state.logged_in || state.handoff {
            return Err(ErrorCode::InvalidState.into());
        }
        enqueue()
    }
    pub(super) fn begin_handoff(&self) -> Result<()> {
        let _observations = Observations(self);
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed
            || !state.logged_in
            || state.handoff
            || state
                .grace_deadline
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        state.handoff = true;
        self.detach_locked(&mut state);
        if state.closed {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        Ok(())
    }
    pub(super) fn cancel_handoff(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed
            || state
                .grace_deadline
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        state.handoff = false;
        Ok(())
    }
    pub(super) fn snapshot(
        &self,
        ack: u64,
    ) -> Result<tok_rpc::protocol::internal::v1::ChannelSnapshot> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed || !state.handoff {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        if state.replay_cache.replay_after(ack).is_none() {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let mut snapshot = state.replay_cache.snapshot();
        snapshot.topics = state.topics.iter().cloned().collect();
        snapshot.last_request_id = state.last_request_id;
        snapshot.grace_remaining_ms = state
            .grace_deadline
            .map(|deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64
            })
            .unwrap_or(0);
        if snapshot.grace_remaining_ms == 0 {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        Ok(snapshot)
    }
    pub(super) fn restore(
        snapshot: &tok_rpc::protocol::internal::v1::ChannelSnapshot,
        elapsed: Duration,
        config: &Config,
        cancel: CancellationToken,
        sender: OutboundSender,
    ) -> Result<Self> {
        let cache = ReplayCache::restore(config, snapshot, elapsed)?;
        if snapshot.topics.len() > config.max_topics
            || snapshot
                .topics
                .iter()
                .any(|name| tok_events::Topic::named(name).is_err())
            || snapshot.topics.iter().collect::<BTreeSet<_>>().len() != snapshot.topics.len()
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        for entry in &snapshot.entries {
            let frame =
                Frame::decode(entry.body.as_slice()).map_err(|_| ErrorCode::ResumeUnavailable)?;
            let seq = match frame.content {
                Some(Content::Response(response)) => response.server_seq,
                Some(Content::Notify(notify)) => notify.server_seq,
                _ => return Err(ErrorCode::ResumeUnavailable.into()),
            };
            if frame.version != VERSION || seq != entry.seq {
                return Err(ErrorCode::ResumeUnavailable.into());
            }
        }
        let grace = Duration::from_millis(snapshot.grace_remaining_ms).saturating_sub(elapsed);
        if grace.is_zero() {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        let channel = Self::new(sender, config, cancel);
        {
            let mut state = channel.state.lock().map_err(|_| ErrorCode::Internal)?;
            state.logged_in = true;
            state.connection = None;
            state.last_request_id = snapshot.last_request_id;
            state.replay_cache = cache;
            state.topics = snapshot.topics.iter().cloned().collect();
            state.grace_deadline = Some(Instant::now() + grace);
        }
        Ok(channel)
    }
    pub(super) fn new(
        outbound_sender: OutboundSender,
        config: &Config,
        session_cancel: CancellationToken,
    ) -> Self {
        let binding = Binding {
            generation: 1,
            cancellation: session_cancel.child_token(),
        };
        Self {
            max_frame_size: config.max_frame_size,
            max_topics: config.max_topics,
            topic_operations: tokio::sync::Semaphore::new(config.max_topics),
            disconnect_grace: Duration::from_millis(config.disconnect_grace_ms),
            session_cancel,
            binding_changed: Notify::new(),
            topic_membership: OnceLock::new(),
            session: OnceLock::new(),
            state: Mutex::new(ChannelState {
                changes: Vec::new(),
                topics: BTreeSet::new(),
                topic_interests: BTreeMap::new(),
                pending_topics: BTreeMap::new(),
                topic_revision: 0,
                handoff: false,
                logged_in: false,
                closed: false,
                generation: 1,
                connection: Some(AttachedConnection {
                    binding,
                    outbound_sender,
                    replaying: false,
                }),
                grace_deadline: None,
                last_request_id: 0,
                replay_cache: ReplayCache::new(config),
            }),
        }
    }

    fn require_current_binding<'a>(
        state: &'a ChannelState,
        binding: &Binding,
    ) -> Result<&'a AttachedConnection> {
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        let connection = state
            .connection
            .as_ref()
            .filter(|connection| connection.binding.generation == binding.generation)
            .ok_or(ErrorCode::SessionReplaced)?;
        if connection.binding.cancellation.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        Ok(connection)
    }

    pub(super) fn complete_login(&self, login: Frame) -> Result<Binding> {
        let _observations = Observations(self);
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed || state.logged_in {
            return Err(ErrorCode::SessionClosed.into());
        }
        if login.encoded_len() > self.max_frame_size {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        let connection = state.connection.as_ref().ok_or(ErrorCode::SessionClosed)?;
        connection
            .outbound_sender
            .try_send(Outbound::Frame(login, None))
            .map_err(|_| ErrorCode::SessionClosed)?;
        let binding = connection.binding.clone();
        // 完成的是服务端登录回应入队；不表示 socket 已写完或客户端已确认。
        state.logged_in = true;
        state.changes.push(super::extension::Change::Bound {
            generation: binding.generation(),
        });
        Ok(binding)
    }

    // 校验绑定并在同一锁内生成、入队控制回应，旧连接或交接/补发期间不能签发凭证。
    pub(super) fn send_bound_frame(
        &self,
        binding: &Binding,
        build: impl FnOnce() -> Result<Frame>,
    ) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        let connection = Self::require_current_binding(&state, binding)?;
        if !state.logged_in || state.handoff || connection.replaying {
            return Err(ErrorCode::InvalidState.into());
        }
        let frame = build()?;
        if frame.encoded_len() > self.max_frame_size {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        connection
            .outbound_sender
            .try_send(Outbound::Frame(frame, None))
            .map_err(|_| ErrorCode::SessionClosed.into())
    }

    pub(super) fn admit_request<T>(
        &self,
        binding: &Binding,
        request_id: u64,
        enqueue: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.handoff {
            return Err(ErrorCode::InvalidState.into());
        }
        if Self::require_current_binding(&state, binding)?.replaying {
            return Err(ErrorCode::InvalidState.into());
        }
        if request_id == 0 || request_id <= state.last_request_id {
            return Err(ErrorCode::InvalidRequest.into());
        }
        // 包括队列拒绝在内，已收到的请求 ID 均不可在恢复后重新提交。
        state.last_request_id = request_id;
        enqueue()
    }

    pub(super) fn last_request_id(&self) -> Result<u64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .last_request_id)
    }

    #[cfg(test)]
    pub(super) fn publish(&self, message: LogicalMessage, request_message_id: u32) -> Result<()> {
        self.publish_from(None, message, request_message_id)
    }

    // Some(binding) 校验消息来源仍为当前连接；None 用于 session 内部完成的回应或通知。
    pub(super) fn publish_from(
        &self,
        binding: Option<&Binding>,
        message: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        let _observations = Observations(self);
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        self.publish_locked(&mut state, binding, message, request_message_id)
    }
    pub(super) fn check_publication(
        &self,
        binding: Option<&Binding>,
        kind: MessageKind,
    ) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::ensure_publishable(&state, binding, kind)
    }
    fn ensure_publishable(
        state: &ChannelState,
        binding: Option<&Binding>,
        kind: MessageKind,
    ) -> Result<()> {
        if state.handoff && kind == MessageKind::Notify {
            return Err(ErrorCode::InvalidState.into());
        }
        if let Some(binding) = binding {
            Self::require_current_binding(state, binding)?;
        }
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if !state.logged_in {
            return Err(ErrorCode::InvalidState.into());
        }
        Ok(())
    }
    fn publish_locked(
        &self,
        state: &mut ChannelState,
        binding: Option<&Binding>,
        mut message: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        Self::ensure_publishable(state, binding, message.kind)?;
        if binding.is_some() {
            state.last_request_id = state.last_request_id.max(message.request_id);
        }
        let seq = state.replay_cache.next_seq()?;
        message.server_seq = Some(seq);
        let mut frame = message_frame(message)?;
        if frame.encoded_len() > self.max_frame_size {
            let Some(Content::Response(response)) = &mut frame.content else {
                return Err(ErrorCode::FrameTooLarge.into());
            };
            response.message_id = request_message_id;
            response.error_code = ErrorCode::FrameTooLarge as u32;
            response.body.clear();
            if frame.encoded_len() > self.max_frame_size {
                return Err(ErrorCode::SessionClosed.into());
            }
        }
        let body: Arc<[u8]> = frame.encode_to_vec().into();
        state.replay_cache.push(seq, Arc::clone(&body));
        if let Some(connection) = &state.connection
            && !connection.replaying
            && connection
                .outbound_sender
                .try_send(Outbound::Encoded(body))
                .is_err()
        {
            self.detach_locked(state);
            if state.closed {
                return Err(ErrorCode::SessionClosed.into());
            }
        }
        Ok(())
    }

    pub(super) fn acknowledge(&self, binding: &Binding, seq: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::require_current_binding(&state, binding)?;
        state.replay_cache.acknowledge(seq)
    }

    pub(super) fn replay_after(&self, seq: u64) -> Result<Option<ReplayBatch>> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        Ok(state.replay_cache.replay_after(seq))
    }

    pub(super) fn rebind_connection(
        self: &Arc<Self>,
        outbound_sender: OutboundSender,
        ack: u64,
        mut response: protocol::ResumeResponse,
    ) -> Result<Binding> {
        let _observations = Observations(self);
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed
            || state.handoff
            || !state.logged_in
            || state
                .grace_deadline
                .is_some_and(|until| until <= Instant::now())
        {
            return Err(ErrorCode::ResumeUnavailable.into());
        }
        if ack > state.replay_cache.last_published_seq() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let batch = state
            .replay_cache
            .replay_after(ack)
            .ok_or(ErrorCode::ResumeUnavailable)?;
        response.last_request_id = state.last_request_id;
        if wrap_frame(Content::Resumed(response.clone())).encoded_len() > self.max_frame_size {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(ErrorCode::SessionClosed)?;
        state.replay_cache.acknowledge(ack)?;
        if let Some(old) = state.connection.take() {
            state.changes.push(super::extension::Change::Detached {
                generation: old.binding.generation(),
            });
            old.binding.cancellation.cancel();
        }
        let binding = Binding {
            generation,
            cancellation: self.session_cancel.child_token(),
        };
        state.generation = generation;
        state.grace_deadline = None;
        state.changes.push(super::extension::Change::Resuming {
            generation: binding.generation(),
        });
        state.connection = Some(AttachedConnection {
            binding: binding.clone(),
            outbound_sender: outbound_sender.clone(),
            replaying: true,
        });
        if outbound_sender
            .try_send(Outbound::Replay(ReplayPlan {
                channel: Arc::clone(self),
                binding: binding.clone(),
                response,
                batch,
            }))
            .is_err()
        {
            self.detach_locked(&mut state);
            return Err(ErrorCode::SessionClosed.into());
        }
        self.binding_changed.notify_one();
        Ok(binding)
    }

    fn next_replay_batch(&self, binding: &Binding, cursor: u64) -> Result<ReplayBatch> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        Self::require_current_binding(&state, binding)?;
        let batch = state
            .replay_cache
            .replay_after(cursor)
            .ok_or(ErrorCode::ResumeUnavailable)?;
        if batch.frames.is_empty() {
            // writer 仍在执行 ReplayPlan，后续普通入队不能越过紧接着写出的完成标记。
            state
                .connection
                .as_mut()
                .ok_or(ErrorCode::SessionClosed)?
                .replaying = false;
        }
        Ok(batch)
    }

    fn detach_locked(&self, state: &mut ChannelState) {
        if let Some(connection) = state.connection.take() {
            state.changes.push(super::extension::Change::Detached {
                generation: connection.binding.generation(),
            });
            connection.binding.cancellation.cancel();
            state.grace_deadline = Some(Instant::now() + self.disconnect_grace);
            if self.disconnect_grace.is_zero() {
                self.close_locked(state);
            }
            self.binding_changed.notify_one();
        }
    }

    pub(super) fn detach(&self, binding: &Binding) -> bool {
        let _observations = Observations(self);
        if let Ok(mut state) = self.state.lock()
            && Self::require_current_binding(&state, binding).is_ok()
        {
            self.detach_locked(&mut state);
            return true;
        }
        false
    }

    // 协议错误终止整个逻辑会话；旧连接不能通过此入口关闭已重绑定的会话。
    pub(super) fn close_bound_session(&self, binding: &Binding) {
        let _observations = Observations(self);
        if let Ok(mut state) = self.state.lock()
            && Self::require_current_binding(&state, binding).is_ok()
        {
            self.close_locked(&mut state);
        }
    }

    fn close_locked(&self, state: &mut ChannelState) {
        if !state.closed {
            state.changes.push(super::extension::Change::Closing);
        }
        state.closed = true;
        if let Some(topic_membership) = self.topic_membership.get() {
            topic_membership.clear(&state.topics);
        }
        state.topics.clear();
        state.topic_interests.clear();
        for (_, (_, cancel)) in std::mem::take(&mut state.pending_topics) {
            cancel.cancel();
        }
        if let Some(connection) = state.connection.take() {
            state.changes.push(super::extension::Change::Detached {
                generation: connection.binding.generation(),
            });
            connection.binding.cancellation.cancel();
        }
        state.replay_cache.clear();
        self.session_cancel.cancel();
        self.binding_changed.notify_one();
    }

    pub(super) fn close(&self) {
        let _observations = Observations(self);
        if let Ok(mut state) = self.state.lock() {
            self.close_locked(&mut state);
        }
    }

    pub(super) async fn run_grace_timer(&self) {
        let _observations = Observations(self);
        loop {
            let binding_changed = self.binding_changed.notified();
            let deadline = match self.state.lock() {
                Ok(state) => state.grace_deadline,
                Err(_) => {
                    self.session_cancel.cancel();
                    return;
                }
            };
            tokio::select! {
                biased;
                _ = self.session_cancel.cancelled() => return,
                _ = binding_changed => {},
                _ = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline).await; } else { std::future::pending::<()>().await; } } => {
                    if let Ok(mut state) = self.state.lock()
                        && state.grace_deadline.is_some_and(|until| until <= Instant::now())
                    { self.close_locked(&mut state); return; }
                }
            }
        }
    }
}

// 写循环执行的恢复回应、连续补发和完成标记计划，不是整个 session 恢复状态机。
pub(crate) struct ReplayPlan {
    channel: Arc<Channel>,
    binding: Binding,
    response: protocol::ResumeResponse,
    batch: ReplayBatch,
}
impl ReplayPlan {
    pub(crate) async fn write_to<W: AsyncWrite + Unpin>(
        self,
        codec: &FrameCodec,
        writer: &mut W,
    ) -> Result<()> {
        codec
            .write(writer, &wrap_frame(Content::Resumed(self.response)))
            .await?;
        let mut batch = self.batch;
        loop {
            for body in &batch.frames {
                if self.binding.cancellation.is_cancelled() {
                    return Err(ErrorCode::SessionReplaced.into());
                }
                codec.write_body(writer, body).await?;
            }
            let cursor = batch.through_seq;
            batch = self.channel.next_replay_batch(&self.binding, cursor)?;
            if batch.frames.is_empty() {
                let result = codec
                    .write(
                        writer,
                        &wrap_frame(Content::Recovered(protocol::RecoveryComplete {
                            through_seq: cursor,
                        })),
                    )
                    .await;
                if result.is_ok()
                    && !self.binding.cancellation.is_cancelled()
                    && let Some(state) = self
                        .channel
                        .session
                        .get()
                        .and_then(std::sync::Weak::upgrade)
                {
                    super::SessionHandle(state).changed(super::extension::Change::Resumed {
                        generation: self.binding.generation(),
                    });
                }
                return result;
            }
        }
    }
}

// 为任意消息内容添加协议版本和 Frame 外层，控制消息与业务消息共用。
fn wrap_frame(content: Content) -> Frame {
    Frame {
        version: VERSION,
        content: Some(content),
    }
}

fn message_frame(message: LogicalMessage) -> Result<Frame> {
    let server_seq = message.server_seq.ok_or(ErrorCode::InvalidState)?;
    let content = match message.kind {
        MessageKind::Response => Content::Response(protocol::BusinessResponse {
            request_id: message.request_id,
            message_id: message.message_id,
            error_code: message.error_code,
            body: message.body,
            server_seq,
        }),
        MessageKind::Notify => Content::Notify(protocol::Notify {
            message_id: message.message_id,
            body: message.body,
            server_seq,
        }),
        MessageKind::Request => return Err(ErrorCode::InvalidState.into()),
    };
    Ok(wrap_frame(content))
}

pub(crate) fn error_response(request: &LogicalMessage, code: ErrorCode) -> LogicalMessage {
    LogicalMessage {
        kind: MessageKind::Response,
        message_id: request.message_id,
        request_id: request.request_id,
        server_seq: None,
        error_code: code as u32,
        body: Vec::new(),
    }
}

#[cfg(test)]
mod tests;

impl Drop for Channel {
    fn drop(&mut self) {
        // 即使状态锁已中毒，析构仍可独占读取成员名，避免索引留下失效弱引用。
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(topic_membership) = self.topic_membership.get() {
            topic_membership.clear(&state.topics);
        }
    }
}
