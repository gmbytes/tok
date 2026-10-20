//! 会话通信通道：连接绑定、请求准入、下行发布、确认缓存及断线恢复。

use super::{
    PlayerConfig,
    replay::{ReplayBatch, ReplayCache},
};
use crate::{
    ErrorCode, Result,
    codec::{FrameCodec, VERSION},
    protocol::{self, Frame, frame::Content},
};
use prost::Message;
use std::{
    sync::{Arc, Mutex},
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
pub(super) struct ConnectionBinding {
    generation: u64,
    pub(super) cancellation: CancellationToken,
}

struct AttachedConnection {
    binding: ConnectionBinding,
    outbound_sender: OutboundSender,
    replaying: bool,
}

struct State {
    logged_in: bool,
    closed: bool,
    generation: u64,
    connection: Option<AttachedConnection>,
    grace_deadline: Option<Instant>,
    last_request_id: u64,
    replay_cache: ReplayCache,
}

// 管理逻辑 session 与可替换连接之间的通信状态，不执行业务或调度业务 timer。
pub(super) struct SessionChannel {
    max_frame_size: usize,
    disconnect_grace: Duration,
    session_cancel: CancellationToken,
    binding_changed: Notify,
    state: Mutex<State>,
}

impl SessionChannel {
    pub(super) fn new(
        outbound_sender: OutboundSender,
        config: &PlayerConfig,
        session_cancel: CancellationToken,
    ) -> Self {
        let binding = ConnectionBinding {
            generation: 1,
            cancellation: session_cancel.child_token(),
        };
        Self {
            max_frame_size: config.max_frame_size,
            disconnect_grace: Duration::from_millis(config.disconnect_grace_ms),
            session_cancel,
            binding_changed: Notify::new(),
            state: Mutex::new(State {
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
        state: &'a State,
        binding: &ConnectionBinding,
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

    pub(super) fn complete_login(&self, login: Frame) -> Result<ConnectionBinding> {
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
        Ok(binding)
    }

    pub(super) fn ensure_logged_in(&self) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            Err(ErrorCode::SessionClosed.into())
        } else if !state.logged_in {
            Err(ErrorCode::InvalidState.into())
        } else {
            Ok(())
        }
    }

    pub(super) fn admit_request<T>(
        &self,
        binding: &ConnectionBinding,
        request_id: u64,
        enqueue: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
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

    pub(super) fn publish(&self, message: LogicalMessage, request_message_id: u32) -> Result<()> {
        self.publish_from(None, message, request_message_id)
    }

    // Some(binding) 校验消息来源仍为当前连接；None 用于 session 内部完成的回应或通知。
    pub(super) fn publish_from(
        &self,
        binding: Option<&ConnectionBinding>,
        mut message: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if let Some(binding) = binding {
            Self::require_current_binding(&state, binding)?;
            state.last_request_id = state.last_request_id.max(message.request_id);
        }
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if !state.logged_in {
            return Err(ErrorCode::InvalidState.into());
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
        state.replay_cache.push(seq, body.clone());
        if let Some(connection) = &state.connection
            && !connection.replaying
            && connection
                .outbound_sender
                .try_send(Outbound::Encoded(body))
                .is_err()
        {
            self.detach_locked(&mut state);
            if state.closed {
                return Err(ErrorCode::SessionClosed.into());
            }
        }
        Ok(())
    }

    pub(super) fn acknowledge(&self, binding: &ConnectionBinding, seq: u64) -> Result<()> {
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
    ) -> Result<ConnectionBinding> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed
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
            old.binding.cancellation.cancel();
        }
        let binding = ConnectionBinding {
            generation,
            cancellation: self.session_cancel.child_token(),
        };
        state.generation = generation;
        state.grace_deadline = None;
        state.connection = Some(AttachedConnection {
            binding: binding.clone(),
            outbound_sender: outbound_sender.clone(),
            replaying: true,
        });
        if outbound_sender
            .try_send(Outbound::Replay(ReplayPlan {
                channel: self.clone(),
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

    fn next_replay_batch(&self, binding: &ConnectionBinding, cursor: u64) -> Result<ReplayBatch> {
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

    fn detach_locked(&self, state: &mut State) {
        if let Some(connection) = state.connection.take() {
            connection.binding.cancellation.cancel();
            state.grace_deadline = Some(Instant::now() + self.disconnect_grace);
            if self.disconnect_grace.is_zero() {
                self.close_locked(state);
            }
            self.binding_changed.notify_one();
        }
    }

    pub(super) fn detach(&self, binding: &ConnectionBinding) {
        if let Ok(mut state) = self.state.lock()
            && Self::require_current_binding(&state, binding).is_ok()
        {
            self.detach_locked(&mut state);
        }
    }

    // 协议错误终止整个逻辑会话；旧连接不能通过此入口关闭已重绑定的会话。
    pub(super) fn close_bound_session(&self, binding: &ConnectionBinding) {
        if let Ok(mut state) = self.state.lock()
            && Self::require_current_binding(&state, binding).is_ok()
        {
            self.close_locked(&mut state);
        }
    }

    fn close_locked(&self, state: &mut State) {
        state.closed = true;
        if let Some(connection) = state.connection.take() {
            connection.binding.cancellation.cancel();
        }
        state.replay_cache.clear();
        self.session_cancel.cancel();
        self.binding_changed.notify_one();
    }

    pub(super) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            self.close_locked(&mut state);
        }
    }

    pub(super) async fn run_grace_timer(&self) {
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
    channel: Arc<SessionChannel>,
    binding: ConnectionBinding,
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
                return codec
                    .write(
                        writer,
                        &wrap_frame(Content::Recovered(protocol::RecoveryComplete {
                            through_seq: cursor,
                        })),
                    )
                    .await;
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
