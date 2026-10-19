//! 会话统一下行发布：尺寸归一化、编号、编码、缓存及有界入队。

use super::{
    PlayerConfig,
    replay::{ReplayBatch, ReplayCache},
};
use crate::{
    ErrorCode, Result,
    codec::VERSION,
    protocol::{self, Frame, frame::Content},
};
use prost::Message;
use std::sync::{Arc, Mutex};
use tok_core::{LogicalMessage, MessageKind};
use tokio::sync::{mpsc, oneshot};

/// 按发送前的数据形式区分下行项，不限定消息职责或来源。
pub(crate) enum Outbound {
    /// 连接控制帧，可通知本地写出完成；不表示客户端确认。
    Frame(Frame, Option<oneshot::Sender<()>>),
    /// 已编号并缓存的最终正文；writer 不得修改内容。
    Encoded(Arc<[u8]>),
}
pub(crate) type Sender = mpsc::Sender<Outbound>;

struct State {
    ready: bool,
    closed: bool,
    cache: ReplayCache,
}

pub(super) struct Downlink {
    sender: Sender,
    limit: usize,
    state: Mutex<State>,
}

impl Downlink {
    pub(super) fn new(sender: Sender, config: &PlayerConfig) -> Self {
        Self {
            sender,
            limit: config.max_frame_size,
            state: Mutex::new(State {
                ready: false,
                closed: false,
                cache: ReplayCache::new(config),
            }),
        }
    }

    pub(super) fn start(&self, login: Frame) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed || state.ready {
            return Err(ErrorCode::SessionClosed.into());
        }
        if login.encoded_len() > self.limit {
            return Err(ErrorCode::FrameTooLarge.into());
        }
        self.sender
            .try_send(Outbound::Frame(login, None))
            .map_err(|_| ErrorCode::SessionClosed)?;
        state.ready = true;
        Ok(())
    }

    pub(super) fn check_ready(&self) -> Result<()> {
        let state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            Err(ErrorCode::SessionClosed.into())
        } else if !state.ready {
            Err(ErrorCode::InvalidState.into())
        } else {
            Ok(())
        }
    }

    pub(super) fn publish(
        &self,
        mut message: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        if !state.ready {
            return Err(ErrorCode::InvalidState.into());
        }
        let seq = state.cache.next()?;
        message.server_seq = Some(seq);
        let mut frame = frame(message)?;
        if frame.encoded_len() > self.limit {
            let Some(Content::Response(response)) = &mut frame.content else {
                return Err(ErrorCode::FrameTooLarge.into());
            };
            response.message_id = request_message_id;
            response.error_code = ErrorCode::FrameTooLarge as u32;
            response.body.clear();
            if frame.encoded_len() > self.limit {
                return Err(ErrorCode::SessionClosed.into());
            }
        }
        let body: Arc<[u8]> = frame.encode_to_vec().into();
        state.cache.push(seq, body.clone());
        self.sender
            .try_send(Outbound::Encoded(body))
            .map_err(|_| ErrorCode::SessionClosed.into())
    }

    pub(super) fn acknowledge(&self, seq: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        state.cache.acknowledge(seq)
    }

    pub(super) fn replay_after(&self, seq: u64) -> Result<Option<ReplayBatch>> {
        let mut state = self.state.lock().map_err(|_| ErrorCode::Internal)?;
        if state.closed {
            return Err(ErrorCode::SessionClosed.into());
        }
        Ok(state.cache.after(seq))
    }

    pub(super) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.cache.clear();
        }
    }
}

fn frame(message: LogicalMessage) -> Result<Frame> {
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
    Ok(Frame {
        version: VERSION,
        content: Some(content),
    })
}

pub(crate) fn failed(request: &LogicalMessage, code: ErrorCode) -> LogicalMessage {
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
