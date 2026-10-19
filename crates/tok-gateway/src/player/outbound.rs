//! 有界下行入口；server_seq、确认与补发缓存按第三批实施。

use crate::{
    ErrorCode, Result,
    codec::VERSION,
    protocol::{self, Frame, frame::Content},
};
use tok_core::{LogicalMessage, MessageKind};
use tokio::sync::{mpsc, oneshot};

/// 按发送前的数据形式区分下行项，不限定消息职责或来源。
pub(crate) enum Outbound {
    /// 已构造的协议帧；可在写出后通知等待方，不代表客户端确认。
    Frame(Frame, Option<oneshot::Sender<()>>),
    /// 由连接写循环转换为协议帧的逻辑回应或通知。
    Logical(LogicalMessage),
}
pub(crate) type Sender = mpsc::Sender<Outbound>;

pub(crate) fn send(sender: &Sender, message: LogicalMessage) -> Result<()> {
    sender
        .try_send(Outbound::Logical(message))
        .map_err(|_| ErrorCode::SessionClosed.into())
}

pub(crate) fn frame(message: LogicalMessage) -> Result<Frame> {
    let content = match message.kind {
        MessageKind::Response => Content::Response(protocol::BusinessResponse {
            request_id: message.request_id,
            message_id: message.message_id,
            error_code: message.error_code,
            body: message.body,
        }),
        MessageKind::Notify => Content::Notify(protocol::Notify {
            message_id: message.message_id,
            body: message.body,
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
