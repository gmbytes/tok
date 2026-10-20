//! 会话提交、通知和关闭句柄，以及绑定特定连接的操作视图。
use super::{
    channel,
    replay::ReplayBatch,
    state::SessionState,
    worker::{Job, RequestJob},
};
use crate::{ErrorCode, Result};
use std::{future::Future, pin::Pin, sync::Arc};
use tok_core::{LogicalMessage, MessageKind, SessionIdentity};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// 单次排队请求的回应，不创建额外业务任务；队列关闭时返回失败回应。
pub type PendingResponse = Pin<Box<dyn Future<Output = LogicalMessage> + Send>>;

/// 一个玩家当前逻辑会话的有界提交入口，可用于后续转发事件。
#[derive(Clone)]
pub struct SessionHandle(pub(super) Arc<SessionState>);

#[derive(Clone)]
// 绑定特定连接代次的 session 操作视图；异于不绑定连接的 SessionHandle。
pub(in crate::player) struct BoundSession {
    pub(super) session: SessionHandle,
    pub(super) binding: channel::Binding,
}

impl BoundSession {
    pub(in crate::player) fn cancellation(&self) -> CancellationToken {
        self.binding.cancellation.clone()
    }
    pub(in crate::player) fn detach(&self) {
        self.session.0.channel.detach(&self.binding);
    }
    pub(in crate::player) fn close_session(&self) {
        self.session.0.channel.close_bound_session(&self.binding);
    }
    pub(in crate::player) fn last_request_id(&self) -> Result<u64> {
        self.session.0.channel.last_request_id()
    }
    pub(in crate::player) fn submit(&self, request: LogicalMessage) -> Result<()> {
        self.session
            .0
            .channel
            .admit_request(&self.binding, request.request_id, || {
                self.session.enqueue_request(request, None)
            })
    }
    pub(in crate::player) fn acknowledge(&self, seq: u64) -> Result<()> {
        self.session.0.channel.acknowledge(&self.binding, seq)
    }
    pub(in crate::player) fn publish_response(
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
    /// 加入可信服务器指定的事件主题；同一 session 恢复和交接保留，数量有界。
    pub fn join_topic(&self, topic: &str) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.change_topic(topic, true)
    }
    /// 退出事件主题；不存在时成功，冻结或已关闭的会话拒绝修改。
    pub fn leave_topic(&self, topic: &str) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.change_topic(topic, false)
    }
    /// 将内部事件排入原 session 队列；只接收在线、非恢复和非冻结会话，不产生客户端回应。
    pub fn post_event(&self, event: Arc<tok_events::Message>) -> Result<()> {
        self.enqueue_event(event, None)
    }
    pub(super) fn enqueue_event(
        &self,
        event: Arc<tok_events::Message>,
        topic: Option<&str>,
    ) -> Result<()> {
        event
            .validate(self.0.channel.max_event_bytes())
            .map_err(|_| ErrorCode::InvalidRequest)?;
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.admit_event(topic, || {
            self.0
                .job_sender
                .try_send(Job::Event(super::worker::EventJob {
                    event,
                    parent: tracing::Span::current(),
                }))
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => ErrorCode::QueueFull.into(),
                    mpsc::error::TrySendError::Closed(_) => ErrorCode::SessionClosed.into(),
                })
        })
    }
    pub(super) fn notify_event(
        &self,
        message_id: u32,
        body: &[u8],
        topic: Option<&str>,
    ) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.notify_event(message_id, body, topic)
    }

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

    pub(in crate::player) fn complete_login(
        &self,
        login: crate::protocol::Frame,
    ) -> Result<BoundSession> {
        let binding = self.0.channel.complete_login(login)?;
        Ok(BoundSession {
            session: self.clone(),
            binding,
        })
    }

    pub(in crate::player) fn resume_token(&self) -> &str {
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
        self.0
            .channel
            .admit_internal(|| self.enqueue_request(request, response_sender))
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
