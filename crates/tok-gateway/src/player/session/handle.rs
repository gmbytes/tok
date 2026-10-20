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
    pub(in crate::player) async fn submit(&self, request: LogicalMessage) -> Result<()> {
        self.session
            .0
            .channel
            .admit_request(&self.binding, request.request_id, || Ok(()))?;
        let original = (request.kind, request.request_id, request.message_id);
        let context = self.session.context_with_cancellation(
            format!("player/admission/{}", request.message_id),
            tok_core::execution::Source::Client {
                request_id: request.request_id,
                message_id: request.message_id,
            },
            self.binding.cancellation.child_token(),
        )?;
        let pipeline = Arc::clone(&self.session.0.pipeline);
        use tower::{Layer as _, ServiceExt};
        let request = tok_service::Layer
            .layer(tower::service_fn(move |request| {
                let pipeline = Arc::clone(&pipeline);
                async move { pipeline.admit(request).await }
            }))
            .oneshot(tok_service::Request::new(context, request))
            .await
            .map_err(super::worker::execution_error)?;
        if (request.kind, request.request_id, request.message_id) != original
            || request.server_seq.is_some()
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        self.session.0.channel.admit_bound(&self.binding, || {
            self.session.enqueue_request(request, None)
        })
    }
    pub(in crate::player) fn acknowledge(&self, seq: u64) -> Result<()> {
        self.session.0.channel.acknowledge(&self.binding, seq)
    }
    pub(in crate::player) async fn publish_response(
        &self,
        response: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        let result = self
            .session
            .process_downlink(
                response,
                request_message_id,
                Some(self.binding.clone()),
                None,
            )
            .await;
        if let Err(error) = &result
            && matches!(error.code, ErrorCode::SessionClosed | ErrorCode::Internal)
        {
            self.close_session();
        }
        result
    }
}

#[derive(Clone)]
struct EventTarget {
    topic: Option<String>,
}

struct PendingTopic {
    channel: Arc<channel::Channel>,
    name: String,
    version: u64,
}
impl Drop for PendingTopic {
    fn drop(&mut self) {
        self.channel.abort_topic(&self.name, self.version);
    }
}
impl SessionHandle {
    /// 等待所有绑定组件的主题兴趣就绪后加入成员，最终重验当前连接代次。
    /// 宽限期保留成员与兴趣；客户端没有直接调用入口。
    pub async fn join_topic(&self, topic: &str) -> Result<()> {
        let _operation = self.0.channel.reserve_topic_operation()?;
        let work = async {
            let _gate = self.0.topic_gate.lock().await;
            let Some(ticket) = self.0.channel.begin_topic(topic)? else {
                return Ok(());
            };
            let _pending = PendingTopic {
                channel: Arc::clone(&self.0.channel),
                name: topic.into(),
                version: ticket.version,
            };
            let bindings = self
                .0
                .manager
                .upgrade()
                .and_then(|manager| manager.topic_interests.get().cloned());
            let leases = tokio::select! {
                biased;
                () = ticket.cancel.cancelled() => return Err(ErrorCode::InvalidState.into()),
                () = ticket.binding.cancelled() => return Err(ErrorCode::SessionReplaced.into()),
                result = async { match bindings { Some(bindings) => bindings.acquire(topic).await, None => Ok(Vec::new()) } } => result?,
            };
            self.0.channel.activate_topic(topic, &ticket, leases)
        };
        tokio::select! {
            biased;
            () = self.0.cancel.cancelled() => Err(ErrorCode::SessionClosed.into()),
            result = tokio::time::timeout(self.0.execution_budget, work) => result.map_err(|_| ErrorCode::Timeout)?,
        }
    }
    /// 先移除本地资格，再释放自身兴趣引用；最后一个引用等待有限退订。
    pub async fn leave_topic(&self, topic: &str) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        let leases = self.0.channel.remove_topic(topic)?;
        tokio::time::timeout(self.0.execution_budget, async {
            for lease in leases {
                lease.release().await.map_err(|_| ErrorCode::Unavailable)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| ErrorCode::Timeout)?
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
    pub(super) async fn notify_event(
        &self,
        message_id: u32,
        body: &[u8],
        topic: Option<&str>,
    ) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.0.channel.check_event(topic)?;
        self.process_downlink(
            LogicalMessage {
                kind: MessageKind::Notify,
                message_id,
                request_id: 0,
                server_seq: None,
                error_code: 0,
                body: body.to_vec(),
            },
            0,
            None,
            Some(EventTarget {
                topic: topic.map(str::to_owned),
            }),
        )
        .await
    }

    pub(super) fn closing(&self) {
        if !self
            .0
            .closing_notified
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.changed(super::extension::Change::Closing);
        }
    }
    pub(super) fn changed(&self, change: super::extension::Change) {
        let Ok(context) = self.context(
            "player/session/change".into(),
            tok_core::execution::Source::Internal,
        ) else {
            return;
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.0
                .extension
                .on_change(tok_service::Request::new(context, change))
        }))
        .is_err()
        {
            tracing::error!("会话生命周期观察者异常，关闭会话");
            self.0.cancel.cancel();
            self.0.channel.close();
        }
    }
    /// 当前会话工厂安装的本地能力；不会隐式写入远端上下文。
    pub fn extensions(&self) -> &tok_core::execution::Extensions {
        &self.0.extensions
    }
    pub(super) fn context(
        &self,
        operation: String,
        source: tok_core::execution::Source,
    ) -> Result<tok_core::execution::Context> {
        self.context_with_cancellation(operation, source, self.0.cancel.child_token())
    }
    // 停止新业务后仍按原预算收束在途执行；会话取消信号通过 SessionHandle 单独提供。
    pub(super) fn execution_context(
        &self,
        operation: String,
        source: tok_core::execution::Source,
    ) -> Result<tok_core::execution::Context> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        self.context_with_cancellation(operation, source, CancellationToken::new())
    }
    fn context_with_cancellation(
        &self,
        operation: String,
        source: tok_core::execution::Source,
        cancellation: CancellationToken,
    ) -> Result<tok_core::execution::Context> {
        let owner = &self.0.ownership.owner;
        let mut context = tok_core::execution::Context::new(
            tok_core::service::Identity {
                node_id: owner.node_id.clone(),
                service_name: owner.service_name.clone(),
            },
            operation,
            source,
            self.0.execution_budget,
            cancellation,
        )
        .map_err(|_| ErrorCode::Internal)?;
        let manager = self
            .0
            .manager
            .upgrade()
            .map(|inner| super::manager::SessionManager { inner });
        if let Some(manager) = &manager {
            context.extensions_mut().extend(&manager.extensions());
        }
        context.extensions_mut().extend(&self.0.extensions);
        if let Some(manager) = manager {
            context.extensions_mut().insert(manager.gateway());
            context.extensions_mut().insert(manager);
        }
        context.extensions_mut().insert(self.clone());
        Ok(context)
    }
    /// 查询会话阶段、连接、主题及 FIFO 容量，不替代后续准入检查。
    pub fn snapshot(&self) -> Result<super::Snapshot> {
        let mut snapshot = self.0.channel.status()?;
        if self
            .0
            .completion
            .done
            .load(std::sync::atomic::Ordering::Acquire)
        {
            snapshot.phase = super::Phase::Stopped;
        }
        snapshot.queue_capacity = self.0.job_sender.max_capacity();
        snapshot.queue_len = snapshot.queue_capacity - self.0.job_sender.capacity();
        Ok(snapshot)
    }
    /// 等待当前会话任务及其 timer/宽限期 future 释放，不得在自身执行中等待。
    pub async fn wait_finished(&self) {
        self.0.completion.wait().await;
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
        self.closing();
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

    pub(super) async fn publish_response(
        &self,
        response: LogicalMessage,
        request_message_id: u32,
    ) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if let Err(error) = self
            .process_downlink(response, request_message_id, None, None)
            .await
        {
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
    async fn process_downlink(
        &self,
        message: LogicalMessage,
        request_message_id: u32,
        binding: Option<channel::Binding>,
        event_target: Option<EventTarget>,
    ) -> Result<()> {
        self.0
            .channel
            .check_publication(binding.as_ref(), message.kind)?;
        let original = (message.kind, message.request_id);
        let cancellation = binding.as_ref().map_or_else(
            || self.0.cancel.child_token(),
            |binding| binding.cancellation.child_token(),
        );
        let context = self.context_with_cancellation(
            format!("player/downlink/{}", message.message_id),
            tok_core::execution::Source::Internal,
            cancellation,
        )?;
        let session = self.clone();
        use tower::{Layer as _, ServiceExt};
        let service = tower::service_fn(move |request: tok_service::Request<LogicalMessage>| {
            let session = session.clone();
            let binding = binding.clone();
            let event_target = event_target.clone();
            async move {
                // 异步串行门锁只保护新下行处理；通道锁不跨 await，ACK 和补发可继续推进。
                let _gate = session.0.downlink_gate.lock().await;
                let message = session.0.pipeline.downlink(request).await?;
                if (message.kind, message.request_id) != original
                    || message.server_seq.is_some()
                    || (message.kind == MessageKind::Notify && message.message_id == 0)
                {
                    return Err(ErrorCode::InvalidRequest.into());
                }
                if let Some(target) = event_target {
                    session
                        .0
                        .channel
                        .publish_event(message, target.topic.as_deref())
                } else if binding.is_none() && original.0 == MessageKind::Notify {
                    session
                        .0
                        .channel
                        .publish_from(None, message, request_message_id)
                } else {
                    session
                        .0
                        .channel
                        .publish_from(binding.as_ref(), message, request_message_id)
                }
            }
        });
        tok_service::Layer
            .layer(service)
            .oneshot(tok_service::Request::new(context, message))
            .await
            .map_err(super::worker::execution_error)
    }
    /// 主动通知进入下行入口，不调用 player；断线或慢连接时在宽限期内继续缓存。
    /// 超大通知返回 FrameTooLarge，不消耗序号；登录回应入队前返回 InvalidState。
    pub async fn notify(&self, message: LogicalMessage) -> Result<()> {
        if self.0.cancel.is_cancelled() {
            return Err(ErrorCode::SessionClosed.into());
        }
        if message.kind != MessageKind::Notify
            || (message.kind == MessageKind::Notify && message.message_id == 0)
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        if let Err(error) = self.process_downlink(message, 0, None, None).await {
            if matches!(error.code, ErrorCode::SessionClosed | ErrorCode::Internal) {
                self.close();
            }
            return Err(error);
        }
        Ok(())
    }
}
