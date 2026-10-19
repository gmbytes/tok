//! session 内的 player 执行契约与无自动重试的 RPC 适配。

use crate::{ErrorCode, Result};
use std::{future::Future, pin::Pin, sync::Arc};
use tok_core::{
    CallContext, LogicalMessage, MessageKind, ServiceRouter, SessionIdentity, TimerSpec,
    TimerTrigger, TimerUpdate,
};
use tok_rpc::{
    RpcClients,
    protocol::internal::v1::Payload,
    tonic::{Code, Status},
};

/// 单次玩家调用的结果，受 session 的调用期限控制。
pub type PlayerTask<'a> = Pin<Box<dyn Future<Output = Result<PlayerReply>> + Send + 'a>>;

/// 玩家执行结果，计时更新留在 session 内部，不进入客户端协议正文。
pub struct PlayerReply {
    /// 客户端请求必须有回应；内部 timer 可以没有回应。
    pub response: Option<LogicalMessage>,
    /// None 保持当前计时集合。
    pub timers: Option<TimerUpdate>,
}
impl From<LogicalMessage> for PlayerReply {
    fn from(response: LogicalMessage) -> Self {
        Self {
            response: Some(response),
            timers: None,
        }
    }
}

/// 所有玩家执行来源应进入 session 队列后使用的执行扩展点。
pub trait PlayerExecutor: Send + Sync {
    /// 使用 session 身份执行请求，不能从消息正文选择其他玩家。
    fn execute(&self, identity: SessionIdentity, request: LogicalMessage) -> PlayerTask<'_>;
    /// 执行内部 timer；序号由 session 独立分配，不占用客户端请求序列。默认拒绝未支持的计时调用。
    fn timer(
        &self,
        _identity: SessionIdentity,
        _request_id: u64,
        _spec: TimerSpec,
        _trigger: TimerTrigger,
    ) -> PlayerTask<'_> {
        Box::pin(async { Err(ErrorCode::UnknownMessage.into()) })
    }
}

/// 用户可替换的 RPC 系统错误到客户端错误码映射。
pub trait RpcErrorMapper: Send + Sync {
    /// 返回有限客户端错误码，不将原始 Status 文本发送给客户端。
    fn map(&self, status: &Status) -> ErrorCode;
}

/// 默认的 RPC 错误分类，超时、不可用和未知消息分别管理。
pub struct DefaultRpcErrorMapper;
impl RpcErrorMapper for DefaultRpcErrorMapper {
    fn map(&self, status: &Status) -> ErrorCode {
        match status.code() {
            Code::InvalidArgument => ErrorCode::InvalidRequest,
            Code::Unauthenticated | Code::PermissionDenied => ErrorCode::Unauthenticated,
            Code::ResourceExhausted => ErrorCode::QueueFull,
            Code::FailedPrecondition => ErrorCode::SessionClosed,
            Code::Unavailable => ErrorCode::Unavailable,
            Code::DeadlineExceeded | Code::Cancelled => ErrorCode::Timeout,
            Code::Unimplemented => ErrorCode::UnknownMessage,
            _ => ErrorCode::Internal,
        }
    }
}

/// 以可信玩家 ID 路由到 player 的内部 RPC 执行器。
pub struct RpcPlayerExecutor {
    service_name: String,
    router: Arc<dyn ServiceRouter>,
    clients: Arc<RpcClients>,
    mapper: Arc<dyn RpcErrorMapper>,
}

impl RpcPlayerExecutor {
    /// 创建 player RPC 适配，router 与连接缓存由组装层注入。
    pub fn new(
        service_name: impl Into<String>,
        router: Arc<dyn ServiceRouter>,
        clients: Arc<RpcClients>,
    ) -> Result<Self> {
        let service_name = service_name.into();
        if service_name.trim().is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(Self {
            service_name,
            router,
            clients,
            mapper: Arc::new(DefaultRpcErrorMapper),
        })
    }
    /// 替换系统错误映射，不改变无自动重试语义。
    pub fn with_error_mapper(mut self, mapper: Arc<dyn RpcErrorMapper>) -> Self {
        self.mapper = mapper;
        self
    }
}

impl PlayerExecutor for RpcPlayerExecutor {
    fn execute(&self, identity: SessionIdentity, request: LogicalMessage) -> PlayerTask<'_> {
        self.call(identity, request, None)
    }
    fn timer(
        &self,
        identity: SessionIdentity,
        request_id: u64,
        spec: TimerSpec,
        trigger: TimerTrigger,
    ) -> PlayerTask<'_> {
        self.call(
            identity,
            LogicalMessage {
                kind: MessageKind::Request,
                message_id: spec.message_id,
                request_id,
                server_seq: None,
                error_code: 0,
                body: spec.body,
            },
            Some(trigger),
        )
    }
}
impl RpcPlayerExecutor {
    fn call(
        &self,
        identity: SessionIdentity,
        request: LogicalMessage,
        timer: Option<TimerTrigger>,
    ) -> PlayerTask<'_> {
        Box::pin(async move {
            let is_timer = timer.is_some();
            let target = self
                .router
                .route(&self.service_name, &identity.player_id)
                .map_err(|_| ErrorCode::Unavailable)?;
            let response = self
                .clients
                .player_call(
                    &target,
                    CallContext {
                        node_id: target.node_id.clone(),
                        service_name: target.service_name.clone(),
                        bid: Some(identity.player_id.clone()),
                        request_id: request.request_id,
                        session: Some(identity),
                        timer,
                    },
                    Payload {
                        message_id: request.message_id,
                        body: request.body,
                    },
                )
                .await
                .map_err(|status| self.mapper.map(&status))?;
            let timers = response
                .timers
                .map(tok_rpc::timer::decode_update)
                .transpose()
                .map_err(|_| ErrorCode::Internal)?;
            if let Some(error) = response.error {
                if error.code == 0 {
                    return Err(ErrorCode::Internal.into());
                }
                return Ok(PlayerReply {
                    response: Some(LogicalMessage {
                        kind: MessageKind::Response,
                        request_id: response.request_id,
                        message_id: request.message_id,
                        error_code: error.code,
                        body: Vec::new(),
                        server_seq: None,
                    }),
                    timers,
                });
            }
            if is_timer {
                return Ok(PlayerReply {
                    response: None,
                    timers,
                });
            }
            let payload = response.payload.ok_or(ErrorCode::Internal)?;
            Ok(PlayerReply {
                response: Some(LogicalMessage {
                    kind: MessageKind::Response,
                    request_id: response.request_id,
                    message_id: payload.message_id,
                    body: payload.body,
                    error_code: 0,
                    server_seq: None,
                }),
                timers,
            })
        })
    }
}
