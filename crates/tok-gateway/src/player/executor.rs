//! session 内的 player 执行契约与无自动重试的 RPC 适配。

use crate::{ErrorCode, Result};
use std::{future::Future, pin::Pin, sync::Arc};
use tok_core::ServiceRouter;
use tok_game_rpc::CallContext;
use tok_game_rpc::{
    player::Client,
    protocol::internal::v1::Payload,
    tonic::{Code, Status},
};
use tok_player::TimerTrigger;
use tok_player::{LogicalMessage, MessageKind, SessionIdentity, TimerSpec, TimerUpdate};

/// 单次玩家调用的结果，受 session 的调用期限控制。
pub type Task<'a> = Pin<Box<dyn Future<Output = Result<Reply>> + Send + 'a>>;

/// 内部事件的执行效果，只允许更新计时，不包含客户端回应。
pub type EventTask<'a> = Pin<Box<dyn Future<Output = Result<Option<TimerUpdate>>> + Send + 'a>>;

/// 玩家执行结果，计时更新留在 session 内部，不进入客户端协议正文。
pub struct Reply {
    /// 客户端请求必须有回应；内部 timer 可以没有回应。
    pub response: Option<LogicalMessage>,
    /// None 保持当前计时集合。
    pub timers: Option<TimerUpdate>,
}
impl From<LogicalMessage> for Reply {
    fn from(response: LogicalMessage) -> Self {
        Self {
            response: Some(response),
            timers: None,
        }
    }
}

/// 可信客户端请求的关联标识；不含正文，不作为认证或分发依据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientRequest {
    /// 当前客户端请求关联序号。
    pub request_id: u64,
    /// 已准入的业务消息 ID。
    pub message_id: u32,
}

/// 客户端业务执行输入，身份由会话所有权确定。
pub struct RequestInput {
    /// 当前会话的可信身份。
    pub identity: SessionIdentity,
    /// 已准入的客户端业务请求。
    pub message: LogicalMessage,
}
/// 服务器内部事件执行输入，不与客户端请求共用序号。
pub struct EventInput {
    /// 当前会话的可信身份。
    pub identity: SessionIdentity,
    /// 当前会话的内部事件调用序号。
    pub request_id: u64,
    /// 保留原始 ID 和编码的共享事件。
    pub event: Arc<tok_events::Message>,
}
/// 内部 timer 执行输入，与客户端业务入口隔离。
pub struct TimerInput {
    /// 当前会话的可信身份。
    pub identity: SessionIdentity,
    /// 当前会话的内部 timer 调用序号。
    pub request_id: u64,
    /// 当前有效的计时需求。
    pub spec: TimerSpec,
    /// 本次预定和实际触发时刻。
    pub trigger: TimerTrigger,
}
/// 只在 session FIFO 和所有权检查之后执行的业务扩展点，使用共同执行上下文。
pub trait Executor: Send + Sync {
    /// 执行服务器事件；默认拒绝，不自动向客户端回应。
    fn execute_event(&self, _request: tok_execution::Request<EventInput>) -> EventTask<'_> {
        Box::pin(async { Err(ErrorCode::UnknownMessage.into()) })
    }
    /// 执行已准入的客户端业务请求。
    fn execute_request(&self, request: tok_execution::Request<RequestInput>) -> Task<'_>;
    /// 执行内部 timer；默认拒绝未支持的来源。
    fn execute_timer(&self, _request: tok_execution::Request<TimerInput>) -> Task<'_> {
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
pub struct RpcExecutor {
    service_name: String,
    router: Arc<dyn ServiceRouter>,
    client: Client,
    mapper: Arc<dyn RpcErrorMapper>,
}

impl RpcExecutor {
    /// 创建 player RPC 执行器，路由器与玩家协议客户端由组装层注入。
    pub fn new(
        service_name: impl Into<String>,
        router: Arc<dyn ServiceRouter>,
        client: Client,
    ) -> Result<Self> {
        let service_name = service_name.into();
        if service_name.trim().is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(Self {
            service_name,
            router,
            client,
            mapper: Arc::new(DefaultRpcErrorMapper),
        })
    }
    /// 替换系统错误映射，不改变无自动重试语义。
    pub fn with_error_mapper(mut self, mapper: Arc<dyn RpcErrorMapper>) -> Self {
        self.mapper = mapper;
        self
    }
}

impl Executor for RpcExecutor {
    fn execute_event(&self, request: tok_execution::Request<EventInput>) -> EventTask<'_> {
        let EventInput {
            identity,
            request_id,
            event,
        } = request.input;
        Box::pin(async move {
            let target = self
                .router
                .route(&self.service_name, &identity.player_id)
                .map_err(|_| ErrorCode::Unavailable)?;
            let response = self
                .client
                .handle_event(
                    &target,
                    CallContext {
                        node_id: target.node_id.clone(),
                        service_name: target.service_name.clone(),
                        bid: Some(identity.player_id.clone()),
                        request_id,
                        session: Some(identity),
                        timer: None,
                    },
                    &event,
                )
                .await
                .map_err(|status| self.mapper.map(&status))?;
            let timers = response
                .timers
                .map(tok_game_rpc::timer::decode_update)
                .transpose()
                .map_err(|_| ErrorCode::Internal)?;
            if let Some(error) = response.error {
                tracing::error!(code = error.code, "内部事件返回业务错误，不重试");
                return Err(ErrorCode::Internal.into());
            }
            Ok(timers)
        })
    }

    fn execute_request(&self, request: tok_execution::Request<RequestInput>) -> Task<'_> {
        self.call(request.input.identity, request.input.message, None)
    }
    fn execute_timer(&self, request: tok_execution::Request<TimerInput>) -> Task<'_> {
        let TimerInput {
            identity,
            request_id,
            spec,
            trigger,
        } = request.input;
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
impl RpcExecutor {
    fn call(
        &self,
        identity: SessionIdentity,
        request: LogicalMessage,
        timer: Option<TimerTrigger>,
    ) -> Task<'_> {
        Box::pin(async move {
            let is_timer = timer.is_some();
            let target = self
                .router
                .route(&self.service_name, &identity.player_id)
                .map_err(|_| ErrorCode::Unavailable)?;
            let response = self
                .client
                .call(
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
                .map(tok_game_rpc::timer::decode_update)
                .transpose()
                .map_err(|_| ErrorCode::Internal)?;
            if let Some(error) = response.error {
                if error.code == 0 {
                    return Err(ErrorCode::Internal.into());
                }
                return Ok(Reply {
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
                return Ok(Reply {
                    response: None,
                    timers,
                });
            }
            let payload = response.payload.ok_or(ErrorCode::Internal)?;
            Ok(Reply {
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
