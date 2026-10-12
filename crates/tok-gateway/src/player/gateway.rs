//! 受监督的 TCP 连接接入、认证与玩家 session 绑定。

use crate::{
    ErrorCode, GatewayError, Result,
    codec::{FrameCodec, VERSION},
    player::{
        Authenticator, GatewayConfig, PlayerExecutor, SessionHandle, SessionManager,
        outbound::{self, Outbound, Sender},
        session::SessionTask,
    },
    protocol::{self, Frame, frame::Content},
};
use prost::Message;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::{
    LogicalMessage, MessageKind,
    coordination::{OwnershipStore, SessionOwner},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

/// 控制登录凭证的最大 UTF-8 字节数，不作为日志字段。
pub const MAX_CREDENTIAL_BYTES: usize = 1024;

/// 单进程玩家 TCP 网关，session 与连接任务由 serve 统一监督。
pub struct PlayerGateway {
    config: Arc<GatewayConfig>,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    launches: mpsc::Receiver<SessionTask>,
}

struct StopSessions(SessionManager);
impl Drop for StopSessions {
    fn drop(&mut self) {
        self.0.close_all();
    }
}

impl PlayerGateway {
    /// 注入认证、所有权与执行后端；网关不直接依赖 Redis 或具体游戏。
    pub fn new(
        config: GatewayConfig,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
    ) -> Result<Self> {
        config.validate().map_err(|_| ErrorCode::InvalidRequest)?;
        if owner.node_id.trim().is_empty() || owner.service_name.trim().is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let config = Arc::new(config);
        let (sender, launches) = mpsc::channel(config.max_sessions);
        let sessions = SessionManager::new(
            config.clone(),
            owner,
            ownership,
            executor,
            sender,
            CancellationToken::new(),
        );
        Ok(Self {
            config,
            authenticator,
            sessions,
            launches,
        })
    }

    /// 获取活动 session 的查询与通知入口，不能创建未认证会话。
    pub fn sessions(&self) -> SessionManager {
        self.sessions.clone()
    }

    /// 接管已绑定 TCP 监听，监督连接与 session；取消后停止新工作并条件释放租约。
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let Self {
            config,
            authenticator,
            sessions,
            mut launches,
        } = self;
        let _stop = StopSessions(sessions.clone());
        let codec = FrameCodec::new(
            config.max_frame_size,
            Duration::from_millis(config.io_timeout_ms),
        )
        .map_err(std::io::Error::other)?;
        let capacity = Arc::new(Semaphore::new(config.max_connections));
        let mut connections = JoinSet::new();
        let mut actors = JoinSet::new();
        let mut outcome = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break Ok(()),
                Some(task) = launches.recv() => { actors.spawn(task); }
                result = actors.join_next(), if !actors.is_empty() => {
                    if let Some(Err(error)) = result { break Err(std::io::Error::other(format!("session 任务崩溃：{error}"))); }
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = result { break Err(std::io::Error::other(format!("连接任务崩溃：{error}"))); }
                }
                accepted = listener.accept() => {
                    let (stream, _) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(error) };
                    let permit = match capacity.clone().try_acquire_owned() { Ok(permit) => permit, Err(_) => {
                        metrics::counter!("tok_gateway_connections_rejected_total").increment(1);
                        drop(stream); continue;
                    }};
                    let config = config.clone();
                    let codec = codec.clone();
                    let authenticator = authenticator.clone();
                    let sessions = sessions.clone();
                    let shutdown = shutdown.child_token();
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = connection(stream, config, codec, authenticator, sessions, shutdown).await {
                            tracing::debug!(code = ?error.code, "客户端连接已结束");
                        }
                    });
                }
            }
        };
        drop(listener);
        sessions.close_all();
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        // 尚未开始的 session 也纳入监督，取消后只清理而不执行新业务。
        while let Ok(task) = launches.try_recv() {
            actors.spawn(task);
        }
        while let Some(result) = actors.join_next().await {
            if let Err(error) = result {
                tracing::error!(error = %error, "session 清理任务异常");
                if outcome.is_ok() {
                    outcome = Err(std::io::Error::other(format!(
                        "session 清理任务异常：{error}"
                    )));
                }
            }
        }
        sessions.release_all().await;
        outcome
    }
}

#[derive(Default)]
struct Binding(Mutex<Option<SessionHandle>>);
impl Drop for Binding {
    fn drop(&mut self) {
        if let Ok(Some(session)) = self.0.get_mut() {
            session.close();
        }
    }
}

async fn connection(
    stream: TcpStream,
    config: Arc<GatewayConfig>,
    codec: FrameCodec,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    shutdown: CancellationToken,
) -> Result<()> {
    stream.set_nodelay(true).map_err(|_| ErrorCode::Internal)?;
    let (mut reader, mut writer) = stream.into_split();
    let (sender, mut outgoing) = mpsc::channel(config.outbound_capacity);
    let binding = Arc::new(Binding::default());
    let writing = async {
        while let Some(outbound) = outgoing.recv().await {
            let (mut frame, written) = match outbound {
                Outbound::Control(frame, written) => (frame, written),
                Outbound::Message(message) => (outbound::frame(message)?, None),
            };
            if frame.encoded_len() > config.max_frame_size {
                if let Some(Content::Response(response)) = &frame.content {
                    frame = Frame {
                        version: VERSION,
                        content: Some(Content::Response(protocol::BusinessResponse {
                            request_id: response.request_id,
                            message_id: response.message_id,
                            error_code: ErrorCode::FrameTooLarge as u32,
                            body: Vec::new(),
                        })),
                    };
                } else {
                    return Err(ErrorCode::FrameTooLarge.into());
                }
            }
            codec.write(&mut writer, &frame).await?;
            if let Some(written) = written {
                let _ = written.send(());
            }
        }
        Ok(())
    };
    let reading = read_connection(
        &mut reader,
        &config,
        &codec,
        authenticator,
        sessions,
        sender,
        binding.clone(),
    );
    tokio::select! { biased; _ = shutdown.cancelled() => Ok(()), result = writing => result, result = reading => result }
}

fn control(sender: &Sender, content: Content) -> Result<()> {
    sender
        .try_send(Outbound::Control(
            Frame {
                version: VERSION,
                content: Some(content),
            },
            None,
        ))
        .map_err(|_| ErrorCode::SessionClosed.into())
}

async fn close_with_error(sender: &Sender, code: ErrorCode, timeout: Duration) {
    let (written, completed) = oneshot::channel();
    let frame = Frame {
        version: VERSION,
        content: Some(Content::Error(protocol::ControlError {
            request_id: 0,
            error_code: code as u32,
        })),
    };
    if sender
        .try_send(Outbound::Control(frame, Some(written)))
        .is_ok()
    {
        let _ = tokio::time::timeout(timeout, completed).await;
    }
}

async fn read_connection(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    config: &GatewayConfig,
    codec: &FrameCodec,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    sender: Sender,
    binding: Arc<Binding>,
) -> Result<()> {
    let mut session: Option<SessionHandle> = None;
    let mut last_request_id = 0;
    loop {
        let cancellation = session.as_ref().map(SessionHandle::cancellation);
        let lost_session = async {
            if let Some(cancellation) = cancellation {
                cancellation.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let frame = tokio::select! { biased; _ = lost_session => return Ok(()), frame = codec.read(reader) => frame };
        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                close_with_error(
                    &sender,
                    error.code,
                    Duration::from_millis(config.io_timeout_ms),
                )
                .await;
                return Err(error);
            }
        };
        match frame.content.ok_or(ErrorCode::InvalidRequest)? {
            Content::Login(login) => {
                let error = if login.request_id == 0 || login.request_id <= last_request_id {
                    Some(ErrorCode::InvalidRequest)
                } else if session.is_some() {
                    Some(ErrorCode::InvalidState)
                } else if login.credential.is_empty()
                    || login.credential.len() > MAX_CREDENTIAL_BYTES
                {
                    Some(ErrorCode::AuthenticationFailed)
                } else {
                    None
                };
                last_request_id = last_request_id.max(login.request_id);
                let authenticated = if let Some(code) = error {
                    Err(GatewayError::from(code))
                } else {
                    match tokio::time::timeout(
                        Duration::from_millis(config.auth_timeout_ms),
                        authenticator.authenticate(login.credential),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(ErrorCode::Timeout.into()),
                    }
                };
                let opened = match authenticated {
                    Ok(player)
                        if !player.player_id.trim().is_empty() && player.player_id.len() <= 256 =>
                    {
                        sessions.open(player.player_id, sender.clone()).await
                    }
                    Ok(_) => Err(ErrorCode::AuthenticationFailed.into()),
                    Err(error) => Err(error),
                };
                let response = match opened {
                    Ok(opened) => {
                        let identity = opened.identity().clone();
                        *binding.0.lock().map_err(|_| ErrorCode::Internal)? = Some(opened.clone());
                        session = Some(opened);
                        protocol::LoginResponse {
                            request_id: login.request_id,
                            error_code: 0,
                            player_id: identity.player_id,
                            session_id: identity.session_id,
                            epoch: identity.epoch,
                        }
                    }
                    Err(error) => protocol::LoginResponse {
                        request_id: login.request_id,
                        error_code: error.code as u32,
                        player_id: String::new(),
                        session_id: String::new(),
                        epoch: 0,
                    },
                };
                control(&sender, Content::LoggedIn(response))?;
            }
            Content::Request(request) => {
                let request = LogicalMessage {
                    kind: MessageKind::Request,
                    message_id: request.message_id,
                    request_id: request.request_id,
                    server_seq: None,
                    error_code: 0,
                    body: request.body,
                };
                let result = if request.request_id == 0
                    || request.request_id <= last_request_id
                    || request.message_id == 0
                {
                    Err(ErrorCode::InvalidRequest.into())
                } else if let Some(session) = &session {
                    session.submit(request.clone())
                } else {
                    Err(ErrorCode::Unauthenticated.into())
                };
                last_request_id = last_request_id.max(request.request_id);
                if let Err(error) = result {
                    outbound::send(&sender, outbound::failed(&request, error.code))?;
                }
            }
            _ => {
                close_with_error(
                    &sender,
                    ErrorCode::InvalidState,
                    Duration::from_millis(config.io_timeout_ms),
                )
                .await;
                return Err(ErrorCode::InvalidState.into());
            }
        }
    }
}
