//! Player 接入的 TCP 协议处理、登录认证与玩家 session 绑定。

use super::ingress::MAX_CREDENTIAL_BYTES;
use super::session::BoundSession;
use crate::{
    ErrorCode, GatewayError, Result,
    codec::{FrameCodec, VERSION},
    player::{
        AuthenticatedPlayer, Authenticator, PlayerConfig, SessionManager,
        channel::{self, Outbound, OutboundSender},
    },
    protocol::{self, Frame, frame::Content},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::{LogicalMessage, MessageKind};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
// 连接任务退出时解除本代绑定；session 是否保留由通信通道的宽限期决定。
struct ConnectionGuard(Mutex<Option<BoundSession>>);
impl ConnectionGuard {
    fn close_session(&self) {
        if let Ok(Some(binding)) = self.0.lock().as_deref() {
            binding.close_session();
        }
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Ok(Some(session)) = self.0.get_mut() {
            session.detach();
        }
    }
}

pub(super) async fn serve_connection(
    stream: TcpStream,
    config: Arc<PlayerConfig>,
    codec: FrameCodec,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    shutdown: CancellationToken,
) -> Result<()> {
    stream.set_nodelay(true).map_err(|_| ErrorCode::Internal)?;
    let (mut reader, mut writer) = stream.into_split();
    let (sender, mut outgoing) = mpsc::channel(config.outbound_capacity);
    let binding = Arc::new(ConnectionGuard::default());
    let writing = async {
        while let Some(channel) = outgoing.recv().await {
            match channel {
                Outbound::Frame(frame, written) => {
                    codec.write(&mut writer, &frame).await?;
                    if let Some(written) = written {
                        let _ = written.send(());
                    }
                }
                Outbound::Encoded(body) => codec.write_body(&mut writer, &body).await?,
                Outbound::Replay(recovery) => {
                    let result = tokio::time::timeout(
                        Duration::from_millis(config.io_timeout_ms),
                        recovery.write_to(&codec, &mut writer),
                    )
                    .await
                    .map_err(|_| GatewayError::from(ErrorCode::Timeout))
                    .and_then(|result| result);
                    if let Err(error) = result {
                        if error.code == ErrorCode::ResumeUnavailable {
                            codec
                                .write(
                                    &mut writer,
                                    &Frame {
                                        version: VERSION,
                                        content: Some(Content::Error(protocol::ControlError {
                                            request_id: 0,
                                            error_code: error.code as u32,
                                        })),
                                    },
                                )
                                .await?;
                        }
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    };
    let reading = handle_frames(
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

// 将内容包装为协议帧并非阻塞入队，实际 socket 写入由连接写循环负责。
fn enqueue_content(sender: &OutboundSender, content: Content) -> Result<()> {
    sender
        .try_send(Outbound::Frame(
            Frame {
                version: VERSION,
                content: Some(content),
            },
            None,
        ))
        .map_err(|_| ErrorCode::SessionClosed.into())
}

// 发送错误并有限等待本地写入；关闭连接由调用方返回和守卫清理完成。
async fn send_error(sender: &OutboundSender, code: ErrorCode, timeout: Duration) {
    let (written, completed) = oneshot::channel();
    let frame = Frame {
        version: VERSION,
        content: Some(Content::Error(protocol::ControlError {
            request_id: 0,
            error_code: code as u32,
        })),
    };
    if sender
        .try_send(Outbound::Frame(frame, Some(written)))
        .is_ok()
    {
        let _ = tokio::time::timeout(timeout, completed).await;
    }
}

async fn handle_frames(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    config: &PlayerConfig,
    codec: &FrameCodec,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    sender: OutboundSender,
    binding: Arc<ConnectionGuard>,
) -> Result<()> {
    let mut session: Option<BoundSession> = None;
    let mut last_request_id = 0;
    loop {
        let cancellation = session.as_ref().map(BoundSession::cancellation);
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
                send_error(
                    &sender,
                    error.code,
                    Duration::from_millis(config.io_timeout_ms),
                )
                .await;
                if !matches!(error.code, ErrorCode::Timeout | ErrorCode::SessionClosed) {
                    binding.close_session();
                }
                return Err(error);
            }
        };
        match frame.content.ok_or(ErrorCode::InvalidRequest)? {
            Content::Login(login) => {
                let error = if login.request_id == 0 || login.request_id <= last_request_id {
                    Some(ErrorCode::InvalidRequest)
                } else if session.is_some() {
                    Some(ErrorCode::InvalidState)
                } else if (login.resume_token.is_empty()
                    && (login.credential.is_empty()
                        || login.credential.len() > MAX_CREDENTIAL_BYTES))
                    || (!login.resume_token.is_empty()
                        && (!login.credential.is_empty() || login.resume_token.len() != 64))
                {
                    Some(ErrorCode::AuthenticationFailed)
                } else {
                    None
                };
                last_request_id = last_request_id.max(login.request_id);
                let authenticated = if let Some(code) = error {
                    Err(GatewayError::from(code))
                } else if !login.resume_token.is_empty() {
                    sessions
                        .authenticate_resume_token(&login.resume_token)
                        .map(|player_id| AuthenticatedPlayer { player_id })
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
                        sessions
                            .create_session(player.player_id, sender.clone())
                            .await
                    }
                    Ok(_) => Err(ErrorCode::AuthenticationFailed.into()),
                    Err(error) => Err(error),
                };
                let response = match opened {
                    Ok(opened) => {
                        let identity = opened.identity().clone();
                        let response = protocol::LoginResponse {
                            request_id: login.request_id,
                            error_code: 0,
                            player_id: identity.player_id,
                            session_id: identity.session_id,
                            epoch: identity.epoch,
                            resume_token: opened.resume_token().to_owned(),
                            resume_token_ttl_ms: config.resume_token_ttl_ms,
                        };
                        let attached = match opened.complete_login(Frame {
                            version: VERSION,
                            content: Some(Content::LoggedIn(response)),
                        }) {
                            Ok(attached) => attached,
                            Err(error) => {
                                opened.close();
                                return Err(error);
                            }
                        };
                        *binding.0.lock().map_err(|_| ErrorCode::Internal)? =
                            Some(attached.clone());
                        session = Some(attached);
                        continue;
                    }
                    Err(error) => protocol::LoginResponse {
                        request_id: login.request_id,
                        error_code: error.code as u32,
                        player_id: String::new(),
                        session_id: String::new(),
                        ..Default::default()
                    },
                };
                enqueue_content(&sender, Content::LoggedIn(response))?;
            }
            Content::Resume(resume) => {
                let result = if resume.request_id == 0 || resume.request_id <= last_request_id {
                    Err(ErrorCode::InvalidRequest.into())
                } else if session.is_some() {
                    Err(ErrorCode::InvalidState.into())
                } else {
                    sessions
                        .resume_session(
                            &resume.resume_token,
                            resume.ack_seq,
                            resume.request_id,
                            sender.clone(),
                        )
                        .await
                };
                last_request_id = last_request_id.max(resume.request_id);
                match result {
                    Ok(attached) => {
                        last_request_id = last_request_id.max(attached.last_request_id()?);
                        *binding.0.lock().map_err(|_| ErrorCode::Internal)? =
                            Some(attached.clone());
                        session = Some(attached);
                    }
                    Err(error) => enqueue_content(
                        &sender,
                        Content::Resumed(protocol::ResumeResponse {
                            request_id: resume.request_id,
                            error_code: error.code as u32,
                            ..Default::default()
                        }),
                    )?,
                }
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
                    if let Some(session) = &session {
                        session.publish_response(
                            channel::error_response(&request, error.code),
                            request.message_id,
                        )?;
                    } else {
                        enqueue_content(
                            &sender,
                            Content::Error(protocol::ControlError {
                                request_id: request.request_id,
                                error_code: error.code as u32,
                            }),
                        )?;
                    }
                }
            }
            Content::Ack(ack) => {
                let result = session
                    .as_ref()
                    .ok_or_else(|| GatewayError::from(ErrorCode::Unauthenticated))
                    .and_then(|session| session.acknowledge(ack.ack_seq));
                if let Err(error) = result {
                    send_error(
                        &sender,
                        error.code,
                        Duration::from_millis(config.io_timeout_ms),
                    )
                    .await;
                    binding.close_session();
                    return Err(error);
                }
            }
            _ => {
                send_error(
                    &sender,
                    ErrorCode::InvalidState,
                    Duration::from_millis(config.io_timeout_ms),
                )
                .await;
                binding.close_session();
                return Err(ErrorCode::InvalidState.into());
            }
        }
    }
}
