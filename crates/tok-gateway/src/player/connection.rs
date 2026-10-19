//! Player 接入的 TCP 协议处理、登录认证与玩家 session 绑定。

use super::ingress::MAX_CREDENTIAL_BYTES;
use crate::{
    ErrorCode, GatewayError, Result,
    codec::{FrameCodec, VERSION},
    player::{
        Authenticator, PlayerConfig, SessionHandle, SessionManager,
        outbound::{self, Outbound, Sender},
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
struct Binding(Mutex<Option<SessionHandle>>);
impl Drop for Binding {
    fn drop(&mut self) {
        if let Ok(Some(session)) = self.0.get_mut() {
            session.close();
        }
    }
}

pub(super) async fn connection(
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
    let binding = Arc::new(Binding::default());
    let writing = async {
        while let Some(outbound) = outgoing.recv().await {
            match outbound {
                Outbound::Frame(frame, written) => {
                    codec.write(&mut writer, &frame).await?;
                    if let Some(written) = written {
                        let _ = written.send(());
                    }
                }
                Outbound::Encoded(body) => codec.write_body(&mut writer, &body).await?,
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

// 将内容包装为协议帧并非阻塞入队，实际 socket 写入由连接写循环负责。
fn enqueue_content(sender: &Sender, content: Content) -> Result<()> {
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
        .try_send(Outbound::Frame(frame, Some(written)))
        .is_ok()
    {
        let _ = tokio::time::timeout(timeout, completed).await;
    }
}

async fn read_connection(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    config: &PlayerConfig,
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
                        let response = protocol::LoginResponse {
                            request_id: login.request_id,
                            error_code: 0,
                            player_id: identity.player_id,
                            session_id: identity.session_id,
                            epoch: identity.epoch,
                        };
                        opened.start(Frame {
                            version: VERSION,
                            content: Some(Content::LoggedIn(response)),
                        })?;
                        session = Some(opened);
                        continue;
                    }
                    Err(error) => protocol::LoginResponse {
                        request_id: login.request_id,
                        error_code: error.code as u32,
                        player_id: String::new(),
                        session_id: String::new(),
                        epoch: 0,
                    },
                };
                enqueue_content(&sender, Content::LoggedIn(response))?;
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
                        session
                            .respond(outbound::failed(&request, error.code), request.message_id)?;
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
                    close_with_error(
                        &sender,
                        error.code,
                        Duration::from_millis(config.io_timeout_ms),
                    )
                    .await;
                    return Err(error);
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
