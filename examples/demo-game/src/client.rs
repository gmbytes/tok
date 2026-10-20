//! 使用真实 TCP 与框架信封验证登录、状态读写和公会调用的开发客户端。

use crate::protocol::{GameMessage, v1};
use std::time::Duration;
use tok::{
    gateway::{
        codec::{FrameCodec, VERSION},
        protocol::{self, Frame, frame::Content},
    },
    observability::Error,
};
use tokio::net::TcpStream;

/// 连接示例网关，验证可信身份、状态读写及 player 到 guild 的实际调用。
pub async fn smoke(address: std::net::SocketAddr) -> Result<(), Error> {
    let mut stream = TcpStream::connect(address).await?;
    let codec = FrameCodec::new(1_048_576, Duration::from_secs(5))?;
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Login(protocol::LoginRequest {
                    request_id: 1,
                    credential: "demo:42".into(),
                    resume_token: String::new(),
                })),
            },
        )
        .await?;
    let login = codec
        .read(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::other("登录前连接结束"))?;
    let Some(Content::LoggedIn(login)) = login.content else {
        return Err(std::io::Error::other("未收到登录回应").into());
    };
    if login.error_code != 0
        || login.player_id != "42"
        || login.epoch == 0
        || login.session_id.is_empty()
    {
        return Err(std::io::Error::other("登录身份无效").into());
    }
    let mut server_seq = 0;
    let player = call::<_, v1::GetPlayerResponse>(
        &mut stream,
        &codec,
        &mut server_seq,
        2,
        &v1::GetPlayerRequest { player_id: 9999 },
    )
    .await?;
    if player.player_id != 42 {
        return Err(std::io::Error::other("玩家身份取自客户端正文").into());
    }
    let set = call::<_, v1::SetPlayerResponse>(
        &mut stream,
        &codec,
        &mut server_seq,
        3,
        &v1::SetPlayerRequest { value: 123 },
    )
    .await?;
    let player = call::<_, v1::GetPlayerResponse>(
        &mut stream,
        &codec,
        &mut server_seq,
        4,
        &v1::GetPlayerRequest { player_id: 0 },
    )
    .await?;
    if set.player_id != 42 || set.value != 123 || player.value != 123 {
        return Err(std::io::Error::other("玩家状态读写不一致").into());
    }
    let guild = call::<_, v1::QueryGuildResponse>(
        &mut stream,
        &codec,
        &mut server_seq,
        5,
        &v1::QueryGuildRequest { guild_id: 7 },
    )
    .await?;
    if guild.guild_id != 7 {
        return Err(std::io::Error::other("公会回应不一致").into());
    }
    call::<_, v1::StartProductionResponse>(
        &mut stream,
        &codec,
        &mut server_seq,
        6,
        &v1::StartProductionRequest {},
    )
    .await?;
    // 在线连接主动刷新凭证；客户端收到成功回应后替换保存，不改变业务序号。
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::RefreshResumeToken(
                    protocol::RefreshResumeTokenRequest { request_id: 7 },
                )),
            },
        )
        .await?;
    let refreshed = codec
        .read(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::other("凭证刷新前连接结束"))?;
    let Some(Content::ResumeTokenRefreshed(refreshed)) = refreshed.content else {
        return Err(std::io::Error::other("缺少凭证刷新回应").into());
    };
    if refreshed.request_id != 7
        || refreshed.error_code != 0
        || refreshed.resume_token.is_empty()
        || refreshed.resume_token_ttl_ms == 0
    {
        return Err(std::io::Error::other("凭证刷新失败").into());
    }
    // 短线期间生产 timer 继续运行；新连接恢复原会话及下行位置。
    drop(stream);
    tokio::time::sleep(Duration::from_millis(80)).await;
    stream = TcpStream::connect(address).await?;
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Resume(protocol::ResumeRequest {
                    request_id: 1,
                    resume_token: refreshed.resume_token.clone(),
                    ack_seq: server_seq,
                })),
            },
        )
        .await?;
    let resumed = codec
        .read(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::other("恢复前连接结束"))?;
    let Some(Content::Resumed(resumed)) = resumed.content else {
        return Err(std::io::Error::other("未收到恢复回应").into());
    };
    if resumed.error_code != 0
        || resumed.session_id != login.session_id
        || resumed.epoch != login.epoch
    {
        return Err(std::io::Error::other("原会话恢复失败").into());
    }
    let recovered = codec
        .read(&mut stream)
        .await?
        .ok_or_else(|| std::io::Error::other("恢复期间连接结束"))?;
    let Some(Content::Recovered(recovered)) = recovered.content else {
        return Err(std::io::Error::other("缺少恢复完成标记").into());
    };
    if recovered.through_seq != server_seq {
        return Err(std::io::Error::other("恢复序号不一致").into());
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut request_id = resumed.last_request_id.max(7) + 1;
        loop {
            let state = call::<_, v1::GetPlayerResponse>(
                &mut stream,
                &codec,
                &mut server_seq,
                request_id,
                &v1::GetPlayerRequest { player_id: 0 },
            )
            .await?;
            if state.value == 124 {
                return Ok::<(), Error>(());
            }
            request_id += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| std::io::Error::other("示例 timer 未按期更新玩家状态"))??;
    Ok(())
}

async fn call<Req: GameMessage, Resp: GameMessage + Default>(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    server_seq: &mut u64,
    request_id: u64,
    request: &Req,
) -> Result<Resp, Error> {
    codec
        .write(
            stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Request(protocol::BusinessRequest {
                    request_id,
                    message_id: Req::MESSAGE_ID,
                    body: request.encode_to_vec(),
                })),
            },
        )
        .await?;
    let response = codec
        .read(stream)
        .await?
        .ok_or_else(|| std::io::Error::other("业务回应前连接结束"))?;
    let Some(Content::Response(response)) = response.content else {
        return Err(std::io::Error::other("未收到业务回应").into());
    };
    if response.request_id != request_id
        || response.server_seq
            != server_seq
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("下行序号耗尽"))?
        || response.message_id != Resp::MESSAGE_ID
        || response.error_code != 0
    {
        return Err(
            std::io::Error::other(format!("客户端请求失败：{}", response.error_code)).into(),
        );
    }
    let message = Resp::decode(response.body.as_slice())?;
    *server_seq = response.server_seq;
    codec
        .write(
            stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Ack(protocol::Ack {
                    ack_seq: *server_seq,
                })),
            },
        )
        .await?;
    Ok(message)
}
