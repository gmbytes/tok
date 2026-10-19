//! 使用真实 TCP 与框架信封验证登录、状态读写和公会调用的开发客户端。

use crate::{player, protocol::v1};
use prost::Message;
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
    let player = call::<_, v1::GetPlayerResponse>(
        &mut stream,
        &codec,
        2,
        player::GET_PLAYER,
        player::GET_PLAYER_RESPONSE,
        &v1::GetPlayerRequest { player_id: 9999 },
    )
    .await?;
    if player.player_id != 42 {
        return Err(std::io::Error::other("玩家身份取自客户端正文").into());
    }
    let set = call::<_, v1::SetPlayerResponse>(
        &mut stream,
        &codec,
        3,
        player::SET_PLAYER,
        player::SET_PLAYER_RESPONSE,
        &v1::SetPlayerRequest { value: 123 },
    )
    .await?;
    let player = call::<_, v1::GetPlayerResponse>(
        &mut stream,
        &codec,
        4,
        player::GET_PLAYER,
        player::GET_PLAYER_RESPONSE,
        &v1::GetPlayerRequest { player_id: 0 },
    )
    .await?;
    if set.player_id != 42 || set.value != 123 || player.value != 123 {
        return Err(std::io::Error::other("玩家状态读写不一致").into());
    }
    let guild = call::<_, v1::QueryGuildResponse>(
        &mut stream,
        &codec,
        5,
        player::QUERY_GUILD,
        player::QUERY_GUILD_RESPONSE,
        &v1::QueryGuildRequest { guild_id: 7 },
    )
    .await?;
    if guild.guild_id != 7 {
        return Err(std::io::Error::other("公会回应不一致").into());
    }
    call::<_, v1::StartProductionResponse>(
        &mut stream,
        &codec,
        6,
        player::START_PRODUCTION,
        player::START_PRODUCTION_RESPONSE,
        &v1::StartProductionRequest {},
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut request_id = 7;
        loop {
            let state = call::<_, v1::GetPlayerResponse>(
                &mut stream,
                &codec,
                request_id,
                player::GET_PLAYER,
                player::GET_PLAYER_RESPONSE,
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

async fn call<Req: Message, Resp: Message + Default>(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    request_id: u64,
    message_id: u32,
    response_id: u32,
    request: &Req,
) -> Result<Resp, Error> {
    codec
        .write(
            stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Request(protocol::BusinessRequest {
                    request_id,
                    message_id,
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
        || response.message_id != response_id
        || response.error_code != 0
    {
        return Err(
            std::io::Error::other(format!("客户端请求失败：{}", response.error_code)).into(),
        );
    }
    Ok(Resp::decode(response.body.as_slice())?)
}
