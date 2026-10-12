//! 显式注册 player 业务消息，身份来自网关调用上下文。

use crate::protocol::v1::{GetPlayerRequest, GetPlayerResponse};
use tok::rpc::{HandlerRegistry, tonic::Status};

/// 读取玩家信息的请求消息 ID。
pub const GET_PLAYER: u32 = 1;
/// 读取玩家信息的回应消息 ID。
pub const GET_PLAYER_RESPONSE: u32 = 2;

/// 注册示例玩家请求，以上下文中的会话身份选择玩家。
pub fn handlers() -> Result<HandlerRegistry, tok::rpc::Error> {
    let mut registry = HandlerRegistry::new();
    registry.register(
        GET_PLAYER,
        GET_PLAYER_RESPONSE,
        |context, _request: GetPlayerRequest| async move {
            let identity = context
                .session
                .ok_or_else(|| Status::failed_precondition("缺少玩家会话"))?;
            let player_id = identity
                .player_id
                .parse::<u64>()
                .map_err(|_| Status::invalid_argument("示例玩家 ID 必须为整数"))?;
            Ok(GetPlayerResponse { player_id })
        },
    )?;
    Ok(registry)
}
