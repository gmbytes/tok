//! 公会请求示例；请求级锁后端和持久化仍待后续实现。

use crate::protocol::v1::{GetGuildRequest, GetGuildResponse};
use tok::rpc::{HandlerRegistry, tonic::Status};

/// 读取公会信息的请求消息 ID。
pub const GET_GUILD: u32 = 3;
/// 读取公会信息的回应消息 ID。
pub const GET_GUILD_RESPONSE: u32 = 4;

/// 注册示例公会请求，校验消息与上下文中的业务 ID 一致。
pub fn handlers() -> Result<HandlerRegistry, tok::rpc::Error> {
    let mut registry = HandlerRegistry::new();
    registry.register(
        GET_GUILD,
        GET_GUILD_RESPONSE,
        |context, request: GetGuildRequest| async move {
            if context.bid.as_deref() != Some(request.guild_id.to_string().as_str()) {
                return Err(Status::invalid_argument("公会请求与目标业务 ID 不匹配"));
            }
            Ok(GetGuildResponse {
                guild_id: request.guild_id,
            })
        },
    )?;
    Ok(registry)
}
