//! 显式注册 player 业务消息，身份来自网关调用上下文。

use crate::protocol::v1::{
    GetGuildRequest, GetGuildResponse, GetPlayerRequest, GetPlayerResponse, QueryGuildRequest,
    QueryGuildResponse, SetPlayerRequest, SetPlayerResponse,
};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tok::{
    core::{CallContext, ServiceDirectory},
    rpc::{HandlerRegistry, RpcClients, protocol::internal::v1::Payload, tonic::Status},
};

/// 读取玩家信息的请求消息 ID。
pub const GET_PLAYER: u32 = 1;
/// 读取玩家信息的回应消息 ID。
pub const GET_PLAYER_RESPONSE: u32 = 2;
/// 写入示例玩家状态的请求 ID。
pub const SET_PLAYER: u32 = 5;
/// 写入示例玩家状态的回应 ID。
pub const SET_PLAYER_RESPONSE: u32 = 6;
/// 由 player 查询公会的请求 ID。
pub const QUERY_GUILD: u32 = 7;
/// player 返回公会查询结果的回应 ID。
pub const QUERY_GUILD_RESPONSE: u32 = 8;

/// 模拟游戏存储的有界内存状态，多个 player 请求共享，不创建常驻 player 对象。
pub struct State {
    capacity: usize,
    values: Mutex<BTreeMap<u64, i64>>,
}
impl State {
    /// 创建具有指定记录上限的示例存储，capacity 必须大于零。
    pub fn new(capacity: usize) -> Result<Self, tok::core::Error> {
        if capacity == 0 {
            return Err(tok::core::Error::InvalidInput(
                "示例状态容量必须大于零".into(),
            ));
        }
        Ok(Self {
            capacity,
            values: Mutex::new(BTreeMap::new()),
        })
    }
}

fn player_id(context: &CallContext) -> Result<u64, Status> {
    context
        .session
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("缺少玩家会话"))?
        .player_id
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("示例玩家 ID 必须为整数"))
}

/// 注册示例玩家请求，以上下文中的会话身份选择玩家。
pub fn handlers(
    state: Arc<State>,
    directory: Arc<dyn ServiceDirectory>,
) -> Result<HandlerRegistry, tok::rpc::Error> {
    let mut registry = HandlerRegistry::new();
    registry.register(GET_PLAYER, GET_PLAYER_RESPONSE, {
        let state = state.clone();
        move |context, _request: GetPlayerRequest| {
            let state = state.clone();
            async move {
                let player_id = player_id(&context)?;
                let value = state
                    .values
                    .lock()
                    .map_err(|_| Status::internal("示例存储锁不可用"))?
                    .get(&player_id)
                    .copied()
                    .unwrap_or(0);
                Ok(GetPlayerResponse { player_id, value })
            }
        }
    })?;
    registry.register(
        SET_PLAYER,
        SET_PLAYER_RESPONSE,
        move |context, request: SetPlayerRequest| {
            let state = state.clone();
            async move {
                let player_id = player_id(&context)?;
                let mut values = state
                    .values
                    .lock()
                    .map_err(|_| Status::internal("示例存储锁不可用"))?;
                if !values.contains_key(&player_id) && values.len() >= state.capacity {
                    return Err(Status::resource_exhausted("示例存储容量已满"));
                }
                values.insert(player_id, request.value);
                Ok(SetPlayerResponse {
                    player_id,
                    value: request.value,
                })
            }
        },
    )?;
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 16)?);
    registry.register(
        QUERY_GUILD,
        QUERY_GUILD_RESPONSE,
        move |context, request: QueryGuildRequest| {
            let directory = directory.clone();
            let clients = clients.clone();
            async move {
                let target = tok::infra::routing::route(
                    directory.as_ref(),
                    "guild",
                    &request.guild_id.to_string(),
                )
                .map_err(|_| Status::unavailable("公会服务不可用"))?;
                let response = clients
                    .business_call(
                        &target,
                        CallContext {
                            node_id: target.node_id.clone(),
                            service_name: target.service_name.clone(),
                            bid: Some(request.guild_id.to_string()),
                            request_id: context.request_id,
                            session: None,
                        },
                        Payload {
                            message_id: crate::guild::GET_GUILD,
                            body: GetGuildRequest {
                                guild_id: request.guild_id,
                            }
                            .encode_to_vec(),
                        },
                    )
                    .await?;
                let payload = response
                    .payload
                    .ok_or_else(|| Status::internal("公会回应缺少消息正文"))?;
                let body = GetGuildResponse::decode(payload.body.as_slice())
                    .map_err(|_| Status::internal("公会回应解码失败"))?;
                Ok(QueryGuildResponse {
                    guild_id: body.guild_id,
                })
            }
        },
    )?;
    Ok(registry)
}
