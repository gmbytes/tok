//! 显式注册 player 业务消息，身份来自网关调用上下文。

mod production;

use crate::protocol::GameMessage;
use crate::protocol::v1::{
    GetGuildRequest, GetGuildResponse, GetPlayerRequest, GetPlayerResponse, ProductionFinished,
    QueryGuildRequest, QueryGuildResponse, SetPlayerRequest, SetPlayerResponse,
    StartProductionRequest, StartProductionResponse,
};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok::{
    core::{CallContext, ServiceDirectory, TimerSpec, TimerUpdate},
    rpc::{
        HandlerRegistry, HandlerReply, RpcClients, protocol::internal::v1::Payload, tonic::Status,
    },
};
use tokio::sync::Mutex;

const PRODUCTION_TIME_MS: i64 = 50;

/// 模拟游戏存储的有界内存状态，多个 player 请求共享，不创建常驻 player 对象。
pub struct State {
    capacity: usize,
    values: Mutex<BTreeMap<u64, PlayerState>>,
}
#[derive(Clone, Copy, Default)]
struct PlayerState {
    value: i64,
    production_due: Option<i64>,
}

fn production_spec(due_at_ms: i64) -> TimerSpec {
    TimerSpec {
        timer_id: "production".into(),
        message_id: ProductionFinished::MESSAGE_ID,
        body: ProductionFinished {}.encode_to_vec(),
        due_at_ms,
        interval_ms: None,
    }
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
    registry.register_with_timers(
        GetPlayerRequest::MESSAGE_ID,
        GetPlayerResponse::MESSAGE_ID,
        {
            let state = Arc::clone(&state);
            move |context, _request: GetPlayerRequest| {
                let state = Arc::clone(&state);
                async move {
                    let player_id = player_id(&context)?;
                    let stored = state
                        .values
                        .lock()
                        .await
                        .get(&player_id)
                        .copied()
                        .unwrap_or_default();
                    Ok(HandlerReply {
                        message: GetPlayerResponse {
                            player_id,
                            value: stored.value,
                        },
                        timers: Some(TimerUpdate::ReplaceAll(
                            stored
                                .production_due
                                .into_iter()
                                .map(production_spec)
                                .collect(),
                        )),
                    })
                }
            }
        },
    )?;
    registry.register(
        SetPlayerRequest::MESSAGE_ID,
        SetPlayerResponse::MESSAGE_ID,
        {
            let state = Arc::clone(&state);
            move |context, request: SetPlayerRequest| {
                let state = Arc::clone(&state);
                async move {
                    let player_id = player_id(&context)?;
                    let mut values = state.values.lock().await;
                    if !values.contains_key(&player_id) && values.len() >= state.capacity {
                        return Err(Status::resource_exhausted("示例存储容量已满"));
                    }
                    values.entry(player_id).or_default().value = request.value;
                    Ok(SetPlayerResponse {
                        player_id,
                        value: request.value,
                    })
                }
            }
        },
    )?;
    registry.register_with_timers(
        StartProductionRequest::MESSAGE_ID,
        StartProductionResponse::MESSAGE_ID,
        {
            let state = Arc::clone(&state);
            move |context, _request: StartProductionRequest| {
                let state = Arc::clone(&state);
                async move {
                    let id = player_id(&context)?;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| Status::internal("系统时钟无效"))?
                        .as_millis() as i64;
                    let due = now + PRODUCTION_TIME_MS;
                    let mut values = state.values.lock().await;
                    if !values.contains_key(&id) && values.len() >= state.capacity {
                        return Err(Status::resource_exhausted("示例存储容量已满"));
                    }
                    values.entry(id).or_default().production_due = Some(due);
                    Ok(HandlerReply {
                        message: StartProductionResponse {},
                        timers: Some(TimerUpdate::Patch {
                            upsert: vec![production_spec(due)],
                            cancel: vec![],
                        }),
                    })
                }
            }
        },
    )?;
    let production = Arc::new(
        production::dispatcher()
            .map_err(|error| tok::rpc::Error::Configuration(error.to_string()))?,
    );
    let events_state = Arc::clone(&state);
    registry.register_timer_service(
        ProductionFinished::MESSAGE_ID,
        tok::rpc::tower::service_fn(
            move |request: tok::service::Request<(CallContext, ProductionFinished)>| {
                let execution = request.context;
                let context = request.input.0;
                let state = Arc::clone(&state);
                let production = Arc::clone(&production);
                async move {
                    let id = player_id(&context)?;
                    let mut values = state.values.lock().await;
                    if !values.contains_key(&id) && values.len() >= state.capacity {
                        return Err(Status::resource_exhausted("示例存储容量已满"));
                    }
                    let scheduled = context
                        .timer
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("缺少内部计时来源"))?
                        .scheduled_at_ms;
                    let stored = values.entry(id).or_default();
                    let event = tok::events::Envelope::new(
                        tok::events::EventId::generate(),
                        production::Completed {
                            scheduled_at_ms: scheduled,
                        },
                    )
                    .map_err(|error| Status::internal(error.to_string()))?;
                    production
                        .dispatch(stored, tok::service::Request::new(execution, &event))
                        .await
                        .map_err(|error| match error {
                            tok::events::DispatchError::Handler(error) => error,
                            other => Status::internal(other.to_string()),
                        })?;
                    Ok(None)
                }
            },
        ),
    )?;
    registry.register_event::<crate::events::AddValue, _>(move |request| {
        let context = request.input.state;
        let event = &request.input.event.payload;
        let state = Arc::clone(&events_state);
        Box::pin(async move {
            let id = player_id(context)?;
            let mut values = state.values.lock().await;
            if !values.contains_key(&id) && values.len() >= state.capacity {
                return Err(Status::resource_exhausted("示例存储容量已满"));
            }
            let stored = values.entry(id).or_default();
            stored.value = stored
                .value
                .checked_add(event.delta)
                .ok_or_else(|| Status::out_of_range("事件产出溢出"))?;
            Ok(None)
        })
    })?;
    let client =
        tok::rpc::business::Client::new(Arc::new(RpcClients::new(Duration::from_secs(2), 16)?));
    registry.register(
        QueryGuildRequest::MESSAGE_ID,
        QueryGuildResponse::MESSAGE_ID,
        move |context, request: QueryGuildRequest| {
            let directory = Arc::clone(&directory);
            let client = client.clone();
            async move {
                let target = tok::infra::routing::route(
                    directory.as_ref(),
                    "guild",
                    &request.guild_id.to_string(),
                )
                .map_err(|_| Status::unavailable("公会服务不可用"))?;
                let response = client
                    .call(
                        &target,
                        CallContext {
                            timer: None,
                            node_id: target.node_id.clone(),
                            service_name: target.service_name.clone(),
                            bid: Some(request.guild_id.to_string()),
                            request_id: context.request_id,
                            session: None,
                        },
                        Payload {
                            message_id: GetGuildRequest::MESSAGE_ID,
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
