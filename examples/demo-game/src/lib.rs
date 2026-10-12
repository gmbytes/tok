//! 示例游戏自己的协议和业务模块，框架 crate 不依赖这些模块。

pub mod guild;
pub mod player;
pub mod protocol;

use prost::Message;
use std::{sync::Arc, time::Duration};
use tok::{
    AppConfig,
    core::{CallContext, ServiceDirectory, SessionIdentity},
    observability::Error,
    rpc::{RpcClients, RpcEndpoint, RpcService, protocol::internal::v1::Payload},
    runtime::{Application, ServiceContext, ServiceFactory, ServiceTask, StartTask},
};

struct LifecycleService;
impl ServiceFactory for LifecycleService {
    fn start(&self, context: ServiceContext) -> StartTask {
        Box::pin(async move {
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                Ok(())
            }) as ServiceTask)
        })
    }
}

/// 组装示例的内部 gRPC 服务；客户端网关仍只运行生命周期。
pub fn application(config: AppConfig) -> Result<Application, Error> {
    let mut app = Application::new(config.clone())?;
    for service in &config.services {
        app.register(&service.service_name, LifecycleService)?;
    }
    for endpoint in &config.endpoints {
        if endpoint.protocol != "grpc" {
            continue;
        }
        let mut rpc = RpcEndpoint::new();
        let mut count = 0;
        for service in config
            .services
            .iter()
            .filter(|service| service.endpoint == endpoint.name)
        {
            rpc = match service.service_name.as_str() {
                "player" => rpc.add_service(
                    "player",
                    RpcService::new(&config.node_id, "player", Arc::new(player::handlers()?))?
                        .into_player(),
                )?,
                "guild" => rpc.add_service(
                    "guild",
                    RpcService::new(&config.node_id, "guild", Arc::new(guild::handlers()?))?
                        .into_business(),
                )?,
                other => {
                    return Err(
                        std::io::Error::other(format!("示例尚未提供 RPC 服务：{other}")).into(),
                    );
                }
            };
            count += 1;
        }
        if count > 0 {
            app.register_rpc_endpoint(&endpoint.name, rpc)?;
        }
    }
    Ok(app)
}

/// 内部 RPC 验证，不代表客户端认证或 session 执行器已经实现。
pub async fn rpc_smoke(directory: &dyn ServiceDirectory) -> Result<(), Error> {
    let player = tok::infra::routing::route(directory, "player", "42")?;
    let guild = tok::infra::routing::route(directory, "guild", "7")?;
    let clients = RpcClients::new(Duration::from_secs(2), 8)?;
    let response = clients
        .player_call(
            &player,
            CallContext {
                node_id: player.node_id.clone(),
                service_name: player.service_name.clone(),
                bid: Some("42".into()),
                request_id: 1,
                session: Some(SessionIdentity {
                    player_id: "42".into(),
                    session_id: "rpc-smoke".into(),
                    epoch: 1,
                }),
            },
            Payload {
                message_id: player::GET_PLAYER,
                body: protocol::v1::GetPlayerRequest { player_id: 9999 }.encode_to_vec(),
            },
        )
        .await?;
    let payload = response
        .payload
        .ok_or_else(|| std::io::Error::other("player 回应缺少 payload"))?;
    let body = protocol::v1::GetPlayerResponse::decode(payload.body.as_slice())?;
    if response.request_id != 1
        || payload.message_id != player::GET_PLAYER_RESPONSE
        || body.player_id != 42
    {
        return Err(std::io::Error::other("player 回应与可信上下文不匹配").into());
    }
    let response = clients
        .business_call(
            &guild,
            CallContext {
                node_id: guild.node_id.clone(),
                service_name: guild.service_name.clone(),
                bid: Some("7".into()),
                request_id: 2,
                session: None,
            },
            Payload {
                message_id: guild::GET_GUILD,
                body: protocol::v1::GetGuildRequest { guild_id: 7 }.encode_to_vec(),
            },
        )
        .await?;
    let payload = response
        .payload
        .ok_or_else(|| std::io::Error::other("guild 回应缺少 payload"))?;
    let body = protocol::v1::GetGuildResponse::decode(payload.body.as_slice())?;
    if response.request_id != 2
        || payload.message_id != guild::GET_GUILD_RESPONSE
        || body.guild_id != 7
    {
        return Err(std::io::Error::other("guild 回应不匹配").into());
    }
    Ok(())
}
