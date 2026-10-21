//! 直接本地调用与 RPC 服务共用的公会消息分发表，不启动网络或后台服务。
#![deny(clippy::unwrap_used, clippy::expect_used)]

use demo_game::{
    guild,
    protocol::{
        GameMessage,
        v1::{GetGuildRequest, GetGuildResponse},
    },
};
use prost::Message as _;
use std::time::Duration;
use tok::{
    core::Identity,
    execution::{Context, Request},
    lifecycle::task::BoxError,
    protobuf::Message,
};
use tokio_util::sync::CancellationToken;
use tower::{ServiceBuilder, ServiceExt as _};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let registry = guild::handlers()?;
    let context = Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "guild".into(),
        },
        "guild.get",
        Duration::from_secs(1),
        CancellationToken::new(),
    )?;
    // 公共策略包住整个路由表，注册路由内部仍可安装自己的 Layer。
    let service = ServiceBuilder::new()
        .map_request(|request: Request<(u64, Message<u32>)>| {
            assert_eq!(request.context.service().service_name, "guild");
            request
        })
        .service(registry);
    let reply = service
        .oneshot(Request::new(
            context,
            (
                42,
                Message {
                    key: GetGuildRequest::MESSAGE_ID,
                    body: GetGuildRequest { guild_id: 42 }.encode_to_vec(),
                },
            ),
        ))
        .await?;
    assert_eq!(reply.output.message.key, GetGuildResponse::MESSAGE_ID);
    assert_eq!(
        GetGuildResponse::decode(reply.output.message.body.as_slice())?.guild_id,
        42
    );
    println!("本地公会分发验证通过；处理链与 RPC 公会入口共用");
    Ok(())
}
