//! 通过真实客户端与内部 RPC 验证示例业务和客户端错误映射。

use demo_game::protocol::GameMessage;
use std::time::Duration;
use tok::{
    AppConfig,
    core::ServiceDirectory,
    gateway::{
        ErrorCode,
        codec::{FrameCodec, VERSION},
        protocol::{self, Frame, frame::Content},
    },
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[test]
fn demo_rejects_unavailable_unknown_or_incompatible_factories() {
    let local = include_str!("../config/local.toml");
    for source in [
        local.replace(
            "factory = \"player-ingress\"",
            "factory = \"scene-ingress\"",
        ),
        local.replace("factory = \"player-ingress\"", "factory = \"unknown\""),
        local.replace("factory = \"player-ingress\"", "factory = \"player\""),
        local.replace("endpoint = \"internal-rpc\"", "endpoint = \"client\""),
    ] {
        let config = AppConfig::from_toml(&source).unwrap();
        assert!(demo_game::application(config).is_err());
    }
}

#[tokio::test]
async fn client_to_rpc_reads_writes_queries_guild_and_maps_errors() {
    let client_reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_address = client_reservation.local_addr().unwrap();
    let rpc_address = rpc_reservation.local_addr().unwrap();
    let config = AppConfig::from_toml(&format!("node_id='local'\ndiscovery='memory'\n[[endpoints]]\nname='client'\nprotocol='tcp'\naddress='{client_address}'\n[[endpoints]]\nname='internal'\nprotocol='grpc'\naddress='{rpc_address}'\n[[services]]\nservice_name='custom-entry'\nfactory='player-ingress'\nendpoint='client'\n[[services]]\nservice_name='player'\nendpoint='internal'\n[[services]]\nservice_name='guild'\nendpoint='internal'")).unwrap();
    let app = demo_game::application(config).unwrap();
    let directory = app.directory();
    let stop = CancellationToken::new();
    let signal = stop.clone();
    drop(client_reservation);
    drop(rpc_reservation);
    let task = tokio::spawn(app.run_until(async move {
        signal.cancelled().await;
        Ok(())
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        while directory.snapshot("guild").unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    demo_game::client::smoke(client_address).await.unwrap();
    assert_eq!(directory.snapshot("custom-entry").unwrap().len(), 1);
    let codec = FrameCodec::new(1_048_576, Duration::from_secs(2)).unwrap();
    let mut stream = TcpStream::connect(client_address).await.unwrap();
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Login(protocol::LoginRequest {
                    request_id: 1,
                    credential: "demo:00042".into(),
                })),
            },
        )
        .await
        .unwrap();
    let Some(Content::LoggedIn(login)) = codec.read(&mut stream).await.unwrap().unwrap().content
    else {
        panic!("缺少登录回应");
    };
    assert_eq!(login.player_id, "42");
    // 模拟发现快照失去 guild，player 的跨服务错误仍映射为业务请求回应。
    directory.unregister("local", "guild").unwrap();
    for (id, message, body, code) in [
        (
            2,
            demo_game::protocol::v1::GetPlayerRequest::MESSAGE_ID,
            vec![0xff],
            ErrorCode::InvalidRequest,
        ),
        (3, 999, Vec::new(), ErrorCode::UnknownMessage),
        (
            4,
            demo_game::protocol::v1::ProductionFinished::MESSAGE_ID,
            Vec::new(),
            ErrorCode::UnknownMessage,
        ),
        (
            5,
            demo_game::protocol::v1::QueryGuildRequest::MESSAGE_ID,
            Vec::new(),
            ErrorCode::Unavailable,
        ),
    ] {
        codec
            .write(
                &mut stream,
                &Frame {
                    version: VERSION,
                    content: Some(Content::Request(protocol::BusinessRequest {
                        request_id: id,
                        message_id: message,
                        body,
                    })),
                },
            )
            .await
            .unwrap();
        let Some(Content::Response(response)) =
            codec.read(&mut stream).await.unwrap().unwrap().content
        else {
            panic!("缺少错误回应");
        };
        assert_eq!(response.request_id, id);
        assert_eq!(response.error_code, code as u32);
    }
    drop(stream);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(directory.snapshot("player").unwrap().is_empty());
    TcpListener::bind(client_address).await.unwrap();
    TcpListener::bind(rpc_address).await.unwrap();
}
