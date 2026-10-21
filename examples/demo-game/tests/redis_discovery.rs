//! 真实 Redis 与独立 OS 进程：网关经发现调用游戏，退出、崩溃与重启均可收敛。

use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tok::{
    core::ServiceDirectory,
    gateway::{
        ErrorCode,
        codec::{FrameCodec, VERSION},
        protocol::{Frame, LoginRequest, LoginResponse, frame::Content},
    },
    infra::{
        discovery::redis::{RedisDirectory, RedisDiscoveryConfig},
        ownership::redis::{RedisOwnership, RedisOwnershipConfig},
    },
    player::ownership::{Owner as SessionOwner, Store as OwnershipStore},
};
use tokio_util::sync::CancellationToken;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "测试析构时子进程可能已退出，终止失败不能引起二次 panic。"
        )]
        let _ = self.0.kill();
        #[expect(
            clippy::let_underscore_must_use,
            reason = "测试析构时子进程可能已回收，等待失败不能引起二次 panic。"
        )]
        let _ = self.0.wait();
    }
}
struct Files(PathBuf);
impl Drop for Files {
    fn drop(&mut self) {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "测试析构仅尽力清理自身临时目录，保留原始测试失败。"
        )]
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn config(path: &Path, namespace: &str, url: &str, node: &str, port: u16, gateway: bool) {
    // URL 从环境读取，使用 TOML 的单引号字面量且不回显凭据。
    assert!(!url.contains(['\'', '\n', '\r']));
    let mut source = format!(
        "node_id='{node}'\ndiscovery='redis'\nownership='{}'\n[redis_discovery]\nurl='{url}'\nnamespace='{namespace}'\nlease_ttl_ms=1800\nrefresh_interval_ms=100\noperation_timeout_ms=500\n[[endpoints]]\nname='network'\nprotocol='{}'\naddress='0.0.0.0:{port}'\nadvertise_address='127.0.0.1:{port}'\n",
        if gateway { "redis" } else { "memory" },
        if gateway { "tcp" } else { "grpc" }
    );
    for service in if gateway {
        vec!["player-gateway"]
    } else {
        vec!["player", "guild"]
    } {
        source.push_str(&format!(
            "[[services]]\nservice_name='{service}'\nendpoint='network'\n"
        ));
        if gateway {
            source.push_str("factory='player-ingress'\n");
        }
    }
    if gateway {
        source.push_str(&format!("[redis_ownership]\nurl='{url}'\nnamespace='{namespace}'\noperation_timeout_ms=300\n[player_gateway]\nrpc_timeout_ms=500\nownership_ttl_ms=1800\nrenew_interval_ms=200\ndisconnect_grace_ms=250\n"));
    }
    std::fs::write(path, source).unwrap();
}
fn spawn(path: &Path, arguments: &[&str]) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_demo-game"))
            .arg("--config")
            .arg(path)
            .args(arguments)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
async fn wait_exit(process: &mut Process) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success(), "子进程退出失败：{status}");
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("子进程未按期退出");
}
async fn wait_nodes(directory: &RedisDirectory, port: Option<u16>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if directory.snapshot("player").is_ok_and(|nodes| match port {
                Some(port) => nodes.len() == 1 && nodes[0].address.port() == port,
                None => nodes.is_empty(),
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("跨进程目录未收敛");
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just redis-smoke 显式运行"]
async fn two_process_client_smoke_graceful_withdraw_crash_and_restart() {
    let namespace = format!("process-{}", uuid::Uuid::new_v4());
    let url =
        std::env::var("TOK_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let files = Files(std::env::temp_dir().join(&namespace));
    std::fs::create_dir(&files.0).unwrap();
    let game_file = files.0.join("game.toml");
    let gateway_file = files.0.join("gateway.toml");
    let game_port = port();
    config(&game_file, &namespace, &url, "game", game_port, false);
    config(&gateway_file, &namespace, &url, "gateway", port(), true);
    let observer = RedisDirectory::new(
        RedisDiscoveryConfig {
            url: url.clone(),
            namespace: namespace.clone(),
            lease_ttl_ms: 1800,
            refresh_interval_ms: 100,
            operation_timeout_ms: 500,
            ..Default::default()
        },
        "observer",
    )
    .unwrap();
    observer.initialize().await.unwrap();
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let watcher = Arc::clone(&observer);
    let signal = stop.clone();
    tasks.spawn(async move { watcher.run(signal).await });
    let mut game = spawn(&game_file, &["--run-for", "8"]);
    wait_nodes(&observer, Some(game_port)).await;
    let mut gateway = spawn(&gateway_file, &["--client-smoke"]);
    wait_exit(&mut gateway).await;
    wait_exit(&mut game).await;
    wait_nodes(&observer, None).await;
    let restarted_port = port();
    config(&game_file, &namespace, &url, "game", restarted_port, false);
    let mut restarted = spawn(&game_file, &["--run-for", "60"]);
    wait_nodes(&observer, Some(restarted_port)).await;
    restarted.0.kill().unwrap();
    restarted.0.wait().unwrap();
    wait_nodes(&observer, None).await;
    stop.cancel();
    tasks.join_next().await.unwrap().unwrap().unwrap();
    clear_epoch(&url, &namespace).await;
}

async fn clear_epoch(url: &str, namespace: &str) {
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    // 仅清理本次独立 namespace 中的测试玩家 42（十六进制 3432）。
    redis::cmd("DEL")
        .arg(format!("tok:ownership:{{{namespace}:3432}}:lease"))
        .arg(format!("tok:ownership:{{{namespace}:3432}}:epoch"))
        .arg(format!("tok:ownership:{{{namespace}:3432}}:session"))
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
}
async fn login(port: u16) -> (tokio::net::TcpStream, LoginResponse) {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let codec = FrameCodec::new(1_048_576, Duration::from_secs(3)).unwrap();
    codec
        .write(
            &mut stream,
            &Frame {
                version: VERSION,
                content: Some(Content::Login(LoginRequest {
                    request_id: 1,
                    credential: "demo:42".into(),
                    resume_token: String::new(),
                })),
            },
        )
        .await
        .unwrap();
    let Some(Content::LoggedIn(response)) = codec.read(&mut stream).await.unwrap().unwrap().content
    else {
        panic!("缺少登录回应")
    };
    (stream, response)
}
async fn wait_release(store: &RedisOwnership) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.lookup("42").await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("所有权未按期释放或过期");
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just handoff-smoke 显式运行"]
async fn three_process_handoff_preserves_replay_and_crash_falls_back_to_fast_login() {
    use demo_game::protocol::{GameMessage, v1};
    use prost::Message;
    use tok::gateway::protocol::{BusinessRequest, ResumeRequest};
    let namespace = format!("handoff-{}", uuid::Uuid::new_v4());
    let url =
        std::env::var("TOK_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let files = Files(std::env::temp_dir().join(&namespace));
    std::fs::create_dir(&files.0).unwrap();
    let game_file = files.0.join("game.toml");
    let a_file = files.0.join("a.toml");
    let b_file = files.0.join("b.toml");
    let game_port = port();
    let a_port = port();
    let b_port = port();
    config(&game_file, &namespace, &url, "game", game_port, false);
    for (file, node, client_port) in [
        (&a_file, "gateway-a", a_port),
        (&b_file, "gateway-b", b_port),
    ] {
        config(file, &namespace, &url, node, client_port, true);
        let mut source = std::fs::read_to_string(file)
            .unwrap()
            .replace(
                "[player_gateway]\n",
                "[player_gateway]\nhandoff_enabled=true\nhandoff_timeout_ms=5000\n",
            )
            .replace("disconnect_grace_ms=250", "disconnect_grace_ms=2000");
        source.push_str(&format!("\n[resume_token]\nsecret_key='1111111111111111111111111111111111111111111111111111111111111111'\n[[endpoints]]\nname='internal'\nprotocol='grpc'\naddress='127.0.0.1:{}'\n[[services]]\nservice_name='gateway-rpc'\nendpoint='internal'\n", port()));
        std::fs::write(file, source).unwrap();
    }
    let observer = RedisDirectory::new(
        RedisDiscoveryConfig {
            url: url.clone(),
            namespace: namespace.clone(),
            lease_ttl_ms: 1800,
            refresh_interval_ms: 100,
            operation_timeout_ms: 500,
            ..Default::default()
        },
        "observer",
    )
    .unwrap();
    observer.initialize().await.unwrap();
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let watcher = Arc::clone(&observer);
    let signal = stop.clone();
    tasks.spawn(async move { watcher.run(signal).await });
    let _game = spawn(&game_file, &["--run-for", "60"]);
    let _a = spawn(&a_file, &["--run-for", "60"]);
    let mut b = spawn(&b_file, &["--run-for", "60"]);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !observer
            .snapshot("gateway-rpc")
            .is_ok_and(|nodes| nodes.len() == 2)
            || !observer
                .snapshot("player")
                .is_ok_and(|nodes| nodes.len() == 1)
        {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    // 各网关本地发现缓存也需要一次同步；通过真实请求确认业务已可用。
    tokio::time::sleep(Duration::from_millis(150)).await;
    let store = RedisOwnership::new(RedisOwnershipConfig {
        url: url.clone(),
        namespace: namespace.clone(),
        operation_timeout_ms: 500,
    })
    .unwrap();
    let (mut original, login) = login(a_port).await;
    assert_eq!(login.error_code, 0);
    let codec = FrameCodec::new(1048576, Duration::from_secs(6)).unwrap();
    let mut responses = Vec::new();
    for (id, message_id, body) in [
        (
            2,
            v1::SetPlayerRequest::MESSAGE_ID,
            v1::SetPlayerRequest { value: 99 }.encode_to_vec(),
        ),
        (
            3,
            v1::StartProductionRequest::MESSAGE_ID,
            v1::StartProductionRequest {}.encode_to_vec(),
        ),
    ] {
        codec
            .write(
                &mut original,
                &Frame {
                    version: VERSION,
                    content: Some(Content::Request(BusinessRequest {
                        request_id: id,
                        message_id,
                        body,
                    })),
                },
            )
            .await
            .unwrap();
        let frame = codec.read(&mut original).await.unwrap().unwrap();
        assert!(
            matches!(&frame.content, Some(Content::Response(response)) if response.error_code == 0)
        );
        responses.push(frame);
    }
    let mut target = tokio::net::TcpStream::connect(("127.0.0.1", b_port))
        .await
        .unwrap();
    codec
        .write(
            &mut target,
            &Frame {
                version: VERSION,
                content: Some(Content::Resume(ResumeRequest {
                    request_id: 1,
                    resume_token: login.resume_token.clone(),
                    ack_seq: 0,
                })),
            },
        )
        .await
        .unwrap();
    let Some(Content::Resumed(resumed)) = codec.read(&mut target).await.unwrap().unwrap().content
    else {
        panic!("缺少恢复回应")
    };
    assert_eq!(resumed.error_code, 0);
    assert_eq!(resumed.session_id, login.session_id);
    assert!(resumed.epoch > login.epoch);
    assert_eq!(resumed.last_request_id, 3);
    for original in responses {
        assert_eq!(codec.read(&mut target).await.unwrap().unwrap(), original);
    }
    assert!(matches!(
        codec.read(&mut target).await.unwrap().unwrap().content,
        Some(Content::Recovered(_))
    ));
    assert_eq!(
        store.lookup("42").await.unwrap().unwrap().owner.node_id,
        "gateway-b"
    );
    assert!(codec.read(&mut original).await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(120)).await;
    codec
        .write(
            &mut target,
            &Frame {
                version: VERSION,
                content: Some(Content::Request(BusinessRequest {
                    request_id: 4,
                    message_id: v1::GetPlayerRequest::MESSAGE_ID,
                    body: v1::GetPlayerRequest { player_id: 0 }.encode_to_vec(),
                })),
            },
        )
        .await
        .unwrap();
    let Some(Content::Response(response)) = codec.read(&mut target).await.unwrap().unwrap().content
    else {
        panic!("缺少业务回应")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.server_seq, 3);
    assert_eq!(
        v1::GetPlayerResponse::decode(response.body.as_slice())
            .unwrap()
            .value,
        100
    );
    // 目标网关刷新后，源网关可独立验证新凭证并用于宕机降级。
    codec
        .write(
            &mut target,
            &Frame {
                version: VERSION,
                content: Some(Content::RefreshResumeToken(
                    tok::gateway::protocol::RefreshResumeTokenRequest { request_id: 5 },
                )),
            },
        )
        .await
        .unwrap();
    let Some(Content::ResumeTokenRefreshed(refreshed)) =
        codec.read(&mut target).await.unwrap().unwrap().content
    else {
        panic!("缺少凭证刷新回应")
    };
    assert_eq!(refreshed.error_code, 0);
    assert!(!refreshed.resume_token.is_empty());
    b.0.kill().unwrap();
    b.0.wait().unwrap();
    let mut fallback = tokio::net::TcpStream::connect(("127.0.0.1", a_port))
        .await
        .unwrap();
    codec
        .write(
            &mut fallback,
            &Frame {
                version: VERSION,
                content: Some(Content::Resume(ResumeRequest {
                    request_id: 1,
                    resume_token: refreshed.resume_token.clone(),
                    ack_seq: 0,
                })),
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(codec.read(&mut fallback).await.unwrap().unwrap().content, Some(Content::Resumed(response)) if response.error_code == ErrorCode::ResumeUnavailable as u32)
    );
    wait_release(&store).await;
    codec
        .write(
            &mut fallback,
            &Frame {
                version: VERSION,
                content: Some(Content::Login(LoginRequest {
                    request_id: 2,
                    credential: String::new(),
                    resume_token: refreshed.resume_token.clone(),
                })),
            },
        )
        .await
        .unwrap();
    let Some(Content::LoggedIn(next)) = codec.read(&mut fallback).await.unwrap().unwrap().content
    else {
        panic!("缺少快速重登回应")
    };
    assert_eq!(next.error_code, 0);
    assert_ne!(next.session_id, login.session_id);
    assert!(next.epoch > resumed.epoch);
    drop(fallback);
    wait_release(&store).await;
    stop.cancel();
    tasks.join_next().await.unwrap().unwrap().unwrap();
    clear_epoch(&url, &namespace).await;
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-smoke 显式运行"]
async fn independent_gateway_processes_share_ownership_and_stop_on_lease_loss() {
    let namespace = format!("gateways-{}", uuid::Uuid::new_v4());
    let url =
        std::env::var("TOK_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let files = Files(std::env::temp_dir().join(&namespace));
    std::fs::create_dir(&files.0).unwrap();
    let game_file = files.0.join("game.toml");
    let a_file = files.0.join("a.toml");
    let b_file = files.0.join("b.toml");
    let game_port = port();
    let a_port = port();
    let b_port = port();
    config(&game_file, &namespace, &url, "game", game_port, false);
    config(&a_file, &namespace, &url, "gateway-a", a_port, true);
    config(&b_file, &namespace, &url, "gateway-b", b_port, true);
    let observer = RedisDirectory::new(
        RedisDiscoveryConfig {
            url: url.clone(),
            namespace: namespace.clone(),
            lease_ttl_ms: 1800,
            refresh_interval_ms: 100,
            operation_timeout_ms: 500,
            ..Default::default()
        },
        "observer",
    )
    .unwrap();
    observer.initialize().await.unwrap();
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let watcher = Arc::clone(&observer);
    let signal = stop.clone();
    tasks.spawn(async move { watcher.run(signal).await });
    let _game = spawn(&game_file, &["--run-for", "60"]);
    let _a = spawn(&a_file, &["--run-for", "60"]);
    let mut b = spawn(&b_file, &["--run-for", "60"]);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !observer
            .snapshot("player-gateway")
            .is_ok_and(|nodes| nodes.len() == 2)
        {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let store = RedisOwnership::new(RedisOwnershipConfig {
        url: url.clone(),
        namespace: namespace.clone(),
        operation_timeout_ms: 500,
    })
    .unwrap();
    let (a_stream, a_login) = login(a_port).await;
    assert_eq!(a_login.error_code, 0);
    assert_eq!(
        store.lookup("42").await.unwrap().unwrap().owner.node_id,
        "gateway-a"
    );
    assert_eq!(
        login(b_port).await.1.error_code,
        ErrorCode::Unavailable as u32
    );
    drop(a_stream);
    wait_release(&store).await;
    let (mut b_stream, b_login) = login(b_port).await;
    assert_eq!(b_login.error_code, 0);
    assert!(b_login.epoch > a_login.epoch);
    // 后端契约允许显式 CAS 接管；网关 B 必须因后续续租冲突而关闭入口。
    let external = store
        .acquire(
            "42".into(),
            "external-session".into(),
            SessionOwner {
                node_id: "external".into(),
                service_name: "gateway".into(),
            },
            Some(b_login.epoch),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let codec = FrameCodec::new(1_048_576, Duration::from_secs(3)).unwrap();
    assert!(codec.read(&mut b_stream).await.unwrap().is_none());
    assert_eq!(store.lookup("42").await.unwrap(), Some(external.clone()));
    store.release(external).await.unwrap();
    let (_b_stream, next) = login(b_port).await;
    assert_eq!(next.error_code, 0);
    b.0.kill().unwrap();
    b.0.wait().unwrap();
    assert_eq!(
        login(a_port).await.1.error_code,
        ErrorCode::Unavailable as u32
    );
    wait_release(&store).await;
    let (stream, after_crash) = login(a_port).await;
    assert_eq!(after_crash.error_code, 0);
    assert!(after_crash.epoch > next.epoch);
    drop(stream);
    wait_release(&store).await;
    stop.cancel();
    tasks.join_next().await.unwrap().unwrap().unwrap();
    clear_epoch(&url, &namespace).await;
}
