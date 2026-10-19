//! 真实 Redis 与独立 OS 进程：网关经发现调用游戏，退出、崩溃与重启均可收敛。

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tok::{
    core::ServiceDirectory,
    infra::discovery::redis::{RedisDirectory, RedisDiscoveryConfig},
};
use tokio_util::sync::CancellationToken;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Files(PathBuf);
impl Drop for Files {
    fn drop(&mut self) {
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
        "node_id='{node}'\ndiscovery='redis'\n[redis_discovery]\nurl='{url}'\nnamespace='{namespace}'\nlease_ttl_ms=1800\nrefresh_interval_ms=100\noperation_timeout_ms=500\n[[endpoints]]\nname='network'\nprotocol='{}'\naddress='0.0.0.0:{port}'\nadvertise_address='127.0.0.1:{port}'\n",
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
    let watcher = observer.clone();
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
}
