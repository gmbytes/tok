//! 真实 TCP 上的认证、玩家串行、容量、会话接管与异常边界集成测试。

use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{
    LogicalMessage, MessageKind, SessionIdentity,
    coordination::{CoordinationTask, OwnershipStore, SessionOwner, SessionOwnership},
};
use tok_gateway::{
    ErrorCode,
    codec::{FrameCodec, VERSION},
    player::{
        AuthenticatedPlayer, AuthenticationTask, Authenticator, PlayerConfig, PlayerExecutor,
        PlayerGateway, PlayerTask, SessionManager,
    },
    protocol::{self, Frame, frame::Content},
};
use tok_infra::ownership::MemoryOwnership;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, credential: String) -> AuthenticationTask<'_> {
        Box::pin(async move {
            if credential == "slow" {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let id = credential
                .parse::<u64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or(ErrorCode::AuthenticationFailed)?;
            Ok(AuthenticatedPlayer {
                player_id: id.to_string(),
            })
        })
    }
}

struct Tracker {
    calls: AtomicUsize,
    total: AtomicUsize,
    peak_total: AtomicUsize,
    players: Mutex<BTreeMap<String, (usize, usize)>>,
    started: mpsc::UnboundedSender<String>,
    permits: Semaphore,
}
struct Inflight(Arc<Tracker>, String);
impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.total.fetch_sub(1, Ordering::SeqCst);
        self.0.players.lock().unwrap().get_mut(&self.1).unwrap().0 -= 1;
    }
}
struct Executor(Arc<Tracker>);
impl PlayerExecutor for Executor {
    fn execute(&self, identity: SessionIdentity, request: LogicalMessage) -> PlayerTask<'_> {
        Box::pin(async move {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            assert_ne!(request.message_id, 666, "测试 session 任务崩溃");
            if request.message_id == 999 {
                return Err(ErrorCode::UnknownMessage.into());
            }
            if request.message_id == 2 {
                return Err(ErrorCode::Unavailable.into());
            }
            let total = self.0.total.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak_total.fetch_max(total, Ordering::SeqCst);
            {
                let mut players = self.0.players.lock().unwrap();
                let counts = players.entry(identity.player_id.clone()).or_default();
                counts.0 += 1;
                counts.1 = counts.1.max(counts.0);
            }
            let _inflight = Inflight(self.0.clone(), identity.player_id.clone());
            self.0.started.send(identity.player_id.clone()).unwrap();
            if request.body == b"block" {
                self.0.permits.acquire().await.unwrap().forget();
            }
            Ok(LogicalMessage {
                kind: MessageKind::Response,
                message_id: 10,
                request_id: request.request_id,
                error_code: 0,
                server_seq: None,
                body: identity.player_id.into_bytes(),
            }
            .into())
        })
    }
}

struct Fixture {
    address: std::net::SocketAddr,
    token: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
    tracker: Arc<Tracker>,
    started: mpsc::UnboundedReceiver<String>,
    sessions: SessionManager,
    ownership: Arc<MemoryOwnership>,
}
impl Fixture {
    async fn new(config: PlayerConfig) -> Self {
        Self::with_backend(config, |memory| memory).await
    }
    async fn with_backend(
        config: PlayerConfig,
        backend: impl FnOnce(Arc<MemoryOwnership>) -> Arc<dyn OwnershipStore>,
    ) -> Self {
        let (started, receiver) = mpsc::unbounded_channel();
        let tracker = Arc::new(Tracker {
            calls: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            peak_total: AtomicUsize::new(0),
            players: Mutex::new(BTreeMap::new()),
            started,
            permits: Semaphore::new(0),
        });
        let ownership = Arc::new(MemoryOwnership::new(config.max_sessions).unwrap());
        let gateway = PlayerGateway::new(
            config,
            SessionOwner {
                node_id: "local".into(),
                service_name: "player-gateway".into(),
            },
            Arc::new(Auth),
            backend(ownership.clone()),
            Arc::new(Executor(tracker.clone())),
        )
        .unwrap();
        let sessions = gateway.sessions();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let token = CancellationToken::new();
        let task = tokio::spawn(gateway.serve(listener, token.clone()));
        Self {
            address,
            token,
            task: Some(task),
            tracker,
            started: receiver,
            sessions,
            ownership,
        }
    }
    async fn connect(&self) -> Client {
        Client::connect(self.address).await
    }
    async fn started(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(2), self.started.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn released(&self, player: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.ownership.lookup(player).await.unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn finish(mut self) {
        self.token.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        TcpListener::bind(self.address).await.unwrap();
    }
}

struct DelayedRenew {
    memory: Arc<MemoryOwnership>,
    delay_once: Arc<AtomicBool>,
    renewals: Arc<AtomicUsize>,
}
impl OwnershipStore for DelayedRenew {
    fn lookup(&self, player: &str) -> CoordinationTask<'_, Option<SessionOwnership>> {
        self.memory.lookup(player)
    }
    fn acquire(
        &self,
        player: String,
        session: String,
        owner: SessionOwner,
        expected: Option<u64>,
        ttl: Duration,
    ) -> CoordinationTask<'_, SessionOwnership> {
        self.memory.acquire(player, session, owner, expected, ttl)
    }
    fn renew(&self, record: SessionOwnership, ttl: Duration) -> CoordinationTask<'_, ()> {
        Box::pin(async move {
            self.memory.renew(record, ttl).await?;
            self.renewals.fetch_add(1, Ordering::SeqCst);
            if self.delay_once.swap(false, Ordering::SeqCst) {
                // 模拟续租已完成后进程暂停；不 yield 的工作不能靠 Tokio timeout 抢占。
                std::thread::sleep(Duration::from_millis(350));
            }
            Ok(())
        })
    }
    fn release(&self, record: SessionOwnership) -> CoordinationTask<'_, bool> {
        self.memory.release(record)
    }
}

#[tokio::test]
async fn known_exhausted_lease_budget_rejects_work_after_process_pause() {
    let delay_once = Arc::new(AtomicBool::new(false));
    let renewals = Arc::new(AtomicUsize::new(0));
    let delay = delay_once.clone();
    let renew_count = renewals.clone();
    let fixture = Fixture::with_backend(
        PlayerConfig {
            rpc_timeout_ms: 20,
            ownership_ttl_ms: 300,
            renew_interval_ms: 100,
            ..Default::default()
        },
        move |memory| {
            Arc::new(DelayedRenew {
                memory,
                delay_once: delay,
                renewals: renew_count,
            })
        },
    )
    .await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    while renewals.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let session = fixture.sessions.session("1").unwrap().unwrap();
    delay_once.store(true, Ordering::SeqCst);
    let response = session
        .try_request(LogicalMessage {
            kind: MessageKind::Request,
            request_id: 2,
            message_id: 1,
            error_code: 0,
            server_seq: None,
            body: Vec::new(),
        })
        .unwrap()
        .await;
    assert_eq!(response.error_code, ErrorCode::SessionClosed as u32);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    assert!(session.cancellation().is_cancelled());
    fixture.finish().await;
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct Client {
    stream: TcpStream,
    codec: FrameCodec,
}
impl Client {
    async fn connect(address: std::net::SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(address).await.unwrap(),
            codec: FrameCodec::new(1_048_576, Duration::from_secs(2)).unwrap(),
        }
    }
    async fn send(&mut self, content: Content) {
        self.codec
            .write(
                &mut self.stream,
                &Frame {
                    version: VERSION,
                    content: Some(content),
                },
            )
            .await
            .unwrap();
    }
    async fn login(&mut self, id: u64, credential: &str) -> protocol::LoginResponse {
        self.send(Content::Login(protocol::LoginRequest {
            request_id: id,
            credential: credential.into(),
        }))
        .await;
        match self.read().await {
            Content::LoggedIn(response) => response,
            other => panic!("登录回应错误：{other:?}"),
        }
    }
    async fn request(&mut self, id: u64, message: u32, body: &[u8]) {
        self.send(Content::Request(protocol::BusinessRequest {
            request_id: id,
            message_id: message,
            body: body.to_vec(),
        }))
        .await;
    }
    async fn read(&mut self) -> Content {
        self.codec
            .read(&mut self.stream)
            .await
            .unwrap()
            .expect("连接不应提前关闭")
            .content
            .unwrap()
    }
    async fn response(&mut self) -> protocol::BusinessResponse {
        match self.read().await {
            Content::Response(response) => response,
            other => panic!("业务回应错误：{other:?}"),
        }
    }
}
fn config() -> PlayerConfig {
    PlayerConfig {
        max_sessions: 8,
        max_connections: 8,
        rpc_timeout_ms: 1000,
        ownership_ttl_ms: 5000,
        renew_interval_ms: 500,
        io_timeout_ms: 3000,
        ..PlayerConfig::default()
    }
}

#[tokio::test]
async fn authentication_errors_and_request_ids_do_not_execute_business() {
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    assert_eq!(
        client.login(1, "invalid").await.error_code,
        ErrorCode::AuthenticationFailed as u32
    );
    client.request(2, 1, b"ok").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::Unauthenticated as u32
    );
    assert_eq!(client.login(3, "1").await.error_code, 0);
    assert_eq!(
        client.login(4, "2").await.error_code,
        ErrorCode::InvalidState as u32
    );
    client.request(4, 1, b"ok").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::InvalidRequest as u32
    );
    client.request(5, 0, b"ok").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::InvalidRequest as u32
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    client.request(6, 999, b"ok").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::UnknownMessage as u32
    );
    client.request(7, 1, b"ok").await;
    let response = client.response().await;
    assert_eq!(response.body, b"1");
    assert_eq!(response.error_code, 0);
    drop(client);
    fixture.released("1").await;
    fixture.finish().await;
}

#[tokio::test]
async fn same_player_is_serial_and_queue_full_is_immediate() {
    let mut fixture = Fixture::new(PlayerConfig {
        queue_capacity: 1,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    client.request(2, 1, b"block").await;
    assert_eq!(fixture.started().await, "1");
    client.request(3, 1, b"block").await;
    client.request(4, 1, b"block").await;
    let rejected = client.response().await;
    assert_eq!(rejected.request_id, 4);
    assert_eq!(rejected.error_code, ErrorCode::QueueFull as u32);
    fixture.tracker.permits.add_permits(1);
    assert_eq!(client.response().await.request_id, 2);
    assert_eq!(fixture.started().await, "1");
    fixture.tracker.permits.add_permits(1);
    assert_eq!(client.response().await.request_id, 3);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.tracker.players.lock().unwrap()["1"].1, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn different_players_execute_concurrently() {
    let mut fixture = Fixture::new(config()).await;
    let mut first = fixture.connect().await;
    let mut second = fixture.connect().await;
    first.login(1, "1").await;
    second.login(1, "2").await;
    first.request(2, 1, b"block").await;
    second.request(2, 1, b"block").await;
    fixture.started().await;
    fixture.started().await;
    assert_eq!(fixture.tracker.peak_total.load(Ordering::SeqCst), 2);
    fixture.tracker.permits.add_permits(2);
    assert_eq!(first.response().await.body, b"1");
    assert_eq!(second.response().await.body, b"2");
    fixture.finish().await;
}

#[tokio::test]
async fn duplicate_login_waits_for_old_call_and_invalidates_old_handle() {
    let mut fixture = Fixture::new(config()).await;
    let mut first = fixture.connect().await;
    let old = first.login(1, "1").await;
    let old_handle = fixture.sessions.session("1").unwrap().unwrap();
    first.request(2, 1, b"block").await;
    fixture.started().await;
    let mut second = fixture.connect().await;
    second
        .send(Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "0001".into(),
        }))
        .await;
    assert!(first.codec.read(&mut first.stream).await.unwrap().is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), second.stream.readable())
            .await
            .is_err()
    );
    fixture.tracker.permits.add_permits(1);
    let Content::LoggedIn(new) = second.read().await else {
        panic!("缺少新登录回应");
    };
    assert_eq!(new.error_code, 0);
    assert!(new.epoch > old.epoch);
    assert_ne!(new.session_id, old.session_id);
    let invalid = LogicalMessage {
        kind: MessageKind::Request,
        request_id: 3,
        message_id: 1,
        body: vec![],
        server_seq: None,
        error_code: 0,
    };
    assert_eq!(
        old_handle.submit(invalid).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    second.request(2, 1, b"ok").await;
    assert_eq!(second.response().await.body, b"1");
    assert_eq!(fixture.tracker.players.lock().unwrap()["1"].1, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn closed_sessions_are_reclaimed_and_foreign_ownership_is_not_taken() {
    let fixture = Fixture::new(PlayerConfig {
        max_sessions: 1,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    first.login(1, "1").await;
    let mut second = fixture.connect().await;
    assert_eq!(
        second.login(1, "2").await.error_code,
        ErrorCode::CapacityExceeded as u32
    );
    drop(first);
    fixture.released("1").await;
    assert_eq!(second.login(2, "2").await.error_code, 0);
    let owned = fixture.ownership.lookup("2").await.unwrap().unwrap();
    let foreign = fixture
        .ownership
        .acquire(
            "2".into(),
            "foreign".into(),
            SessionOwner {
                node_id: "other".into(),
                service_name: "gateway".into(),
            },
            Some(owned.identity.epoch),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    second.request(3, 1, b"ok").await;
    let _ = second.codec.read(&mut second.stream).await;
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    let mut third = fixture.connect().await;
    assert_eq!(
        third.login(1, "2").await.error_code,
        ErrorCode::Unavailable as u32
    );
    assert_eq!(fixture.ownership.lookup("2").await.unwrap(), Some(foreign));
    fixture.finish().await;
}

#[tokio::test]
async fn timeouts_do_not_retry_and_session_remains_usable() {
    let fixture = Fixture::new(PlayerConfig {
        rpc_timeout_ms: 30,
        auth_timeout_ms: 30,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    assert_eq!(
        client.login(1, "slow").await.error_code,
        ErrorCode::Timeout as u32
    );
    assert_eq!(client.login(2, "1").await.error_code, 0);
    client.request(3, 1, b"block").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::Timeout as u32
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 1);
    client.request(4, 2, b"ok").await;
    assert_eq!(
        client.response().await.error_code,
        ErrorCode::Unavailable as u32
    );
    client.request(5, 1, b"ok").await;
    assert_eq!(client.response().await.error_code, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn notify_uses_bounded_outbound_without_executing_player() {
    let fixture = Fixture::new(PlayerConfig {
        outbound_capacity: 1,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    client.login(1, "1").await;
    let handle = fixture.sessions.session("1").unwrap().unwrap();
    let notification = LogicalMessage {
        kind: MessageKind::Notify,
        message_id: 90,
        request_id: 0,
        server_seq: None,
        error_code: 0,
        body: vec![9],
    };
    handle.notify(notification.clone()).unwrap();
    let Content::Notify(message) = client.read().await else {
        panic!("缺少通知");
    };
    assert_eq!(message.body, vec![9]);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    handle.notify(notification.clone()).unwrap();
    assert!(handle.notify(notification).is_err());
    let _ = client.codec.read(&mut client.stream).await;
    fixture.released("1").await;
    fixture.finish().await;
}

#[tokio::test]
async fn malformed_frames_are_rejected_before_business_execution() {
    let fixture = Fixture::new(PlayerConfig {
        max_frame_size: 256,
        ..config()
    })
    .await;
    for (bytes, expected) in [
        (257u32.to_be_bytes().to_vec(), ErrorCode::FrameTooLarge),
        (vec![0, 0, 0, 0], ErrorCode::InvalidRequest),
        (vec![0, 0, 0, 1, 0xff], ErrorCode::InvalidRequest),
    ] {
        let mut client = fixture.connect().await;
        client.stream.write_all(&bytes).await.unwrap();
        let Content::Error(error) = client.read().await else {
            panic!("缺少控制错误");
        };
        assert_eq!(error.error_code, expected as u32);
        assert!(
            client
                .codec
                .read(&mut client.stream)
                .await
                .unwrap()
                .is_none()
        );
    }
    let mut client = fixture.connect().await;
    let body = Frame {
        version: 99,
        content: Some(Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "1".into(),
        })),
    }
    .encode_to_vec();
    client
        .stream
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    client.stream.write_all(&body).await.unwrap();
    let Content::Error(error) = client.read().await else {
        panic!("缺少版本错误");
    };
    assert_eq!(error.error_code, ErrorCode::InvalidRequest as u32);
    let mut partial = fixture.connect().await;
    partial.stream.write_all(&[0, 0]).await.unwrap();
    partial.stream.shutdown().await.unwrap();
    let Content::Error(error) = partial.read().await else {
        panic!("缺少半帧错误");
    };
    assert_eq!(error.error_code, ErrorCode::InvalidRequest as u32);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn frame_deadline_and_connection_capacity_are_bounded() {
    let fixture = Fixture::new(PlayerConfig {
        io_timeout_ms: 50,
        max_connections: 1,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    first.stream.write_all(&[0]).await.unwrap();
    let mut second = fixture.connect().await;
    let refused = second.codec.read(&mut second.stream).await;
    assert!(matches!(refused, Ok(None) | Err(_)));
    let Content::Error(error) = first.read().await else {
        panic!("缺少帧期限错误");
    };
    assert_eq!(error.error_code, ErrorCode::Timeout as u32);
    fixture.finish().await;
}

#[tokio::test]
async fn shutdown_waits_for_inflight_work_and_releases_ownership() {
    let mut fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    client.login(1, "1").await;
    client.request(2, 1, b"block").await;
    fixture.started().await;
    fixture.token.cancel();
    assert!(
        client
            .codec
            .read(&mut client.stream)
            .await
            .unwrap()
            .is_none()
    );
    assert!(fixture.ownership.lookup("1").await.unwrap().is_some());
    fixture.tracker.permits.add_permits(1);
    fixture.released("1").await;
    fixture.finish().await;
}

#[tokio::test]
async fn session_panic_stops_gateway_and_cleans_up_owned_records() {
    let mut fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    client.login(1, "1").await;
    client.request(2, 666, b"ok").await;
    assert!(
        client
            .codec
            .read(&mut client.stream)
            .await
            .unwrap()
            .is_none()
    );
    let result = tokio::time::timeout(Duration::from_secs(5), fixture.task.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(fixture.ownership.lookup("1").await.unwrap().is_none());
    TcpListener::bind(fixture.address).await.unwrap();
}
