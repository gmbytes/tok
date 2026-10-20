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
    LogicalMessage, MessageKind,
    coordination::{CoordinationTask, OwnershipStore, SessionOwner, SessionOwnership},
};
use tok_gateway::{
    ErrorCode, Gateway,
    codec::{FrameCodec, VERSION},
    player::{
        AuthenticatedIdentity, AuthenticationTask, Authenticator, Config, Executor, SessionManager,
        Task,
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
            Ok(AuthenticatedIdentity {
                player_id: id.to_string(),
            })
        })
    }
}

struct Tracker {
    events: Mutex<Vec<u64>>,
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
struct TestExecutor(Arc<Tracker>);
impl Executor for TestExecutor {
    fn execute_event(
        &self,
        input: tok_service::Request<tok_gateway::player::EventInput>,
    ) -> tok_gateway::player::EventTask<'_> {
        let context = input.context;
        let tok_gateway::player::EventInput {
            identity,
            request_id,
            event,
        } = input.input;
        Box::pin(async move {
            self.0.events.lock().unwrap().push(request_id);
            let reply = self
                .execute_request(tok_service::Request::new(
                    context,
                    tok_gateway::player::RequestInput {
                        identity,
                        message: LogicalMessage {
                            kind: MessageKind::Request,
                            message_id: 1,
                            request_id,
                            server_seq: None,
                            error_code: 0,
                            body: event.body.clone(),
                        },
                    },
                ))
                .await?;
            Ok(reply.timers)
        })
    }

    fn execute_request(
        &self,
        input: tok_service::Request<tok_gateway::player::RequestInput>,
    ) -> Task<'_> {
        let tok_gateway::player::RequestInput {
            identity,
            message: request,
        } = input.input;
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
            let _inflight = Inflight(Arc::clone(&self.0), identity.player_id.clone());
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
                body: if request.message_id == 3 {
                    vec![7; 1024]
                } else {
                    identity.player_id.into_bytes()
                },
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
    async fn new(config: Config) -> Self {
        Self::with_backend(config, |memory| memory).await
    }
    async fn with_backend(
        config: Config,
        backend: impl FnOnce(Arc<MemoryOwnership>) -> Arc<dyn OwnershipStore>,
    ) -> Self {
        Self::with_backend_and_events(config, backend, None).await
    }
    async fn with_backend_and_events(
        config: Config,
        backend: impl FnOnce(Arc<MemoryOwnership>) -> Arc<dyn OwnershipStore>,
        events: Option<&tok_events::nats::Bus>,
    ) -> Self {
        let (started, receiver) = mpsc::unbounded_channel();
        let tracker = Arc::new(Tracker {
            events: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            peak_total: AtomicUsize::new(0),
            players: Mutex::new(BTreeMap::new()),
            started,
            permits: Semaphore::new(0),
        });
        let ownership = Arc::new(MemoryOwnership::new(config.max_sessions).unwrap());
        let mut limits = config.gateway_limits();
        if events.is_some() {
            limits.max_tasks += 1;
        }
        let gateway = Gateway::new(tok_core_identity(), limits, |context| {
            tok_gateway::player::Ingress::new(
                context,
                config,
                SessionOwner {
                    node_id: "local".into(),
                    service_name: "player-gateway".into(),
                },
                Arc::new(Auth),
                backend(Arc::clone(&ownership)),
                Arc::new(TestExecutor(Arc::clone(&tracker))),
            )
        })
        .unwrap();
        if let Some(events) = events {
            let subscription = events
                .subscribe(
                    tok_events::Subscriber::new(
                        tok_core::service::Identity {
                            node_id: "test".into(),
                            service_name: "events".into(),
                        },
                        Arc::new(game_subscriber(gateway.ingress().sessions())),
                        None,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let tasks = gateway.tasks();
            let stop = tasks.cancellation();
            tasks
                .launch(
                    "event-subscription",
                    Box::pin(async move {
                        subscription.run(stop).await?;
                        Ok(())
                    }),
                )
                .await
                .unwrap();
        }
        let sessions = gateway.ingress().sessions();
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
    let delay = Arc::clone(&delay_once);
    let renew_count = Arc::clone(&renewals);
    let fixture = Fixture::with_backend(
        Config {
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
            resume_token: String::new(),
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
    async fn resume(&mut self, id: u64, token: &str, ack_seq: u64) -> protocol::ResumeResponse {
        self.send(Content::Resume(protocol::ResumeRequest {
            request_id: id,
            resume_token: token.into(),
            ack_seq,
        }))
        .await;
        let Content::Resumed(response) = self.read().await else {
            panic!("缺少恢复回应");
        };
        response
    }
    async fn refresh(&mut self, id: u64) -> protocol::RefreshResumeTokenResponse {
        self.send(Content::RefreshResumeToken(
            protocol::RefreshResumeTokenRequest { request_id: id },
        ))
        .await;
        let Content::ResumeTokenRefreshed(response) = self.read().await else {
            panic!("缺少凭证刷新回应");
        };
        response
    }
    async fn fast_login(&mut self, id: u64, token: &str) -> protocol::LoginResponse {
        self.send(Content::Login(protocol::LoginRequest {
            request_id: id,
            credential: String::new(),
            resume_token: token.into(),
        }))
        .await;
        let Content::LoggedIn(response) = self.read().await else {
            panic!("缺少快速重登回应");
        };
        response
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
fn config() -> Config {
    Config {
        disconnect_grace_ms: 0,
        max_sessions: 8,
        max_connections: 8,
        rpc_timeout_ms: 1000,
        ownership_ttl_ms: 5000,
        renew_interval_ms: 500,
        io_timeout_ms: 3000,
        ..Config::default()
    }
}

#[tokio::test]
async fn reconnect_replays_inflight_result_without_reexecution_and_preserves_request_progress() {
    let mut fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    assert!(!login.resume_token.is_empty());
    first.request(2, 1, b"block").await;
    fixture.started().await;
    let handle = fixture.sessions.session("1").unwrap().unwrap();
    drop(first);
    handle
        .notify(LogicalMessage {
            kind: MessageKind::Notify,
            message_id: 90,
            request_id: 0,
            server_seq: None,
            error_code: 0,
            body: vec![9],
        })
        .await
        .unwrap();
    fixture.tracker.permits.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.replay_after(0).unwrap().unwrap().through_seq != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.ownership.lookup("1").await.unwrap().is_some());
    let mut second = fixture.connect().await;
    let resumed = second.resume(1, &login.resume_token, 0).await;
    assert_eq!(resumed.error_code, 0);
    assert_eq!(resumed.session_id, login.session_id);
    assert_eq!(resumed.epoch, login.epoch);
    assert_eq!(resumed.last_request_id, 2);
    let Content::Notify(notify) = second.read().await else {
        panic!()
    };
    assert_eq!(notify.server_seq, 1);
    let response = second.response().await;
    assert_eq!(response.server_seq, 2);
    assert_eq!(response.request_id, 2);
    assert_eq!(response.body, b"1");
    let Content::Recovered(done) = second.read().await else {
        panic!("缺少恢复完成标记");
    };
    assert_eq!(done.through_seq, 2);
    second
        .send(Content::Ack(protocol::Ack { ack_seq: 2 }))
        .await;
    second.request(2, 1, b"ok").await;
    let repeated = second.response().await;
    assert_eq!(repeated.error_code, ErrorCode::InvalidRequest as u32);
    assert_eq!(repeated.server_seq, 3);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 1);
    second.request(3, 1, b"ok").await;
    assert_eq!(second.response().await.server_seq, 4);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 2);
    fixture.finish().await;
}

#[tokio::test]
async fn recovery_takes_over_live_connection_and_rejects_invalid_confirmation_without_takeover() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    first.request(2, 1, b"ok").await;
    assert_eq!(first.response().await.server_seq, 1);
    let mut second = fixture.connect().await;
    assert_eq!(
        second.resume(1, &login.resume_token, 999).await.error_code,
        ErrorCode::InvalidRequest as u32
    );
    first.request(3, 1, b"ok").await;
    assert_eq!(first.response().await.server_seq, 2);
    assert_eq!(second.resume(2, &login.resume_token, 2).await.error_code, 0);
    let Content::Recovered(done) = second.read().await else {
        panic!()
    };
    assert_eq!(done.through_seq, 2);
    assert!(first.codec.read(&mut first.stream).await.unwrap().is_none());
    drop(first);
    second.request(4, 1, b"ok").await;
    assert_eq!(second.response().await.server_seq, 3);
    assert_eq!(
        fixture
            .sessions
            .session("1")
            .unwrap()
            .unwrap()
            .identity()
            .session_id,
        login.session_id
    );
    fixture.finish().await;
}

#[tokio::test]
async fn grace_expiry_keeps_valid_identity_token_for_fast_login_and_resets_sequence() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 40,
        resume_token_ttl_ms: 5000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    let old = fixture.sessions.session("1").unwrap().unwrap();
    drop(first);
    fixture.released("1").await;
    assert!(old.cancellation().is_cancelled());
    let mut second = fixture.connect().await;
    assert_eq!(
        second.resume(1, &login.resume_token, 0).await.error_code,
        ErrorCode::ResumeUnavailable as u32
    );
    let fresh = second.fast_login(2, &login.resume_token).await;
    assert_eq!(fresh.error_code, 0);
    assert_ne!(fresh.session_id, login.session_id);
    assert!(fresh.epoch > login.epoch);
    assert_ne!(fresh.resume_token, login.resume_token);
    second.request(3, 1, b"ok").await;
    assert_eq!(second.response().await.server_seq, 1);
    let mut rejected = fixture.connect().await;
    assert_eq!(
        rejected.resume(1, &login.resume_token, 0).await.error_code,
        ErrorCode::ResumeUnavailable as u32
    );
    assert_eq!(
        rejected.fast_login(2, &login.resume_token).await.error_code,
        ErrorCode::SessionReplaced as u32
    );
    assert_eq!(
        fixture
            .sessions
            .session("1")
            .unwrap()
            .unwrap()
            .identity()
            .session_id,
        fresh.session_id
    );
    drop(second);
    fixture.released("1").await;
    assert_eq!(
        rejected.fast_login(3, &login.resume_token).await.error_code,
        ErrorCode::SessionReplaced as u32
    );
    assert_eq!(
        rejected.fast_login(4, &fresh.resume_token).await.error_code,
        0
    );
    fixture.finish().await;
}

#[tokio::test]
async fn cache_gap_requires_fast_login_and_never_silently_skips_messages() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        replay_capacity: 1,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    let handle = fixture.sessions.session("1").unwrap().unwrap();
    drop(first);
    for _ in 0..2 {
        handle
            .notify(LogicalMessage {
                kind: MessageKind::Notify,
                message_id: 90,
                request_id: 0,
                server_seq: None,
                error_code: 0,
                body: vec![],
            })
            .await
            .unwrap();
    }
    let mut second = fixture.connect().await;
    assert_eq!(
        second.resume(1, &login.resume_token, 0).await.error_code,
        ErrorCode::ResumeUnavailable as u32
    );
    assert!(!handle.cancellation().is_cancelled());
    assert_eq!(
        second.fast_login(2, &login.resume_token).await.error_code,
        0
    );
    assert!(handle.cancellation().is_cancelled());
    second.request(3, 1, b"ok").await;
    assert_eq!(second.response().await.server_seq, 1);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn invalid_token_cannot_restore_or_fast_login_and_offline_lease_loss_stops_recovery() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    let handle = fixture.sessions.session("1").unwrap().unwrap();
    drop(first);
    let lease = fixture.ownership.lookup("1").await.unwrap().unwrap();
    fixture
        .ownership
        .acquire(
            "1".into(),
            "foreign".into(),
            SessionOwner {
                node_id: "other".into(),
                service_name: "gateway".into(),
            },
            Some(lease.identity.epoch),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let mut second = fixture.connect().await;
    assert_eq!(
        second.resume(1, &"0".repeat(64), 0).await.error_code,
        ErrorCode::AuthenticationFailed as u32
    );
    assert_eq!(
        second.fast_login(2, &"0".repeat(64)).await.error_code,
        ErrorCode::AuthenticationFailed as u32
    );
    assert_eq!(
        second.resume(3, &login.resume_token, 0).await.error_code,
        ErrorCode::ResumeUnavailable as u32
    );
    assert!(handle.cancellation().is_cancelled());
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn cumulative_ack_releases_cache_and_never_executes_player() {
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    client.login(1, "1").await;
    let session = fixture.sessions.session("1").unwrap().unwrap();
    client
        .send(Content::Ack(protocol::Ack { ack_seq: 0 }))
        .await;
    client.request(2, 1, b"ok").await;
    assert_eq!(client.response().await.server_seq, 1);
    for ack_seq in [0, 1, 1, 0] {
        client.send(Content::Ack(protocol::Ack { ack_seq })).await;
    }
    client.request(3, 1, b"ok").await;
    assert_eq!(client.response().await.server_seq, 2);
    assert!(session.replay_after(0).unwrap().is_none());
    let batch = session.replay_after(1).unwrap().unwrap();
    assert_eq!(batch.through_seq, 2);
    assert_eq!(batch.frames.len(), 1);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 2);
    let internal = session
        .try_request(LogicalMessage {
            kind: MessageKind::Request,
            message_id: 1,
            request_id: 4,
            error_code: 0,
            server_seq: None,
            body: vec![],
        })
        .unwrap()
        .await;
    assert!(internal.server_seq.is_none());
    assert_eq!(session.replay_after(2).unwrap().unwrap().through_seq, 2);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 3);
    client
        .send(Content::Ack(protocol::Ack { ack_seq: 3 }))
        .await;
    let Content::Error(error) = client.read().await else {
        panic!("缺少越界确认错误");
    };
    assert_eq!(error.error_code, ErrorCode::InvalidRequest as u32);
    assert!(
        client
            .codec
            .read(&mut client.stream)
            .await
            .unwrap()
            .is_none()
    );
    fixture.released("1").await;
    assert_eq!(
        session.replay_after(2).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    fixture.finish().await;
}

#[tokio::test]
async fn ack_requires_authenticated_binding_and_cannot_confirm_unpublished_messages() {
    let fixture = Fixture::new(config()).await;
    for authenticated in [false, true] {
        let mut client = fixture.connect().await;
        if authenticated {
            client.login(1, "1").await;
        }
        client
            .send(Content::Ack(protocol::Ack { ack_seq: 1 }))
            .await;
        let Content::Error(error) = client.read().await else {
            panic!("缺少 ACK 控制错误");
        };
        assert_eq!(
            error.error_code,
            if authenticated {
                ErrorCode::InvalidRequest
            } else {
                ErrorCode::Unauthenticated
            } as u32
        );
        assert!(
            client
                .codec
                .read(&mut client.stream)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn oversized_response_is_cached_after_normalization_and_notify_rejection_preserves_sequence()
{
    let fixture = Fixture::new(Config {
        max_frame_size: 512,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    let session = fixture.sessions.session("1").unwrap().unwrap();
    assert_eq!(
        session
            .notify(LogicalMessage {
                kind: MessageKind::Notify,
                message_id: 90,
                request_id: 0,
                error_code: 0,
                server_seq: None,
                body: vec![8; 512],
            })
            .await
            .unwrap_err()
            .code,
        ErrorCode::FrameTooLarge
    );
    assert!(!session.cancellation().is_cancelled());
    client.request(2, 3, b"ok").await;
    let response = client.response().await;
    assert_eq!(response.server_seq, 1);
    assert_eq!(response.request_id, 2);
    assert_eq!(response.message_id, 3);
    assert_eq!(response.error_code, ErrorCode::FrameTooLarge as u32);
    assert!(response.body.is_empty());
    let batch = session.replay_after(0).unwrap().unwrap();
    assert_eq!(
        batch.frames[0].as_ref(),
        Frame {
            version: VERSION,
            content: Some(Content::Response(response))
        }
        .encode_to_vec()
    );
    client.request(3, 1, b"ok").await;
    assert_eq!(client.response().await.server_seq, 2);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 2);
    fixture.finish().await;
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
    let Content::Error(error) = client.read().await else {
        panic!("未认证业务应返回控制错误");
    };
    assert_eq!(error.error_code, ErrorCode::Unauthenticated as u32);
    assert_eq!(error.request_id, 2);
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
    let mut fixture = Fixture::new(Config {
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
    assert_eq!(rejected.server_seq, 1);
    let session = fixture.sessions.session("1").unwrap().unwrap();
    session
        .notify(LogicalMessage {
            kind: MessageKind::Notify,
            message_id: 90,
            request_id: 0,
            error_code: 0,
            server_seq: None,
            body: vec![9],
        })
        .await
        .unwrap();
    let Content::Notify(notify) = client.read().await else {
        panic!("缺少通知");
    };
    assert_eq!(notify.server_seq, 2);
    fixture.tracker.permits.add_permits(1);
    let response = client.response().await;
    assert_eq!(response.request_id, 2);
    assert_eq!(response.server_seq, 3);
    assert_eq!(fixture.started().await, "1");
    fixture.tracker.permits.add_permits(1);
    let response = client.response().await;
    assert_eq!(response.request_id, 3);
    assert_eq!(response.server_seq, 4);
    let batch = session.replay_after(0).unwrap().unwrap();
    assert_eq!(batch.through_seq, 4);
    assert_eq!(batch.frames.len(), 4);
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
    old_handle
        .notify(LogicalMessage {
            kind: MessageKind::Notify,
            message_id: 90,
            request_id: 0,
            error_code: 0,
            server_seq: None,
            body: vec![],
        })
        .await
        .unwrap();
    let Content::Notify(notify) = first.read().await else {
        panic!()
    };
    assert_eq!(notify.server_seq, 1);
    first.request(2, 1, b"block").await;
    fixture.started().await;
    let mut second = fixture.connect().await;
    second
        .send(Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "0001".into(),
            resume_token: String::new(),
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
    let response = second.response().await;
    assert_eq!(response.body, b"1");
    assert_eq!(response.server_seq, 1);
    assert_eq!(
        old_handle.replay_after(0).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    assert_eq!(fixture.tracker.players.lock().unwrap()["1"].1, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn closed_sessions_are_reclaimed_and_foreign_ownership_is_not_taken() {
    let fixture = Fixture::new(Config {
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
    #[expect(
        clippy::let_underscore_must_use,
        reason = "租约丢失时允许连接先关闭，不限定最后一个帧，随后断言业务未执行。"
    )]
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
    let fixture = Fixture::new(Config {
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
    let fixture = Fixture::new(Config {
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
    handle.notify(notification.clone()).await.unwrap();
    let Content::Notify(message) = client.read().await else {
        panic!("缺少通知");
    };
    assert_eq!(message.body, vec![9]);
    assert_eq!(message.server_seq, 1);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    handle.notify(notification.clone()).await.unwrap();
    assert!(handle.notify(notification).await.is_err());
    #[expect(
        clippy::let_underscore_must_use,
        reason = "慢连接退出可能是帧错误或 EOF，随后以所有权释放和缓存关闭作断言。"
    )]
    let _ = client.codec.read(&mut client.stream).await;
    fixture.released("1").await;
    assert_eq!(
        handle.replay_after(0).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    fixture.finish().await;
}

#[tokio::test]
async fn malformed_frames_are_rejected_before_business_execution() {
    let fixture = Fixture::new(Config {
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
    for version in [1, 2, 99] {
        let mut client = fixture.connect().await;
        let body = Frame {
            version,
            content: Some(Content::Login(protocol::LoginRequest {
                request_id: 1,
                credential: "1".into(),
                resume_token: String::new(),
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
    }
    let mut partial = fixture.connect().await;
    partial.stream.write_all(&[0, 0]).await.unwrap();
    partial.stream.shutdown().await.unwrap();
    let Content::Error(error) = partial.read().await else {
        panic!("缺少半帧错误");
    };
    assert_eq!(error.error_code, ErrorCode::SessionClosed as u32);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn truncated_transport_frame_does_not_destroy_the_recoverable_session() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    first.stream.write_all(&[0, 0]).await.unwrap();
    first.stream.shutdown().await.unwrap();
    let Content::Error(error) = first.read().await else {
        panic!()
    };
    assert_eq!(error.error_code, ErrorCode::SessionClosed as u32);
    let mut second = fixture.connect().await;
    assert_eq!(second.resume(1, &login.resume_token, 0).await.error_code, 0);
    assert!(matches!(second.read().await, Content::Recovered(_)));
    second.request(2, 1, b"ok").await;
    assert_eq!(second.response().await.server_seq, 1);
    fixture.finish().await;
}

#[tokio::test]
async fn frame_deadline_and_connection_capacity_are_bounded() {
    let fixture = Fixture::new(Config {
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

#[tokio::test]
async fn new_credential_login_kicks_old_connection_and_old_token_cannot_reenter_after_logout() {
    let fixture = Fixture::new(config()).await;
    let mut first = fixture.connect().await;
    let old = first.login(1, "1").await;
    let old_handle = fixture.sessions.session("1").unwrap().unwrap();
    let mut second = fixture.connect().await;
    let fresh = second.login(1, "1").await;
    assert_eq!(fresh.error_code, 0);
    assert_ne!(fresh.session_id, old.session_id);
    assert!(old_handle.cancellation().is_cancelled());
    assert!(first.codec.read(&mut first.stream).await.unwrap().is_none());
    let mut stale = fixture.connect().await;
    assert_eq!(
        stale.resume(1, &old.resume_token, 0).await.error_code,
        ErrorCode::ResumeUnavailable as u32
    );
    assert_eq!(
        stale.fast_login(2, &old.resume_token).await.error_code,
        ErrorCode::SessionReplaced as u32
    );
    second.request(2, 1, b"still-current").await;
    assert_eq!(second.response().await.error_code, 0);
    drop(second);
    fixture.released("1").await;
    assert_eq!(
        stale.fast_login(3, &old.resume_token).await.error_code,
        ErrorCode::SessionReplaced as u32
    );
    assert_eq!(stale.fast_login(4, &fresh.resume_token).await.error_code, 0);
    fixture.finish().await;
}

#[tokio::test]
async fn online_refresh_after_token_expiry_preserves_session_and_reconnects() {
    let fixture = Fixture::new(Config {
        resume_token_ttl_ms: 1000,
        disconnect_grace_ms: 3000,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    let login = client.login(1, "1").await;
    client.request(2, 1, b"").await;
    assert_eq!(client.response().await.server_seq, 1);
    // 验证在线会话可以在原凭证确实过期后刷新；凭证期限使用真实 UTC。
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut probe = fixture.connect().await;
    assert_eq!(
        probe.resume(1, &login.resume_token, 1).await.error_code,
        ErrorCode::AuthenticationFailed as u32
    );
    let refreshed = client.refresh(3).await;
    assert_eq!(refreshed.error_code, 0);
    assert_eq!(refreshed.resume_token_ttl_ms, 1000);
    assert_ne!(refreshed.resume_token, login.resume_token);
    assert_eq!(
        fixture
            .sessions
            .session("1")
            .unwrap()
            .unwrap()
            .identity()
            .session_id,
        login.session_id
    );
    client.request(4, 1, b"").await;
    assert_eq!(client.response().await.server_seq, 2);
    drop(client);
    let mut restored = fixture.connect().await;
    let resumed = restored.resume(1, &refreshed.resume_token, 2).await;
    assert_eq!(resumed.error_code, 0);
    assert_eq!(resumed.session_id, login.session_id);
    assert_eq!(resumed.epoch, login.epoch);
    assert!(matches!(restored.read().await, Content::Recovered(done) if done.through_seq == 2));
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 2);
    fixture.finish().await;
}

#[tokio::test]
async fn refresh_requires_authentication_unique_request_id_and_cannot_bypass_top_login() {
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    let denied = client.refresh(1).await;
    assert_eq!(denied.error_code, ErrorCode::Unauthenticated as u32);
    assert!(denied.resume_token.is_empty());
    let login = client.login(2, "1").await;
    assert_eq!(login.error_code, 0);
    for id in [0, 2] {
        let denied = client.refresh(id).await;
        assert_eq!(denied.error_code, ErrorCode::InvalidRequest as u32);
        assert!(denied.resume_token.is_empty());
    }
    let refreshed = client.refresh(3).await;
    assert_eq!(refreshed.error_code, 0);
    let duplicate = client.refresh(3).await;
    assert_eq!(duplicate.error_code, ErrorCode::InvalidRequest as u32);
    assert!(duplicate.resume_token.is_empty());
    let mut replacement = fixture.connect().await;
    let new_login = replacement.login(1, "1").await;
    assert_eq!(new_login.error_code, 0);
    assert_ne!(new_login.session_id, login.session_id);
    assert!(
        client
            .codec
            .read(&mut client.stream)
            .await
            .unwrap()
            .is_none()
    );
    let mut stale = fixture.connect().await;
    assert_eq!(
        stale
            .fast_login(1, &refreshed.resume_token)
            .await
            .error_code,
        ErrorCode::SessionReplaced as u32
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn refresh_cannot_issue_credentials_after_ownership_is_lost() {
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    let login = client.login(1, "1").await;
    let ownership = fixture.ownership.lookup("1").await.unwrap().unwrap();
    fixture.ownership.release(ownership).await.unwrap();
    client
        .send(Content::RefreshResumeToken(
            protocol::RefreshResumeTokenRequest { request_id: 2 },
        ))
        .await;
    if let Some(frame) = client.codec.read(&mut client.stream).await.unwrap() {
        let Some(Content::ResumeTokenRefreshed(response)) = frame.content else {
            panic!()
        };
        assert_ne!(response.error_code, 0);
        assert!(response.resume_token.is_empty());
    }
    assert!(fixture.sessions.session("1").unwrap().is_none());
    assert!(!login.resume_token.is_empty());
    fixture.finish().await;
}

#[derive(Clone, Copy)]
enum TestKind {
    Broadcast,
    Forward,
}
fn event_message(
    target: tok_gateway::player::events::Target,
    kind: TestKind,
    body: &[u8],
) -> tok_gateway::player::events::Delivery {
    use tok_gateway::player::events::{Action, Delivery};
    Delivery {
        target,
        action: match kind {
            TestKind::Broadcast => Action::Notify {
                message_id: 10,
                body: body.to_vec(),
            },
            TestKind::Forward => Action::Forward(Arc::new(tok_events::Message {
                event_id: tok_events::EventId::generate(),
                event_type: "test.event".into(),
                body: body.to_vec(),
                trace_context: Default::default(),
            })),
        },
    }
}
// 游戏定义自己的事件正文和网关策略，传输层不认识玩家目标。
#[derive(Clone, PartialEq, prost::Message)]
struct GameEvent {
    #[prost(string, repeated, tag = "1")]
    players: Vec<String>,
    #[prost(string, optional, tag = "2")]
    topic: Option<String>,
    #[prost(bool, tag = "3")]
    notify: bool,
    #[prost(bytes, tag = "4")]
    body: Vec<u8>,
}
impl tok_events::Event for GameEvent {
    const NAME: &'static str = "test.game.event";
}
fn publication(delivery: tok_gateway::player::events::Delivery) -> tok_events::Message {
    use tok_gateway::player::events::{Action, Target};
    let (notify, body) = match delivery.action {
        Action::Notify { body, .. } => (true, body),
        Action::Forward(event) => (false, event.body.clone()),
    };
    let mut game = GameEvent {
        players: vec![],
        topic: None,
        notify,
        body,
    };
    match delivery.target {
        Target::All => {}
        Target::Players(players) => game.players = players,
        Target::Topic(topic) => game.topic = Some(topic),
    }
    tok_events::Message::new(
        &tok_events::Envelope::new(tok_events::EventId::generate(), game).unwrap(),
    )
    .unwrap()
}
fn game_subscriber(sessions: SessionManager) -> tok_events::Registry {
    let mut handlers = tok_events::Registry::new(1).unwrap();
    handlers
        .register::<GameEvent, _>(move |request| {
            let event = &request.input.event.payload;
            let sessions = sessions.clone();
            Box::pin(async move {
                use tok_gateway::player::events::Target;
                let target = if let Some(topic) = event.topic.clone() {
                    Target::Topic(topic)
                } else if event.players.is_empty() {
                    Target::All
                } else {
                    Target::Players(event.players.clone())
                };
                let kind = if event.notify {
                    TestKind::Broadcast
                } else {
                    TestKind::Forward
                };
                let report = sessions
                    .deliver(&event_message(target, kind, &event.body))
                    .await
                    .map_err(|_| tok_events::Error::Unavailable("游戏事件准入失败"))?;
                Ok(report.into())
            })
        })
        .unwrap();
    handlers.enable_protobuf::<GameEvent>().unwrap();
    handlers
}

#[tokio::test]
async fn client_cannot_submit_server_events_or_send_notify_frames() {
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    let event = publication(event_message(
        tok_gateway::player::events::Target::All,
        TestKind::Forward,
        b"forged",
    ));
    // 即使业务正文是完整 broker 事件编码，也只能走普通请求表。
    client.request(2, 999, &event.encode(4096).unwrap()).await;
    assert!(
        matches!(client.read().await, Content::Response(response) if response.error_code == ErrorCode::UnknownMessage as u32)
    );
    assert!(fixture.tracker.events.lock().unwrap().is_empty());
    // Notify 只允许下行；伪造合法帧不能取得服务器事件发送权限。
    client
        .send(Content::Notify(protocol::Notify {
            message_id: 10,
            body: b"forged".to_vec(),
            server_seq: 1,
        }))
        .await;
    assert!(
        matches!(client.read().await, Content::Error(error) if error.error_code == ErrorCode::InvalidState as u32)
    );
    assert!(fixture.tracker.events.lock().unwrap().is_empty());
    fixture.finish().await;
}
#[tokio::test]
async fn events_share_request_fifo_have_independent_sequence_and_respect_queue_capacity() {
    let mut fixture = Fixture::new(Config {
        queue_capacity: 1,
        ..config()
    })
    .await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    client.request(2, 1, b"block").await;
    fixture.started.recv().await.unwrap();
    let message = event_message(
        tok_gateway::player::events::Target::Players(vec!["1".into(), "1".into()]),
        TestKind::Forward,
        b"event",
    );
    assert_eq!(
        fixture.sessions.deliver(&message).await.unwrap().admitted,
        1
    );
    assert_eq!(
        fixture.sessions.deliver(&message).await.unwrap().rejected,
        1
    );
    assert!(fixture.tracker.events.lock().unwrap().is_empty());
    fixture.tracker.permits.add_permits(1);
    assert!(matches!(client.read().await, Content::Response(response) if response.request_id==2));
    fixture.started.recv().await.unwrap();
    client.request(3, 1, b"next").await;
    assert!(
        matches!(client.read().await,Content::Response(response) if response.request_id==3 && response.server_seq==2)
    );
    assert_eq!(*fixture.tracker.events.lock().unwrap(), vec![1]);
    assert_eq!(
        fixture.tracker.players.lock().unwrap().get("1").unwrap().1,
        1
    );
    fixture.finish().await;
}
#[tokio::test]
async fn broadcast_filters_topics_and_offline_sessions_without_executing_player() {
    let fixture = Fixture::new(Config {
        max_topics: 1,
        ..config()
    })
    .await;
    let mut one = fixture.connect().await;
    let mut two = fixture.connect().await;
    assert_eq!(one.login(1, "1").await.error_code, 0);
    assert_eq!(two.login(1, "2").await.error_code, 0);
    let handle = fixture.sessions.session("1").unwrap().unwrap();
    handle.join_topic("guild:7").unwrap();
    handle.join_topic("guild:7").unwrap();
    assert_eq!(
        handle.join_topic("guild:8").unwrap_err().code,
        ErrorCode::CapacityExceeded
    );
    let message = event_message(
        tok_gateway::player::events::Target::Topic("guild:7".into()),
        TestKind::Broadcast,
        b"notice",
    );
    let report = fixture.sessions.deliver(&message).await.unwrap();
    assert_eq!(report.admitted, 1);
    assert_eq!(report.skipped, 0);
    assert!(
        matches!(one.read().await,Content::Notify(notify) if notify.server_seq==1 && notify.body==b"notice")
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    handle.leave_topic("guild:7").unwrap();
    assert_eq!(
        fixture.sessions.deliver(&message).await.unwrap().admitted,
        0
    );
    let all = event_message(
        tok_gateway::player::events::Target::All,
        TestKind::Broadcast,
        b"all",
    );
    assert_eq!(fixture.sessions.deliver(&all).await.unwrap().admitted, 2);
    assert!(matches!(one.read().await, Content::Notify(_)));
    assert!(matches!(two.read().await, Content::Notify(_)));
    drop(one);
    drop(two);
    // 等到 TCP 解绑完成；宽限期保留的 session 不接收新事件，也不消耗下行序号。
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(fixture.sessions.deliver(&all).await.unwrap().admitted, 0);
    assert_eq!(
        fixture
            .sessions
            .deliver(&event_message(
                tok_gateway::player::events::Target::Players(vec!["offline".into()]),
                TestKind::Forward,
                b"skip"
            ))
            .await
            .unwrap()
            .skipped,
        1
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn large_topic_delivery_yields_and_rechecks_membership_after_resuming() {
    use std::{future::Future, task::Poll};
    let fixture = Fixture::new(Config {
        max_sessions: 65,
        max_connections: 65,
        ..config()
    })
    .await;
    let mut clients = Vec::new();
    let mut handles = Vec::new();
    for player in 1..=65 {
        let player = player.to_string();
        let mut client = fixture.connect().await;
        assert_eq!(client.login(1, &player).await.error_code, 0);
        let handle = fixture.sessions.session(&player).unwrap().unwrap();
        handle.join_topic("large").unwrap();
        handles.push(handle);
        clients.push(client);
    }
    let delivery = event_message(
        tok_gateway::player::events::Target::Topic("large".into()),
        TestKind::Broadcast,
        b"batch",
    );
    let mut pending = Box::pin(fixture.sessions.deliver(&delivery));
    let first = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "大规模投递必须让出调度");
    // 暂停期间退出主题不受索引/会话锁阻塞；快照中的剩余目标必须重新校验。
    for handle in &handles {
        handle.leave_topic("large").unwrap();
    }
    let report = pending.await.unwrap();
    assert_eq!(report.admitted, 64);
    assert_eq!(report.skipped, 1);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn topic_index_survives_resume_and_cleans_up_replaced_and_closed_sessions() {
    let fixture = Fixture::new(Config {
        disconnect_grace_ms: 2000,
        ..config()
    })
    .await;
    let mut first = fixture.connect().await;
    let login = first.login(1, "1").await;
    let old = fixture.sessions.session("1").unwrap().unwrap();
    old.join_topic("guild:7").unwrap();
    drop(first);
    let mut resumed = fixture.connect().await;
    assert_eq!(
        resumed.resume(1, &login.resume_token, 0).await.error_code,
        0
    );
    assert!(matches!(resumed.read().await, Content::Recovered(_)));
    let delivery = event_message(
        tok_gateway::player::events::Target::Topic("guild:7".into()),
        TestKind::Broadcast,
        b"notice",
    );
    assert_eq!(
        fixture.sessions.deliver(&delivery).await.unwrap().admitted,
        1
    );
    assert!(matches!(resumed.read().await, Content::Notify(notify) if notify.server_seq == 1));
    let mut replacement = fixture.connect().await;
    assert_eq!(replacement.login(1, "1").await.error_code, 0);
    assert_eq!(
        fixture.sessions.deliver(&delivery).await.unwrap().admitted,
        0
    );
    let current = fixture.sessions.session("1").unwrap().unwrap();
    current.join_topic("guild:7").unwrap();
    assert!(old.leave_topic("guild:7").is_err());
    old.close();
    assert_eq!(
        fixture.sessions.deliver(&delivery).await.unwrap().admitted,
        1
    );
    assert!(matches!(replacement.read().await, Content::Notify(notify) if notify.server_seq == 1));
    current.close();
    assert_eq!(
        fixture.sessions.deliver(&delivery).await.unwrap().admitted,
        0
    );
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "需要真实 NATS；通过 just nats-test 显式运行"]
async fn nats_independent_gateways_route_all_players_topics_and_forward_to_their_own_queues() {
    let config = tok_events::nats::Config {
        url: std::env::var("TOK_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        namespace: format!("gateways-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };
    let a_bus = tok_events::nats::Bus::connect(config.clone())
        .await
        .unwrap();
    let b_bus = tok_events::nats::Bus::connect(config.clone())
        .await
        .unwrap();
    let publisher = tok_events::nats::Bus::connect(config).await.unwrap();
    let mut a =
        Fixture::with_backend_and_events(crate::config(), |memory| memory, Some(&a_bus)).await;
    let mut b =
        Fixture::with_backend_and_events(crate::config(), |memory| memory, Some(&b_bus)).await;
    let mut one = a.connect().await;
    let mut two = b.connect().await;
    assert_eq!(one.login(1, "1").await.error_code, 0);
    assert_eq!(two.login(1, "2").await.error_code, 0);
    a.sessions
        .session("1")
        .unwrap()
        .unwrap()
        .join_topic("guild:7")
        .unwrap();
    let all = event_message(
        tok_gateway::player::events::Target::All,
        TestKind::Broadcast,
        b"all",
    );
    publisher.publish(&publication(all)).await.unwrap();
    assert!(
        matches!(one.read().await,Content::Notify(notify) if notify.body==b"all" && notify.server_seq==1)
    );
    assert!(
        matches!(two.read().await,Content::Notify(notify) if notify.body==b"all" && notify.server_seq==1)
    );
    let topic = event_message(
        tok_gateway::player::events::Target::Topic("guild:7".into()),
        TestKind::Broadcast,
        b"topic",
    );
    publisher.publish(&publication(topic)).await.unwrap();
    assert!(
        matches!(one.read().await,Content::Notify(notify) if notify.body==b"topic" && notify.server_seq==2)
    );
    let forward = event_message(
        tok_gateway::player::events::Target::Players(vec!["1".into(), "2".into()]),
        TestKind::Forward,
        b"forward",
    );
    publisher.publish(&publication(forward)).await.unwrap();
    a.started.recv().await.unwrap();
    b.started.recv().await.unwrap();
    one.request(2, 1, b"after").await;
    two.request(2, 1, b"after").await;
    assert!(
        matches!(one.read().await,Content::Response(response) if response.request_id==2 && response.server_seq==3)
    );
    assert!(
        matches!(two.read().await,Content::Response(response) if response.request_id==2 && response.server_seq==2)
    );
    assert_eq!(*a.tracker.events.lock().unwrap(), vec![1]);
    assert_eq!(*b.tracker.events.lock().unwrap(), vec![1]);
    a.finish().await;
    b.finish().await;
    publisher.close().await.unwrap();
}

#[tokio::test]
async fn built_in_event_handlers_and_custom_components_compose_independently() {
    use tok_events::{Handler, Outcome, Subscriber};
    use tok_gateway::player::events::{Target, forward, notify};
    let fixture = Fixture::new(config()).await;
    let mut client = fixture.connect().await;
    assert_eq!(client.login(1, "1").await.error_code, 0);
    let message = Arc::new(publication(event_message(
        Target::All,
        TestKind::Forward,
        b"value",
    )));
    let forwarding =
        Arc::new(forward::<GameEvent, _>(fixture.sessions.clone(), |_| Ok(Target::All)).unwrap());
    let notifying = Arc::new(
        notify::<GameEvent, _>(fixture.sessions.clone(), 12, |event| {
            Ok((Target::All, event.body.clone()))
        })
        .unwrap(),
    );
    let components = [
        Subscriber::new(
            tok_core::service::Identity {
                node_id: "test".into(),
                service_name: "events".into(),
            },
            Arc::clone(&forwarding) as Arc<dyn Handler>,
            Some("game.forward"),
        )
        .unwrap(),
        Subscriber::new(
            tok_core::service::Identity {
                node_id: "test".into(),
                service_name: "events".into(),
            },
            Arc::clone(&notifying) as Arc<dyn Handler>,
            None,
        )
        .unwrap(),
    ];
    assert_eq!(components[0].group(), Some("game.forward"));
    assert_eq!(components[1].group(), None);
    assert_eq!(
        forwarding
            .handle(event_request(Arc::clone(&message)))
            .await
            .unwrap(),
        Outcome::Handled
    );
    // 请求作为 FIFO 屏障，确保之前入队的事件已经执行；事件本身不生成客户端回应。
    client.request(2, 1, b"barrier").await;
    assert!(matches!(client.read().await, Content::Response(response) if response.request_id == 2));
    assert_eq!(*fixture.tracker.events.lock().unwrap(), vec![1]);
    let calls = fixture.tracker.calls.load(Ordering::SeqCst);
    assert_eq!(
        notifying
            .handle(event_request(Arc::clone(&message)))
            .await
            .unwrap(),
        Outcome::Handled
    );
    assert!(
        matches!(client.read().await, Content::Notify(notice) if notice.message_id == 12 && notice.body == b"value")
    );
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), calls);
    // 本地强类型事件也复用同一转发链，只在进入需要编码的 session 边界时编码。
    let local = tok_events::Envelope::new(
        tok_events::EventId::new("local-forward").unwrap(),
        GameEvent::decode(message.body.as_slice()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        forwarding
            .dispatch(&mut (), event_request(&local))
            .await
            .unwrap(),
        vec![Outcome::Handled]
    );
    client.request(3, 1, b"local-barrier").await;
    assert!(matches!(client.read().await,Content::Response(response) if response.request_id==3));
    assert_eq!(*fixture.tracker.events.lock().unwrap(), vec![1, 2]);
    let calls = fixture.tracker.calls.load(Ordering::SeqCst);
    // 同一事件还可交给完全自定义的处理者，不需要进入转发或通知分支。
    let count = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&count);
    let mut custom = tok_events::Registry::new(1).unwrap();
    custom
        .register::<GameEvent, _>(move |_request| {
            captured.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Outcome::Handled) })
        })
        .unwrap();
    custom.enable_protobuf::<GameEvent>().unwrap();
    assert_eq!(
        custom.handle(event_request(message)).await.unwrap(),
        Outcome::Handled
    );
    let custom = Subscriber::new(
        tok_core::service::Identity {
            node_id: "test".into(),
            service_name: "events".into(),
        },
        Arc::new(custom),
        Some("game.custom"),
    )
    .unwrap();
    assert_eq!(custom.group(), Some("game.custom"));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tracker.calls.load(Ordering::SeqCst), calls);
    fixture.finish().await;
}

fn event_request<T>(input: T) -> tok_service::Request<T> {
    let context = tok_core::execution::Context::new(
        tok_core::service::Identity {
            node_id: "test".into(),
            service_name: "gateway".into(),
        },
        "test",
        tok_core::execution::Source::Rpc { request_id: 1 },
        Duration::from_secs(5),
        CancellationToken::new(),
    )
    .unwrap();
    tok_service::Request::new(context, input)
}

fn tok_core_identity() -> tok_core::service::Identity {
    tok_core::service::Identity {
        node_id: "local".into(),
        service_name: "player-gateway".into(),
    }
}
