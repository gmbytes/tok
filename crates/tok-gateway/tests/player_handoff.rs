//! 真实 TCP/gRPC 双网关交接与失败边界；常规测试不依赖外部服务。

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{
    LogicalMessage, MessageKind, ServiceDescriptor, SessionIdentity,
    coordination::{OwnershipStore, SessionOwner},
    credentials::ResumeTokenCodec,
};
use tok_gateway::{
    Gateway,
    codec::{FrameCodec, VERSION},
    player::{
        AuthenticatedIdentity, AuthenticationTask, Authenticator, Config, Executor, Ingress, Reply,
        SessionManager, Task,
    },
    protocol::{self, Frame, frame::Content},
    rpc::GatewayRpcService,
};
use tok_infra::{discovery::MemoryDirectory, ownership::MemoryOwnership};
use tok_rpc::{RpcEndpoint, protocol::internal::v1 as proto};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct TransferFault {
    inner: Arc<MemoryOwnership>,
    committed_error: bool,
}
impl OwnershipStore for TransferFault {
    fn lookup(
        &self,
        player: &str,
    ) -> tok_core::coordination::CoordinationTask<
        '_,
        Option<tok_core::coordination::SessionOwnership>,
    > {
        self.inner.lookup(player)
    }
    fn acquire(
        &self,
        player: String,
        session: String,
        owner: SessionOwner,
        expected: Option<u64>,
        ttl: Duration,
    ) -> tok_core::coordination::CoordinationTask<'_, tok_core::coordination::SessionOwnership>
    {
        self.inner.acquire(player, session, owner, expected, ttl)
    }
    fn renew(
        &self,
        value: tok_core::coordination::SessionOwnership,
        ttl: Duration,
    ) -> tok_core::coordination::CoordinationTask<'_, ()> {
        self.inner.renew(value, ttl)
    }
    fn release(
        &self,
        value: tok_core::coordination::SessionOwnership,
    ) -> tok_core::coordination::CoordinationTask<'_, bool> {
        self.inner.release(value)
    }
    fn transfer(
        &self,
        value: tok_core::coordination::SessionOwnership,
        target: SessionOwner,
        ttl: Duration,
    ) -> tok_core::coordination::CoordinationTask<'_, tok_core::coordination::SessionOwnership>
    {
        Box::pin(async move {
            if self.committed_error {
                self.inner.transfer(value, target, ttl).await?;
            }
            Err(tok_core::Error::CoordinationUnavailable(
                "injected transfer result loss".into(),
            ))
        })
    }
}

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, player_id: String) -> AuthenticationTask<'_> {
        Box::pin(async move { Ok(AuthenticatedIdentity { player_id }) })
    }
}
struct Execution {
    calls: AtomicUsize,
    ticks: AtomicUsize,
    started: Notify,
    permits: Semaphore,
}
impl Default for Execution {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            ticks: AtomicUsize::new(0),
            started: Notify::new(),
            permits: Semaphore::new(0),
        }
    }
}
impl Executor for Execution {
    fn execute_request(&self, _: SessionIdentity, request: LogicalMessage) -> Task<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            if request.body == b"block" {
                self.permits.acquire().await.unwrap().forget();
            }
            let timers = (request.message_id == 2).then(|| {
                tok_core::TimerUpdate::ReplaceAll(vec![tok_core::TimerSpec {
                    timer_id: "production".into(),
                    message_id: 9,
                    body: vec![],
                    due_at_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                        + 300,
                    interval_ms: None,
                }])
            });
            Ok(Reply {
                response: Some(LogicalMessage {
                    kind: MessageKind::Response,
                    message_id: request.message_id,
                    request_id: request.request_id,
                    server_seq: None,
                    error_code: 0,
                    body: request.body,
                }),
                timers,
            })
        })
    }
    fn execute_timer(
        &self,
        _: SessionIdentity,
        _: u64,
        _: tok_core::TimerSpec,
        _: tok_core::TimerTrigger,
    ) -> Task<'_> {
        Box::pin(async move {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(Reply {
                response: None,
                timers: None,
            })
        })
    }
}
struct Fixture {
    address: std::net::SocketAddr,
    rpc_address: std::net::SocketAddr,
    sessions: SessionManager,
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}
impl Fixture {
    async fn new(
        node: &str,
        directory: Arc<MemoryDirectory>,
        ownership: Arc<dyn OwnershipStore>,
        tokens: Arc<dyn ResumeTokenCodec>,
        execution: Arc<Execution>,
        max_bytes: usize,
    ) -> Self {
        let config = Config {
            handoff_enabled: true,
            handoff_timeout_ms: 1500,
            max_handoff_bytes: max_bytes,
            ..Default::default()
        };
        Self::configured(node, directory, ownership, tokens, execution, config).await
    }
    async fn configured(
        node: &str,
        directory: Arc<MemoryDirectory>,
        ownership: Arc<dyn OwnershipStore>,
        tokens: Arc<dyn ResumeTokenCodec>,
        execution: Arc<Execution>,
        config: Config,
    ) -> Self {
        let service = GatewayRpcService::new(config.max_handoff_bytes).unwrap();
        let gateway = Gateway::new(config.runtime_limits(), |context| {
            Ingress::new(
                context,
                config,
                SessionOwner {
                    node_id: node.into(),
                    service_name: "gateway".into(),
                },
                Arc::new(Auth),
                ownership,
                execution,
            )?
            .with_resume_token_codec(tokens)
            .with_gateway_rpc(Arc::<MemoryDirectory>::clone(&directory), &service)
        })
        .unwrap();
        let sessions = gateway.ingress().sessions();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_address = rpc_listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let rpc = RpcEndpoint::new()
            .add_service("gateway-rpc", service.into_rpc())
            .unwrap();
        rpc.readiness().set("gateway-rpc", true);
        directory
            .register(ServiceDescriptor {
                node_id: node.into(),
                service_name: "gateway-rpc".into(),
                protocol: "grpc".into(),
                address: rpc_address,
            })
            .unwrap();
        let serve = rpc.serve(rpc_listener, shutdown.clone()).unwrap();
        let stop = shutdown.clone();
        let tasks = vec![
            tokio::spawn(async move {
                gateway.serve(listener, stop).await.unwrap();
            }),
            tokio::spawn(async move {
                serve.await.unwrap();
            }),
        ];
        Self {
            address,
            rpc_address,
            sessions,
            shutdown,
            tasks,
        }
    }
    async fn stop(mut self) {
        self.shutdown.cancel();
        for task in self.tasks.drain(..) {
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}
fn codec() -> FrameCodec {
    FrameCodec::new(1048576, Duration::from_secs(3)).unwrap()
}
async fn send(stream: &mut TcpStream, content: Content) {
    codec()
        .write(
            stream,
            &Frame {
                version: VERSION,
                content: Some(content),
            },
        )
        .await
        .unwrap();
}
async fn read(stream: &mut TcpStream) -> Content {
    codec()
        .read(stream)
        .await
        .unwrap()
        .unwrap()
        .content
        .unwrap()
}
async fn login(fixture: &Fixture) -> (TcpStream, protocol::LoginResponse) {
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    send(
        &mut stream,
        Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "42".into(),
            resume_token: String::new(),
        }),
    )
    .await;
    let Content::LoggedIn(login) = read(&mut stream).await else {
        panic!("expected login")
    };
    assert_eq!(login.error_code, 0);
    (stream, login)
}
async fn request(stream: &mut TcpStream, id: u64, message: u32, body: &[u8]) {
    send(
        stream,
        Content::Request(protocol::BusinessRequest {
            request_id: id,
            message_id: message,
            body: body.to_vec(),
        }),
    )
    .await;
}
async fn resume(fixture: &Fixture, login: &protocol::LoginResponse) -> TcpStream {
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    send(
        &mut stream,
        Content::Resume(protocol::ResumeRequest {
            request_id: 1,
            resume_token: login.resume_token.clone(),
            ack_seq: 0,
        }),
    )
    .await;
    stream
}
async fn rpc(
    fixture: &Fixture,
) -> proto::gateway_rpc_client::GatewayRpcClient<tok_rpc::tonic::transport::Channel> {
    proto::gateway_rpc_client::GatewayRpcClient::connect(format!("http://{}", fixture.rpc_address))
        .await
        .unwrap()
}
fn prepare_request(login: &protocol::LoginResponse, id: &str) -> proto::HandoffRequest {
    proto::HandoffRequest {
        handoff_id: id.into(),
        expected: Some(proto::GatewayOwnership {
            identity: Some(proto::SessionIdentity {
                player_id: "42".into(),
                session_id: login.session_id.clone(),
                epoch: login.epoch,
            }),
            owner: Some(proto::GatewayOwner {
                node_id: "a".into(),
                service_name: "gateway".into(),
            }),
        }),
        target: Some(proto::GatewayOwner {
            node_id: "b".into(),
            service_name: "gateway".into(),
        }),
        ack_seq: 0,
        snapshot_id: String::new(),
        budget_ms: 1000,
        source_service_name: "gateway".into(),
    }
}

#[tokio::test]
async fn handoff_replays_original_response_preserves_identity_and_runs_timer_once() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::new(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let (mut stream, login) = login(&a).await;
    request(&mut stream, 2, 2, b"state").await;
    let Content::Response(original) = read(&mut stream).await else {
        panic!()
    };
    drop(stream);
    let old = a.sessions.session("42").unwrap().unwrap();
    let mut stream = resume(&b, &login).await;
    let Content::Resumed(resumed) = read(&mut stream).await else {
        panic!()
    };
    assert_eq!(resumed.error_code, 0);
    assert_eq!(resumed.session_id, login.session_id);
    assert!(resumed.epoch > login.epoch);
    assert_eq!(resumed.last_request_id, 2);
    let Content::Response(replayed) = read(&mut stream).await else {
        panic!()
    };
    assert_eq!(replayed, original);
    assert!(matches!(read(&mut stream).await, Content::Recovered(_)));
    assert!(
        old.try_request(LogicalMessage {
            kind: MessageKind::Request,
            message_id: 1,
            request_id: 3,
            server_seq: None,
            error_code: 0,
            body: vec![]
        })
        .is_err()
    );
    request(&mut stream, 3, 1, b"next").await;
    let Content::Response(next) = read(&mut stream).await else {
        panic!()
    };
    assert_eq!(next.server_seq, 2);
    tokio::time::timeout(Duration::from_secs(2), async {
        while execution.ticks.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(execution.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "b"
    );
    send(
        &mut stream,
        Content::RefreshResumeToken(protocol::RefreshResumeTokenRequest { request_id: 4 }),
    )
    .await;
    let Content::ResumeTokenRefreshed(refreshed) = read(&mut stream).await else {
        panic!()
    };
    assert_eq!(refreshed.error_code, 0);
    assert!(!refreshed.resume_token.is_empty());
    drop(stream);
    let updated = protocol::LoginResponse {
        resume_token: refreshed.resume_token,
        ..login.clone()
    };
    let mut restored = resume(&b, &updated).await;
    let Content::Resumed(reconnected) = read(&mut restored).await else {
        panic!()
    };
    assert_eq!(reconnected.error_code, 0);
    assert_eq!(reconnected.epoch, resumed.epoch);
    assert_eq!(reconnected.session_id, login.session_id);
    drop(restored);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn draining_waits_for_inflight_result_without_reexecuting_it() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::new(
        "b",
        directory,
        ownership,
        tokens,
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let (mut stream, login) = login(&a).await;
    request(&mut stream, 2, 1, b"block").await;
    execution.started.notified().await;
    let mut target = resume(&b, &login).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), read(&mut target))
            .await
            .is_err()
    );
    execution.permits.add_permits(1);
    assert!(
        matches!(read(&mut target).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(
        matches!(read(&mut target).await, Content::Response(response) if response.request_id == 2)
    );
    assert!(matches!(read(&mut target).await, Content::Recovered(_)));
    assert_eq!(execution.calls.load(Ordering::SeqCst), 1);
    drop(stream);
    drop(target);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn cancelled_snapshot_cannot_commit_and_old_session_can_resume() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let a = Fixture::new(
        "a",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        Arc::new(Execution::default()),
        8388608,
    )
    .await;
    let (stream, login) = login(&a).await;
    let mut client = rpc(&a).await;
    let mut request = prepare_request(&login, &uuid::Uuid::new_v4().to_string());
    let snapshot = client
        .prepare_handoff(request.clone())
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    let duplicate = client
        .prepare_handoff(request.clone())
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    assert_eq!(snapshot, duplicate);
    let old = a.sessions.session("42").unwrap().unwrap();
    assert_eq!(
        old.notify(LogicalMessage {
            kind: MessageKind::Notify,
            message_id: 1,
            request_id: 0,
            server_seq: None,
            error_code: 0,
            body: vec![]
        })
        .unwrap_err()
        .code,
        tok_gateway::ErrorCode::InvalidState
    );
    request.snapshot_id = snapshot.snapshot_id;
    client.cancel_handoff(request.clone()).await.unwrap();
    assert!(client.commit_handoff(request).await.is_err());
    let mut restored = resume(&a, &login).await;
    assert!(
        matches!(read(&mut restored).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(matches!(read(&mut restored).await, Content::Recovered(_)));
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "a"
    );
    drop(stream);
    drop(restored);
    a.stop().await;
}

#[tokio::test]
async fn failed_target_validation_cancels_before_ownership_changes() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::new(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        65536,
    )
    .await;
    let (mut stream, login) = login(&a).await;
    request(&mut stream, 2, 1, &vec![7; 70000]).await;
    read(&mut stream).await;
    let mut target = resume(&b, &login).await;
    assert!(
        matches!(read(&mut target).await, Content::Resumed(response) if response.error_code != 0)
    );
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "a"
    );
    let mut restored = resume(&a, &login).await;
    assert!(
        matches!(read(&mut restored).await, Content::Resumed(response) if response.error_code == 0)
    );
    drop(stream);
    drop(target);
    drop(restored);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn uncertain_commit_is_confirmed_by_lookup_and_never_reexecutes_business() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let faulty = Arc::new(TransferFault {
        inner: Arc::<MemoryOwnership>::clone(&ownership),
        committed_error: true,
    });
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        faulty,
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::new(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let (stream, login) = login(&a).await;
    let mut target = resume(&b, &login).await;
    assert!(
        matches!(read(&mut target).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(matches!(read(&mut target).await, Content::Recovered(_)));
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "b"
    );
    assert_eq!(execution.calls.load(Ordering::SeqCst), 0);
    drop(stream);
    drop(target);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn failed_commit_stops_old_session_and_shared_token_can_fast_login() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        Arc::new(TransferFault {
            inner: Arc::<MemoryOwnership>::clone(&ownership),
            committed_error: false,
        }),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::new(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        8388608,
    )
    .await;
    let (stream, login) = login(&a).await;
    let mut target = resume(&b, &login).await;
    assert!(
        matches!(read(&mut target).await, Content::Resumed(response) if response.error_code != 0)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while ownership.lookup("42").await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    send(
        &mut target,
        Content::Login(protocol::LoginRequest {
            request_id: 2,
            credential: String::new(),
            resume_token: login.resume_token.clone(),
        }),
    )
    .await;
    let Content::LoggedIn(new) = read(&mut target).await else {
        panic!()
    };
    assert_eq!(new.error_code, 0);
    assert_ne!(new.session_id, login.session_id);
    assert!(new.epoch > login.epoch);
    drop(stream);
    drop(target);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn expired_preparation_unfreezes_source_and_rejects_late_commit() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let a = Fixture::new(
        "a",
        directory,
        ownership,
        tokens,
        Arc::new(Execution::default()),
        8388608,
    )
    .await;
    let (stream, login) = login(&a).await;
    let mut client = rpc(&a).await;
    let mut request = prepare_request(&login, &uuid::Uuid::new_v4().to_string());
    request.budget_ms = 80;
    let snapshot = client
        .prepare_handoff(request.clone())
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    request.snapshot_id = snapshot.snapshot_id;
    tokio::time::sleep(Duration::from_millis(130)).await;
    assert!(client.commit_handoff(request).await.is_err());
    let mut restored = resume(&a, &login).await;
    assert!(
        matches!(read(&mut restored).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(matches!(read(&mut restored).await, Content::Recovered(_)));
    drop(stream);
    drop(restored);
    a.stop().await;
}

#[tokio::test]
async fn competing_targets_cannot_prepare_the_same_source_together() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let a = Fixture::new(
        "a",
        directory,
        ownership,
        tokens,
        Arc::new(Execution::default()),
        8388608,
    )
    .await;
    let (stream, login) = login(&a).await;
    let mut first = rpc(&a).await;
    let mut second = first.clone();
    let one = prepare_request(&login, &uuid::Uuid::new_v4().to_string());
    let mut two = prepare_request(&login, &uuid::Uuid::new_v4().to_string());
    two.target.as_mut().unwrap().node_id = "c".into();
    let (x, y) = tokio::join!(
        first.prepare_handoff(one.clone()),
        second.prepare_handoff(two.clone())
    );
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    first
        .cancel_handoff(if x.is_ok() { one } else { two })
        .await
        .unwrap();
    drop(stream);
    a.stop().await;
}

#[tokio::test]
async fn client_disconnect_during_drain_does_not_leave_an_unowned_connection_binding() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let a = Fixture::new(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        8388608,
    )
    .await;
    let b = Fixture::configured(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        Arc::clone(&execution),
        Config {
            handoff_enabled: true,
            handoff_timeout_ms: 1500,
            disconnect_grace_ms: 100,
            ..Default::default()
        },
    )
    .await;
    let (mut original, login) = login(&a).await;
    request(&mut original, 2, 1, b"block").await;
    execution.started.notified().await;
    let target = resume(&b, &login).await;
    // A 解绑旧连接说明已进入排空，B 此时仍在等待在途调用。
    assert!(codec().read(&mut original).await.unwrap().is_none());
    drop(target);
    execution.permits.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while ownership.lookup("42").await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(b.sessions.session("42").unwrap().is_none());
    assert_eq!(execution.calls.load(Ordering::SeqCst), 1);
    drop(original);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn gateway_rpc_registers_player_when_handoff_is_disabled() {
    let fixture = Fixture::configured(
        "a",
        Arc::new(MemoryDirectory::default()),
        Arc::new(MemoryOwnership::new(16).unwrap()),
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap()),
        Arc::new(Execution::default()),
        Config::default(),
    )
    .await;
    let mut client = rpc(&fixture).await;
    let request = proto::HandoffRequest {
        source_service_name: "gateway".into(),
        ..Default::default()
    };
    // 统一入口已登记并找到管理器；交接开关在具体操作中拒绝，不影响注册。
    assert_eq!(
        client.prepare_handoff(request).await.unwrap_err().code(),
        tok_rpc::tonic::Code::InvalidArgument
    );
    let unknown = proto::HandoffRequest {
        source_service_name: "missing".into(),
        ..Default::default()
    };
    assert_eq!(
        client.prepare_handoff(unknown).await.unwrap_err().code(),
        tok_rpc::tonic::Code::Unavailable
    );
    fixture.stop().await;
}
