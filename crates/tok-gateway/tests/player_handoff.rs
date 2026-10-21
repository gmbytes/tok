//! 真实 TCP/gRPC 双网关交接与失败边界；常规测试不依赖外部服务。
use tok_core::BoxFuture;
use tok_core::Identity as SessionOwner;
use tok_grpc::Listener;
#[path = "support/topics.rs"]
mod topic_support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::ServiceDescriptor;
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
use tok_message::rpc::protocol::internal::v1 as proto;
use tok_player::{
    LogicalMessage, MessageKind, credentials::ResumeTokenCodec, ownership::Store as OwnershipStore,
};
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
    ) -> BoxFuture<'_, tok_player::ownership::Result<Option<tok_player::ownership::Record>>> {
        self.inner.lookup(player)
    }
    fn acquire(
        &self,
        player: String,
        session: String,
        owner: SessionOwner,
        expected: Option<u64>,
        ttl: Duration,
    ) -> BoxFuture<'_, tok_player::ownership::Result<tok_player::ownership::Record>> {
        self.inner.acquire(player, session, owner, expected, ttl)
    }
    fn renew(
        &self,
        value: tok_player::ownership::Record,
        ttl: Duration,
    ) -> BoxFuture<'_, tok_player::ownership::Result<()>> {
        self.inner.renew(value, ttl)
    }
    fn release(
        &self,
        value: tok_player::ownership::Record,
    ) -> BoxFuture<'_, tok_player::ownership::Result<bool>> {
        self.inner.release(value)
    }
    fn transfer(
        &self,
        value: tok_player::ownership::Record,
        target: SessionOwner,
        ttl: Duration,
    ) -> BoxFuture<'_, tok_player::ownership::Result<tok_player::ownership::Record>> {
        Box::pin(async move {
            if self.committed_error {
                self.inner.transfer(value, target, ttl).await?;
            }
            Err(tok_player::ownership::Error::Unavailable(
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
    events: std::sync::Mutex<Vec<u64>>,
    calls: AtomicUsize,
    ticks: AtomicUsize,
    started: Notify,
    permits: Semaphore,
}
impl Default for Execution {
    fn default() -> Self {
        Self {
            events: std::sync::Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            ticks: AtomicUsize::new(0),
            started: Notify::new(),
            permits: Semaphore::new(0),
        }
    }
}
impl Executor for Execution {
    fn execute_event(
        &self,
        input: tok_execution::Request<tok_gateway::player::EventInput>,
    ) -> tok_gateway::player::EventTask<'_> {
        let tok_gateway::player::EventInput {
            identity: _,
            request_id,
            event: _,
        } = input.input;
        Box::pin(async move {
            self.events.lock().unwrap().push(request_id);
            Ok(tok_execution::Response::new(None))
        })
    }

    fn execute_request(
        &self,
        input: tok_execution::Request<tok_gateway::player::RequestInput>,
    ) -> Task<'_> {
        let tok_gateway::player::RequestInput {
            identity: _,
            message: request,
        } = input.input;
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            if request.body == b"block" {
                self.permits.acquire().await.unwrap().forget();
            }
            let timers = (request.message_id == 2).then(|| {
                tok_player::TimerUpdate::ReplaceAll(vec![tok_player::TimerSpec {
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
            Ok(tok_execution::Response::new(Reply {
                response: Some(LogicalMessage {
                    kind: MessageKind::Response,
                    message_id: request.message_id,
                    request_id: request.request_id,
                    server_seq: None,
                    error_code: 0,
                    body: request.body,
                }),
                timers,
            }))
        })
    }
    fn execute_timer(
        &self,
        input: tok_execution::Request<tok_gateway::player::TimerInput>,
    ) -> Task<'_> {
        let tok_gateway::player::TimerInput {
            identity: _,
            request_id: _,
            spec: _,
            trigger: _,
        } = input.input;
        Box::pin(async move {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(tok_execution::Response::new(Reply {
                response: None,
                timers: None,
            }))
        })
    }
}
#[derive(Default)]
struct FixtureOptions {
    extension: Option<Arc<dyn tok_gateway::player::session::extension::Factory>>,
    topics: Option<Arc<topic_support::TopicTransport>>,
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
        Self::configured_with_extension(node, directory, ownership, tokens, execution, config, None)
            .await
    }
    async fn configured_with_extension(
        node: &str,
        directory: Arc<MemoryDirectory>,
        ownership: Arc<dyn OwnershipStore>,
        tokens: Arc<dyn ResumeTokenCodec>,
        execution: Arc<Execution>,
        config: Config,
        extension: Option<Arc<dyn tok_gateway::player::session::extension::Factory>>,
    ) -> Self {
        Self::configured_options(
            node,
            directory,
            ownership,
            tokens,
            execution,
            config,
            FixtureOptions {
                extension,
                topics: None,
            },
        )
        .await
    }
    async fn configured_options(
        node: &str,
        directory: Arc<MemoryDirectory>,
        ownership: Arc<dyn OwnershipStore>,
        tokens: Arc<dyn ResumeTokenCodec>,
        execution: Arc<Execution>,
        config: Config,
        options: FixtureOptions,
    ) -> Self {
        let service = GatewayRpcService::new(config.max_handoff_bytes).unwrap();
        let gateway = Gateway::new(
            tok_core_identity(node),
            config.gateway_limits(),
            |context| {
                let tasks = context.tasks();
                let ingress = Ingress::new(
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
                .with_resume_token_codec(tokens);
                let ingress =
                    ingress.with_gateway_rpc(Arc::<MemoryDirectory>::clone(&directory), &service);
                // 注册发生在完整组装之后，链式配置顺序不能让内部 RPC 看到旧工厂。
                let ingress = match options.extension {
                    Some(extension) => ingress.with_session_extension(extension)?,
                    None => ingress,
                };
                match options.topics {
                    Some(topics) => ingress
                        .with_topic_interests(topics.pool(tasks), vec!["a".into(), "b".into()]),
                    None => Ok(ingress),
                }
            },
        )
        .unwrap();
        let sessions = gateway.ingress().sessions();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_address = rpc_listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let rpc = Listener::new()
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
) -> proto::gateway_rpc_client::GatewayRpcClient<tok_message::rpc::tonic::transport::Channel> {
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
    let source = a.sessions.session("42").unwrap().unwrap();
    source.join_topic("guild:7").await.unwrap();
    let event = tok_gateway::player::events::Delivery {
        target: tok_gateway::player::events::Target::Topic("guild:7".into()),
        action: tok_gateway::player::events::Action::Forward(Arc::new(tok_events::Message {
            topic: Default::default(),
            event_id: tok_events::EventId::generate(),
            event_type: "test.handoff".into(),
            body: vec![],
            trace_context: Default::default(),
        })),
    };
    assert_eq!(
        a.sessions
            .deliver(tok_execution::Request::new(
                delivery_context(),
                event.clone()
            ))
            .await
            .unwrap()
            .output
            .admitted,
        1
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while execution.events.lock().unwrap().len() != 1 {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();

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
    assert!(source.join_topic("stale").await.is_err());
    assert_eq!(
        b.sessions
            .deliver(tok_execution::Request::new(
                delivery_context(),
                event.clone()
            ))
            .await
            .unwrap()
            .output
            .admitted,
        1
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while execution.events.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    assert_eq!(*execution.events.lock().unwrap(), vec![1, 2]);

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
    let source = a.sessions.session("42").unwrap().unwrap();
    source.join_topic("retained").await.unwrap();

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
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.channel.as_ref().unwrap().topics, vec!["retained"]);
    assert_eq!(
        source.join_topic("during-freeze").await.unwrap_err().code,
        tok_gateway::ErrorCode::InvalidState
    );
    assert_eq!(
        source
            .post_event(Arc::new(tok_events::Message {
                topic: Default::default(),
                event_id: tok_events::EventId::generate(),
                event_type: "test.freeze".into(),
                body: vec![],
                trace_context: Default::default()
            }))
            .unwrap_err()
            .code,
        tok_gateway::ErrorCode::InvalidState
    );

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
        .await
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

    source.leave_topic("retained").await.unwrap();
    source.join_topic("after-cancel").await.unwrap();
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
        tok_message::rpc::tonic::Code::InvalidArgument
    );
    let unknown = proto::HandoffRequest {
        source_service_name: "missing".into(),
        ..Default::default()
    };
    assert_eq!(
        client.prepare_handoff(unknown).await.unwrap_err().code(),
        tok_message::rpc::tonic::Code::Unavailable
    );
    fixture.stop().await;
}

fn tok_core_identity(node: &str) -> tok_core::service::Identity {
    tok_core::service::Identity {
        node_id: node.into(),
        service_name: "gateway".into(),
    }
}

struct ExtensionData {
    value: std::sync::atomic::AtomicU64,
    node: String,
    epoch: u64,
}
#[derive(Clone, Copy)]
enum ExtensionMode {
    Good,
    RejectValidation,
    RejectRestore,
    TooLarge,
}
struct SessionExtension {
    mode: ExtensionMode,
    changes: std::sync::Mutex<Vec<tok_gateway::player::session::extension::Change>>,
}
impl tok_gateway::player::session::extension::Factory for SessionExtension {
    fn kind(&self) -> &str {
        "test.state.v1"
    }
    fn initialize(
        &self,
        request: tok_execution::Request<tok_player::SessionIdentity>,
    ) -> tok_gateway::Result<tok_core::Extensions> {
        let mut extensions = tok_core::Extensions::default();
        extensions.insert(ExtensionData {
            value: std::sync::atomic::AtomicU64::new(0),
            node: request.context.service().node_id.clone(),
            epoch: request.input.epoch,
        });
        Ok(extensions)
    }
    fn snapshot(
        &self,
        request: tok_execution::Request<tok_gateway::player::SessionHandle>,
    ) -> tok_gateway::Result<Vec<u8>> {
        if matches!(self.mode, ExtensionMode::TooLarge) {
            return Ok(vec![0; 65_537]);
        }
        Ok(request
            .input
            .extensions()
            .get::<ExtensionData>()
            .unwrap()
            .value
            .load(Ordering::SeqCst)
            .to_le_bytes()
            .to_vec())
    }
    fn validate(&self, body: &[u8]) -> tok_gateway::Result<()> {
        if body.len() != 8 || matches!(self.mode, ExtensionMode::RejectValidation) {
            Err(tok_gateway::ErrorCode::ResumeUnavailable.into())
        } else {
            Ok(())
        }
    }
    fn restore(
        &self,
        request: tok_execution::Request<(tok_player::SessionIdentity, &[u8])>,
    ) -> tok_gateway::Result<tok_core::Extensions> {
        self.validate(request.input.1)?;
        if matches!(self.mode, ExtensionMode::RejectRestore) {
            return Err(tok_gateway::ErrorCode::Internal.into());
        }
        let mut bytes = [0; 8];
        bytes.copy_from_slice(request.input.1);
        let mut extensions = tok_core::Extensions::default();
        extensions.insert(ExtensionData {
            value: std::sync::atomic::AtomicU64::new(u64::from_le_bytes(bytes)),
            node: request.context.service().node_id.clone(),
            epoch: request.input.0.epoch,
        });
        Ok(extensions)
    }
    fn on_change(
        &self,
        request: tok_execution::Request<tok_gateway::player::session::extension::Change>,
    ) {
        self.changes.lock().unwrap().push(request.input);
    }
}
fn session_extension(mode: ExtensionMode) -> Arc<SessionExtension> {
    Arc::new(SessionExtension {
        mode,
        changes: std::sync::Mutex::new(Vec::new()),
    })
}
#[tokio::test]
async fn required_session_extensions_restore_with_new_owner_and_explicit_snapshot() {
    use tok_gateway::player::session::extension::Change;
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let source = session_extension(ExtensionMode::Good);
    let target = session_extension(ExtensionMode::Good);
    let config = Config {
        handoff_enabled: true,
        handoff_timeout_ms: 1500,
        ..Default::default()
    };
    let a = Fixture::configured_with_extension(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        config.clone(),
        Some(Arc::clone(&source) as Arc<_>),
    )
    .await;
    let b = Fixture::configured_with_extension(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        config,
        Some(Arc::clone(&target) as Arc<_>),
    )
    .await;
    let (stream, login) = login(&a).await;
    let old = a.sessions.session("42").unwrap().unwrap();
    old.extensions()
        .get::<ExtensionData>()
        .unwrap()
        .value
        .store(17, Ordering::SeqCst);
    let mut stream_b = resume(&b, &login).await;
    assert!(
        matches!(read(&mut stream_b).await,Content::Resumed(response) if response.error_code==0)
    );
    assert!(matches!(read(&mut stream_b).await, Content::Recovered(_)));
    let current = b.sessions.session("42").unwrap().unwrap();
    let state = current.extensions().get::<ExtensionData>().unwrap();
    assert_eq!(state.value.load(Ordering::SeqCst), 17);
    assert_eq!(state.node, "b");
    assert_eq!(state.epoch, current.identity().epoch);
    assert!(state.epoch > login.epoch);
    assert!(
        source
            .changes
            .lock()
            .unwrap()
            .contains(&Change::HandoffPreparing)
    );
    assert!(
        source
            .changes
            .lock()
            .unwrap()
            .contains(&Change::HandoffCommitted)
    );
    assert!(target.changes.lock().unwrap().contains(&Change::Restored));
    assert!(old.cancellation().is_cancelled());
    drop(stream);
    drop(stream_b);
    a.stop().await;
    b.stop().await;
}
#[tokio::test]
async fn incompatible_rejected_or_oversized_extensions_do_not_transfer_ownership() {
    use tok_gateway::player::session::extension::Change;
    for mode in [
        None,
        Some(ExtensionMode::RejectValidation),
        Some(ExtensionMode::TooLarge),
    ] {
        let directory = Arc::new(MemoryDirectory::default());
        let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
        let tokens: Arc<dyn ResumeTokenCodec> =
            Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
        let execution = Arc::new(Execution::default());
        let source = session_extension(if matches!(mode, Some(ExtensionMode::TooLarge)) {
            ExtensionMode::TooLarge
        } else {
            ExtensionMode::Good
        });
        let target = mode.map(|_| session_extension(ExtensionMode::RejectValidation));
        let config = Config {
            handoff_enabled: true,
            handoff_timeout_ms: 1500,
            max_handoff_bytes: 65_536,
            ..Default::default()
        };
        let a = Fixture::configured_with_extension(
            "a",
            Arc::clone(&directory),
            Arc::<MemoryOwnership>::clone(&ownership),
            Arc::clone(&tokens),
            Arc::clone(&execution),
            config.clone(),
            Some(Arc::clone(&source) as Arc<_>),
        )
        .await;
        let b = Fixture::configured_with_extension(
            "b",
            directory,
            Arc::<MemoryOwnership>::clone(&ownership),
            tokens,
            execution,
            config,
            target.map(|target| target as Arc<_>),
        )
        .await;
        let (stream, login) = login(&a).await;
        let old = a.sessions.session("42").unwrap().unwrap();
        let mut refused = resume(&b, &login).await;
        assert!(
            matches!(read(&mut refused).await,Content::Resumed(response) if response.error_code!=0)
        );
        assert_eq!(
            ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
            "a"
        );
        assert!(!old.cancellation().is_cancelled());
        assert!(
            source
                .changes
                .lock()
                .unwrap()
                .contains(&Change::HandoffCancelled)
        );
        let mut recovered = resume(&a, &login).await;
        assert!(
            matches!(read(&mut recovered).await,Content::Resumed(response) if response.error_code==0)
        );
        assert!(matches!(read(&mut recovered).await, Content::Recovered(_)));
        drop(stream);
        drop(refused);
        drop(recovered);
        a.stop().await;
        b.stop().await;
    }
}
#[tokio::test]
async fn restore_failure_after_commit_releases_target_without_reopening_source() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(16).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let source = session_extension(ExtensionMode::Good);
    let target = session_extension(ExtensionMode::RejectRestore);
    let config = Config {
        handoff_enabled: true,
        handoff_timeout_ms: 1500,
        ..Default::default()
    };
    let a = Fixture::configured_with_extension(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        config.clone(),
        Some(source),
    )
    .await;
    let b = Fixture::configured_with_extension(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        config,
        Some(target),
    )
    .await;
    let (stream, login) = login(&a).await;
    let old = a.sessions.session("42").unwrap().unwrap();
    let mut rejected = resume(&b, &login).await;
    assert!(
        matches!(read(&mut rejected).await,Content::Resumed(response) if response.error_code!=0)
    );
    assert!(old.cancellation().is_cancelled());
    assert!(b.sessions.session("42").unwrap().is_none());
    assert!(ownership.lookup("42").await.unwrap().is_none());
    drop(stream);
    drop(rejected);
    a.stop().await;
    b.stop().await;
}

#[tokio::test]
async fn target_waits_for_topic_readiness_before_transfer_and_source_releases_after_commit() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(8).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let source_transport = Arc::new(topic_support::TopicTransport::new());
    let target_transport = Arc::new(topic_support::TopicTransport::new());
    source_transport.permits.add_permits(2);
    let config = Config {
        handoff_enabled: true,
        handoff_timeout_ms: 1500,
        ..Default::default()
    };
    let a = Fixture::configured_options(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        config.clone(),
        FixtureOptions {
            topics: Some(Arc::clone(&source_transport)),
            ..Default::default()
        },
    )
    .await;
    let b = Fixture::configured_options(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        config,
        FixtureOptions {
            topics: Some(Arc::clone(&target_transport)),
            ..Default::default()
        },
    )
    .await;
    let (stream, login) = login(&a).await;
    let source = a.sessions.session("42").unwrap().unwrap();
    source.join_topic("room").await.unwrap();
    drop(stream);
    let attempt = tokio::spawn(async move {
        let stream = resume(&b, &login).await;
        (b, stream)
    });
    target_transport.entered.notified().await;
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "a"
    );
    assert_eq!(source_transport.active.load(Ordering::SeqCst), 2);
    target_transport.permits.add_permits(2);
    let (b, mut stream) = attempt.await.unwrap();
    assert!(
        matches!(read(&mut stream).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(matches!(read(&mut stream).await, Content::Recovered(_)));
    source.wait_finished().await;
    assert_eq!(source_transport.active.load(Ordering::SeqCst), 0);
    assert_eq!(target_transport.active.load(Ordering::SeqCst), 2);
    assert_eq!(
        b.sessions
            .session("42")
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .topics,
        vec!["room"]
    );
    a.stop().await;
    b.stop().await;
    assert_eq!(target_transport.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn target_topic_failure_cancels_preparation_without_transferring_ownership() {
    let directory = Arc::new(MemoryDirectory::default());
    let ownership = Arc::new(MemoryOwnership::new(8).unwrap());
    let tokens: Arc<dyn ResumeTokenCodec> =
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap());
    let execution = Arc::new(Execution::default());
    let source_transport = Arc::new(topic_support::TopicTransport::new());
    let target_transport = Arc::new(topic_support::TopicTransport::new());
    source_transport.permits.add_permits(2);
    target_transport.permits.add_permits(1);
    target_transport.fail_at.store(1, Ordering::SeqCst);
    let config = Config {
        handoff_enabled: true,
        handoff_timeout_ms: 1500,
        ..Default::default()
    };
    let a = Fixture::configured_options(
        "a",
        Arc::clone(&directory),
        Arc::<MemoryOwnership>::clone(&ownership),
        Arc::clone(&tokens),
        Arc::clone(&execution),
        config.clone(),
        FixtureOptions {
            topics: Some(Arc::clone(&source_transport)),
            ..Default::default()
        },
    )
    .await;
    let b = Fixture::configured_options(
        "b",
        directory,
        Arc::<MemoryOwnership>::clone(&ownership),
        tokens,
        execution,
        config,
        FixtureOptions {
            topics: Some(Arc::clone(&target_transport)),
            ..Default::default()
        },
    )
    .await;
    let (stream, login) = login(&a).await;
    let source = a.sessions.session("42").unwrap().unwrap();
    source.join_topic("room").await.unwrap();
    drop(stream);
    let mut failed = resume(&b, &login).await;
    assert!(
        matches!(read(&mut failed).await, Content::Resumed(response) if response.error_code != 0)
    );
    assert_eq!(
        ownership.lookup("42").await.unwrap().unwrap().owner.node_id,
        "a"
    );
    assert_eq!(source_transport.active.load(Ordering::SeqCst), 2);
    assert_eq!(target_transport.active.load(Ordering::SeqCst), 0);
    let mut restored = resume(&a, &login).await;
    assert!(
        matches!(read(&mut restored).await, Content::Resumed(response) if response.error_code == 0)
    );
    assert!(matches!(read(&mut restored).await, Content::Recovered(_)));
    assert_eq!(source.snapshot().unwrap().topics, vec!["room"]);
    a.stop().await;
    b.stop().await;
}

#[cfg(test)]
fn delivery_context() -> tok_execution::Context {
    tok_execution::Context::new(
        tok_core::Identity {
            node_id: "local".into(),
            service_name: "test-events".into(),
        },
        "test-delivery",
        std::time::Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn all_gateway_rpc_methods_validate_execution_metadata_before_state_changes() {
    use tok_grpc::tonic;
    let fixture = Fixture::configured(
        "a",
        Arc::new(MemoryDirectory::default()),
        Arc::new(MemoryOwnership::new(16).unwrap()),
        Arc::new(tok_gateway::player::resume_token::SignedResumeTokens::ephemeral().unwrap()),
        Arc::new(Execution::default()),
        Config::default(),
    )
    .await;
    let service = GatewayRpcService::new(Config::default().max_handoff_bytes).unwrap();
    service.register_player(fixture.sessions.clone()).unwrap();
    for operation in 0..4 {
        for (timeout, expected) in [
            ("invalid", tonic::Code::InvalidArgument),
            ("0n", tonic::Code::DeadlineExceeded),
        ] {
            let mut request = tonic::Request::new(proto::HandoffRequest {
                source_service_name: "gateway".into(),
                ..Default::default()
            });
            request
                .metadata_mut()
                .insert("grpc-timeout", timeout.parse().unwrap());
            let error = match operation {
                0 => proto::gateway_rpc_server::GatewayRpc::prepare_handoff(&service, request)
                    .await
                    .unwrap_err(),
                1 => proto::gateway_rpc_server::GatewayRpc::commit_handoff(&service, request)
                    .await
                    .unwrap_err(),
                2 => proto::gateway_rpc_server::GatewayRpc::cancel_handoff(&service, request)
                    .await
                    .unwrap_err(),
                _ => proto::gateway_rpc_server::GatewayRpc::complete_handoff(&service, request)
                    .await
                    .unwrap_err(),
            };
            assert_eq!(error.code(), expected);
        }
    }
    fixture.stop().await;
}
