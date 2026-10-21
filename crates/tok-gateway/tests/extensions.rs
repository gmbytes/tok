//! 完整接入路径验证能力、异步中间件、生命周期及下行补发边界。
#[path = "support/topics.rs"]
mod topic_support;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tok_core::Identity as SessionOwner;
use tok_core::{Extensions, service::Identity};
use tok_execution::Request;
use tok_gateway::{
    ErrorCode, Gateway, GatewayError, Result,
    codec::{FrameCodec, VERSION},
    player::{
        self, Config, EventInput, Executor, Ingress, Reply, RequestInput, SessionHandle,
        SessionManager, TimerInput,
        session::{
            Phase as SessionPhase,
            extension::{self, Change},
        },
    },
    protocol::{self, Frame, frame::Content},
};
use tok_infra::ownership::MemoryOwnership;
use tok_lifecycle::{Builder, Init, InitFuture, task::Task};
use tok_player::{
    LogicalMessage, MessageKind, TimerSpec, TimerUpdate, ownership::Store as OwnershipStore,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
};
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service, ServiceExt};
#[derive(Default)]
struct Counts {
    request: AtomicUsize,
    event: AtomicUsize,
    timer: AtomicUsize,
    downlink: AtomicUsize,
    admission: AtomicUsize,
    changes: Mutex<Vec<Change>>,
    events: Mutex<Vec<Arc<tok_events::Message>>>,
}
struct GatewayData;
struct ManagerData;
struct SessionData {
    value: AtomicU64,
    node: String,
}
struct StateFactory {
    initialize_failure: bool,
    counts: Arc<Counts>,
}
impl extension::Factory for StateFactory {
    fn kind(&self) -> &str {
        "test.state.v1"
    }
    fn initialize(&self, request: Request<tok_player::SessionIdentity>) -> Result<Extensions> {
        assert!(
            request
                .context
                .extensions()
                .get::<SessionManager>()
                .is_some()
        );
        if self.initialize_failure {
            return Err(ErrorCode::Internal.into());
        }
        let mut extensions = Extensions::default();
        extensions.insert(SessionData {
            value: AtomicU64::new(0),
            node: request.context.service().node_id.clone(),
        });
        Ok(extensions)
    }
    fn snapshot(&self, request: Request<SessionHandle>) -> Result<Vec<u8>> {
        Ok(request
            .input
            .extensions()
            .get::<SessionData>()
            .unwrap()
            .value
            .load(Ordering::SeqCst)
            .to_le_bytes()
            .to_vec())
    }
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() == 8 {
            Ok(())
        } else {
            Err(ErrorCode::ResumeUnavailable.into())
        }
    }
    fn restore(
        &self,
        request: Request<(tok_player::SessionIdentity, &[u8])>,
    ) -> Result<Extensions> {
        self.validate(request.input.1)?;
        let mut bytes = [0; 8];
        bytes.copy_from_slice(request.input.1);
        let mut extensions = Extensions::default();
        extensions.insert(SessionData {
            value: AtomicU64::new(u64::from_le_bytes(bytes)),
            node: request.context.service().node_id.clone(),
        });
        Ok(extensions)
    }
    fn on_change(&self, request: Request<Change>) {
        let session = request.context.extensions().get::<SessionHandle>().unwrap();
        // 查询自身与管理器，证明游戏回调不在通道或目录锁内执行。
        session.snapshot().unwrap();
        request
            .context
            .extensions()
            .get::<SessionManager>()
            .unwrap()
            .snapshot()
            .unwrap();
        self.counts.changes.lock().unwrap().push(request.input);
    }
}
struct Auth;
impl player::Authenticator for Auth {
    fn authenticate(&self, credential: String) -> player::AuthenticationTask<'_> {
        Box::pin(async move {
            Ok(player::AuthenticatedIdentity {
                player_id: credential,
            })
        })
    }
}
struct Execution(Arc<Counts>);
impl Executor for Execution {
    fn execute_request(&self, request: Request<RequestInput>) -> player::Task<'_> {
        Box::pin(async move {
            assert_eq!(
                request.context.extensions().get::<player::ClientRequest>(),
                Some(&player::ClientRequest {
                    request_id: request.input.message.request_id,
                    message_id: request.input.message.message_id,
                })
            );
            request
                .context
                .extensions()
                .get::<SessionData>()
                .unwrap()
                .value
                .fetch_add(1, Ordering::SeqCst);
            let timers = if request.input.message.message_id == 80 {
                Some(TimerUpdate::ReplaceAll(vec![TimerSpec {
                    timer_id: "tick".into(),
                    message_id: 81,
                    body: vec![],
                    due_at_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                        + 30,
                    interval_ms: None,
                }]))
            } else {
                None
            };
            Ok(Reply {
                response: Some(LogicalMessage {
                    kind: MessageKind::Response,
                    message_id: 100,
                    request_id: request.input.message.request_id,
                    server_seq: None,
                    error_code: 0,
                    body: request.input.message.body,
                }),
                timers,
            })
        })
    }
    fn execute_event(&self, request: Request<EventInput>) -> player::EventTask<'_> {
        Box::pin(async move {
            request
                .context
                .extensions()
                .get::<SessionData>()
                .unwrap()
                .value
                .fetch_add(10, Ordering::SeqCst);
            self.0.events.lock().unwrap().push(request.input.event);
            Ok(None)
        })
    }
    fn execute_timer(&self, request: Request<TimerInput>) -> player::Task<'_> {
        Box::pin(async move {
            request
                .context
                .extensions()
                .get::<SessionData>()
                .unwrap()
                .value
                .fetch_add(100, Ordering::SeqCst);
            request
                .context
                .extensions()
                .get::<SessionHandle>()
                .unwrap()
                .notify(notification(b"tick"))
                .await?;
            Ok(Reply {
                response: None,
                timers: None,
            })
        })
    }
}
struct Gate {
    stage: &'static str,
    entered: Notify,
    permits: Semaphore,
}
#[derive(Clone)]
struct Audit {
    counts: Arc<Counts>,
    gate: Option<Arc<Gate>>,
}
#[derive(Clone)]
struct Audited<S> {
    inner: S,
    audit: Audit,
}
impl<S> Layer<S> for Audit {
    type Service = Audited<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Audited {
            inner,
            audit: self.clone(),
        }
    }
}
impl<T: Send + 'static, O: Send + 'static, S> Service<Request<T>> for Audited<S>
where
    S: Service<Request<T>, Response = O, Error = GatewayError> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = O;
    type Error = GatewayError;
    type Future = Pin<Box<dyn Future<Output = Result<O>> + Send>>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let inner = self.inner.clone();
        let audit = self.audit.clone();
        Box::pin(async move {
            let capabilities = request.context.extensions();
            assert!(capabilities.get::<GatewayData>().is_some());
            assert!(capabilities.get::<ManagerData>().is_some());
            let gateway = capabilities.get::<tok_gateway::Handle>().unwrap();
            let manager = capabilities.get::<SessionManager>().unwrap();
            let session = capabilities.get::<SessionHandle>().unwrap();
            assert_eq!(
                gateway.identity().service_name,
                manager.owner().service_name
            );
            assert_eq!(session.identity().player_id, "1");
            session.snapshot()?;
            let operation = request.context.operation();
            let counter = if operation.contains("/admission/") {
                &audit.counts.admission
            } else if operation.contains("/downlink/") {
                &audit.counts.downlink
            } else if operation.contains("/event/") {
                &audit.counts.event
            } else if operation.contains("/timer/") {
                &audit.counts.timer
            } else {
                &audit.counts.request
            };
            counter.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = audit.gate
                && operation.contains(gate.stage)
            {
                gate.entered.notify_one();
                gate.permits.acquire().await.unwrap().forget();
            }
            inner.oneshot(request).await
        })
    }
}
#[derive(Clone)]
struct Append;
#[derive(Clone)]
struct Appended<S>(S);
impl<S> Layer<S> for Append {
    type Service = Appended<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Appended(inner)
    }
}
impl<S> Service<Request<LogicalMessage>> for Appended<S>
where
    S: Service<Request<LogicalMessage>, Response = LogicalMessage, Error = GatewayError>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = LogicalMessage;
    type Error = GatewayError;
    type Future = Pin<Box<dyn Future<Output = Result<LogicalMessage>> + Send>>;
    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, mut request: Request<LogicalMessage>) -> Self::Future {
        assert!(request.input.server_seq.is_none());
        request.input.body.push(9);
        let service = self.0.clone();
        Box::pin(service.oneshot(request))
    }
}
fn notification(body: &[u8]) -> LogicalMessage {
    LogicalMessage {
        kind: MessageKind::Notify,
        message_id: 90,
        request_id: 0,
        server_seq: None,
        error_code: 0,
        body: body.to_vec(),
    }
}
struct Fixture {
    address: std::net::SocketAddr,
    manager: SessionManager,
    gateway: tok_gateway::Handle,
    service: tok_lifecycle::Handle,
    task: Option<tokio::task::JoinHandle<std::result::Result<(), tok_lifecycle::Error>>>,
    counts: Arc<Counts>,
    ownership: Arc<MemoryOwnership>,
}
impl Fixture {
    async fn new(gate: Option<Arc<Gate>>) -> Self {
        Self::configured(gate, false).await
    }
    async fn configured(gate: Option<Arc<Gate>>, initialize_failure: bool) -> Self {
        Self::configured_topics(gate, initialize_failure, None).await
    }
    async fn configured_topics(
        gate: Option<Arc<Gate>>,
        initialize_failure: bool,
        topics: Option<Arc<topic_support::TopicTransport>>,
    ) -> Self {
        Self::configured_policy(gate, initialize_failure, topics, None).await
    }
    async fn configured_policy(
        gate: Option<Arc<Gate>>,
        initialize_failure: bool,
        topics: Option<Arc<topic_support::TopicTransport>>,
        policy: Option<Vec<String>>,
    ) -> Self {
        let counts = Arc::new(Counts::default());
        let captured = Arc::clone(&counts);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(sender)));
        let builder = Builder::new(
            Identity {
                node_id: "test".into(),
                service_name: "gateway".into(),
            },
            8,
            CancellationToken::new(),
        )
        .unwrap();
        let service = builder.handle();
        let factory = move |init: Init| -> InitFuture {
            let counts = Arc::clone(&captured);
            let sender = Arc::clone(&sender);
            let gate = gate.clone();
            let topics = topics.clone();
            let policy = policy.clone();
            Box::pin(async move {
                let config = Config {
                    rpc_timeout_ms: 1000,
                    disconnect_grace_ms: 2000,
                    ..Default::default()
                };
                let ownership = Arc::new(MemoryOwnership::new(config.max_sessions)?);
                let executor: Arc<dyn Executor> = Arc::new(Execution(Arc::clone(&counts)));
                let audit = Audit {
                    counts: Arc::clone(&counts),
                    gate,
                };
                let pipeline = player::Pipeline::new(Arc::clone(&executor))
                    .with_admission_layer(audit.clone())
                    .with_request_layer(audit.clone())
                    .with_event_layer(audit.clone())
                    .with_timer_layer(audit.clone())
                    .with_downlink_layer(audit)
                    .with_downlink_layer(Append);
                let gateway = Gateway::new_in(config.gateway_limits(), &init, |handle| {
                    let mut capabilities = Extensions::default();
                    capabilities.insert(ManagerData);
                    let ingress = Ingress::new(
                        handle,
                        config,
                        SessionOwner {
                            node_id: "test".into(),
                            service_name: "gateway".into(),
                        },
                        Arc::new(Auth),
                        Arc::<MemoryOwnership>::clone(&ownership),
                        executor,
                    )?
                    .with_extensions(capabilities)
                    .with_pipeline(pipeline)
                    .with_session_extension(Arc::new(StateFactory {
                        counts,
                        initialize_failure,
                    }))?;
                    let ingress = match policy {
                        Some(topics) => ingress.with_topic_policy(move |request| {
                            assert_eq!(request.input.player_id, "1");
                            Ok(topics.clone())
                        }),
                        None => ingress,
                    };
                    match &topics {
                        Some(topics) => ingress.with_topic_interests(
                            topics.pool(init.service.tasks()),
                            vec!["a".into(), "b".into()],
                        ),
                        None => Ok(ingress),
                    }
                })?;
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                sender
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send((
                        address,
                        gateway.ingress().sessions(),
                        gateway.handle(),
                        ownership,
                    ))
                    .unwrap_or_else(|_| panic!("测试观察端已退出"));
                Ok(Box::pin(async move {
                    gateway.serve(listener, init.service.cancellation()).await?;
                    Ok(())
                }) as Task)
            })
        };
        let mut extensions = Extensions::default();
        extensions.insert(GatewayData);
        let prepared = builder
            .build(Arc::new(factory), extensions, Duration::from_secs(2))
            .await
            .unwrap();
        let task = tokio::spawn(prepared.start().run(Duration::from_secs(2)));
        let (address, manager, gateway, ownership) = receiver.await.unwrap();
        Self {
            address,
            manager,
            gateway,
            service,
            task: Some(task),
            counts,
            ownership,
        }
    }
    async fn connect(&self) -> Client {
        Client {
            stream: TcpStream::connect(self.address).await.unwrap(),
            codec: FrameCodec::new(1_048_576, Duration::from_secs(2)).unwrap(),
        }
    }
    async fn stop(mut self) {
        self.service.request_stop();
        self.task.take().unwrap().await.unwrap().unwrap();
        self.manager.wait_finished().await;
        self.gateway.wait_finished().await;
        assert_eq!(self.gateway.phase(), tok_gateway::Phase::Stopped);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.service.request_stop();
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
    async fn read(&mut self) -> Content {
        self.codec
            .read(&mut self.stream)
            .await
            .unwrap()
            .unwrap()
            .content
            .unwrap()
    }
    async fn login(&mut self) -> protocol::LoginResponse {
        self.send(Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "1".into(),
            resume_token: String::new(),
        }))
        .await;
        let Content::LoggedIn(response) = self.read().await else {
            panic!()
        };
        assert_eq!(response.error_code, 0);
        response
    }
    async fn request(&mut self, id: u64, message_id: u32) {
        self.send(Content::Request(protocol::BusinessRequest {
            request_id: id,
            message_id,
            body: b"value".to_vec(),
        }))
        .await;
    }
    async fn resume(&mut self, token: String) {
        self.send(Content::Resume(protocol::ResumeRequest {
            request_id: 1,
            resume_token: token,
            ack_seq: 0,
        }))
        .await;
        assert!(matches!(self.read().await,Content::Resumed(response) if response.error_code==0));
    }
}
#[tokio::test]
async fn scope_capabilities_layers_and_replay_keep_one_downlink_execution() {
    let fixture = Fixture::new(None).await;
    let mut client = fixture.connect().await;
    let login = client.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    assert_eq!(session.snapshot().unwrap().phase, SessionPhase::Online);
    assert_eq!(fixture.gateway.snapshot().connections, 1);
    client.request(2, 1).await;
    assert!(
        matches!(client.read().await,Content::Response(response) if response.server_seq==1 && response.body==b"value	")
    );
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    session.notify(notification(b"offline")).await.unwrap();
    assert_eq!(session.snapshot().unwrap().phase, SessionPhase::Detached);
    let mut client = fixture.connect().await;
    client.resume(login.resume_token).await;
    assert!(
        matches!(client.read().await,Content::Response(response) if response.server_seq==1 && response.body==b"value	")
    );
    assert!(
        matches!(client.read().await,Content::Notify(notice) if notice.server_seq==2 && notice.body==b"offline	")
    );
    assert!(matches!(client.read().await, Content::Recovered(_)));
    assert_eq!(fixture.counts.downlink.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.counts.admission.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.counts.request.load(Ordering::SeqCst), 1);
    session.close();
    session.wait_finished().await;
    assert_eq!(session.snapshot().unwrap().phase, SessionPhase::Stopped);
    assert_eq!(
        fixture
            .counts
            .changes
            .lock()
            .unwrap()
            .iter()
            .filter(|c| **c == Change::Closing)
            .count(),
        1
    );
    assert_eq!(
        fixture
            .counts
            .changes
            .lock()
            .unwrap()
            .iter()
            .filter(|c| **c == Change::Closed)
            .count(),
        1
    );
    assert!(
        fixture
            .counts
            .changes
            .lock()
            .unwrap()
            .iter()
            .any(|c| matches!(c, Change::Resumed { .. }))
    );
    fixture.stop().await;
}
#[derive(Clone, PartialEq, prost::Message)]
struct Added {}
impl tok_events::Event for Added {
    const NAME: &'static str = "test.added.v1";
}
#[tokio::test]
async fn request_event_and_timer_share_layers_and_session_capabilities_without_event_responses() {
    let fixture = Fixture::new(None).await;
    let mut client = fixture.connect().await;
    client.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    client.request(2, 80).await;
    assert!(matches!(client.read().await, Content::Response(_)));
    assert!(matches!(client.read().await,Content::Notify(notice) if notice.body==b"tick	"));
    let original = Arc::new(
        tok_events::Message::new(
            &tok_events::Envelope::new(tok_events::EventId::generate(), Added {}).unwrap(),
        )
        .unwrap(),
    );
    session.post_event(Arc::clone(&original)).unwrap();
    client.request(3, 1).await;
    assert!(
        matches!(client.read().await,Content::Response(response) if response.request_id==3 && response.server_seq==3)
    );
    assert_eq!(fixture.counts.event.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.counts.timer.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(
        &fixture.counts.events.lock().unwrap()[0],
        &original
    ));
    let state = session.extensions().get::<SessionData>().unwrap();
    assert_eq!(state.value.load(Ordering::SeqCst), 112);
    assert_eq!(state.node, "test");
    fixture.stop().await;
}
#[tokio::test]
async fn async_admission_cannot_submit_into_a_replaced_session() {
    let gate = Arc::new(Gate {
        stage: "/admission/",
        entered: Notify::new(),
        permits: Semaphore::new(0),
    });
    let fixture = Fixture::new(Some(Arc::clone(&gate))).await;
    let mut first = fixture.connect().await;
    first.login().await;
    let old = fixture.manager.session("1").unwrap().unwrap();
    first.request(2, 1).await;
    gate.entered.notified().await;
    let mut second = fixture.connect().await;
    second.login().await;
    old.wait_finished().await;
    assert_eq!(fixture.counts.request.load(Ordering::SeqCst), 0);
    assert!(old.cancellation().is_cancelled());
    assert_ne!(
        fixture.manager.session("1").unwrap().unwrap().identity(),
        old.identity()
    );
    fixture.stop().await;
}
#[tokio::test]
async fn topic_membership_is_rechecked_after_async_downlink_before_assigning_sequence() {
    let gate = Arc::new(Gate {
        stage: "/downlink/",
        entered: Notify::new(),
        permits: Semaphore::new(0),
    });
    let fixture = Fixture::new(Some(Arc::clone(&gate))).await;
    let mut client = fixture.connect().await;
    client.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    session.join_topic("room").await.unwrap();
    let manager = fixture.manager.clone();
    let delivery = tokio::spawn(async move {
        manager
            .deliver(&player::events::Delivery {
                target: player::events::Target::Topic("room".into()),
                action: player::events::Action::Notify {
                    message_id: 90,
                    body: vec![1],
                },
            })
            .await
            .unwrap()
    });
    gate.entered.notified().await;
    session.leave_topic("room").await.unwrap();
    gate.permits.add_permits(1);
    let report = delivery.await.unwrap();
    assert_eq!(report.skipped, 1);
    assert_eq!(session.snapshot().unwrap().last_server_seq, 0);
    fixture.stop().await;
}

#[tokio::test]
async fn extension_initialization_failure_releases_acquired_ownership_without_publishing_session() {
    let fixture = Fixture::configured(None, true).await;
    let mut client = fixture.connect().await;
    client
        .send(Content::Login(protocol::LoginRequest {
            request_id: 1,
            credential: "1".into(),
            resume_token: String::new(),
        }))
        .await;
    assert!(
        matches!(client.read().await,Content::LoggedIn(response) if response.error_code==ErrorCode::Internal as u32)
    );
    assert!(fixture.ownership.lookup("1").await.unwrap().is_none());
    assert_eq!(fixture.manager.snapshot().unwrap().sessions, 0);
    assert_eq!(fixture.counts.request.load(Ordering::SeqCst), 0);
    fixture.stop().await;
}

#[tokio::test]
async fn reconnect_cancels_old_binding_admission_without_closing_the_logical_session() {
    let gate = Arc::new(Gate {
        stage: "/admission/",
        entered: Notify::new(),
        permits: Semaphore::new(0),
    });
    let fixture = Fixture::new(Some(Arc::clone(&gate))).await;
    let mut first = fixture.connect().await;
    let login = first.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    first.request(2, 1).await;
    gate.entered.notified().await;
    let mut second = fixture.connect().await;
    second.resume(login.resume_token).await;
    assert!(matches!(second.read().await, Content::Recovered(_)));
    tokio::time::timeout(Duration::from_millis(200), async {
        while fixture.gateway.snapshot().connections > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!session.cancellation().is_cancelled());
    assert_eq!(fixture.counts.request.load(Ordering::SeqCst), 0);
    gate.permits.add_permits(1);
    second.request(3, 1).await;
    assert!(
        matches!(second.read().await,Content::Response(response) if response.request_id==3 && response.server_seq==1)
    );
    fixture.stop().await;
}

#[tokio::test]
async fn pending_topic_join_is_cancelled_by_leave_and_reconnect_without_stale_membership() {
    let transport = Arc::new(topic_support::TopicTransport::new());
    let fixture = Fixture::configured_topics(None, false, Some(Arc::clone(&transport))).await;
    let mut first = fixture.connect().await;
    let login = first.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    let join = tokio::spawn({
        let session = session.clone();
        async move { session.join_topic("room").await }
    });
    transport.entered.notified().await;
    session.leave_topic("room").await.unwrap();
    assert!(join.await.unwrap().is_err());
    assert!(session.snapshot().unwrap().topics.is_empty());
    let next = tokio::spawn({
        let session = session.clone();
        async move { session.join_topic("room").await }
    });
    transport.entered.notified().await;
    let mut second = fixture.connect().await;
    second.resume(login.resume_token).await;
    assert!(matches!(second.read().await, Content::Recovered(_)));
    assert!(next.await.unwrap().is_err());
    transport.permits.add_permits(4);
    session.join_topic("room").await.unwrap();
    assert_eq!(session.snapshot().unwrap().topics, vec!["room"]);
    session.leave_topic("room").await.unwrap();
    assert_eq!(transport.active.load(Ordering::SeqCst), 0);
    fixture.stop().await;
}
#[tokio::test]
async fn partially_ready_topic_bindings_roll_back_without_publishing_membership() {
    let transport = Arc::new(topic_support::TopicTransport::new());
    transport.fail_at.store(2, Ordering::SeqCst);
    transport.permits.add_permits(2);
    let fixture = Fixture::configured_topics(None, false, Some(Arc::clone(&transport))).await;
    let mut client = fixture.connect().await;
    let login = client.login().await;
    let session = fixture.manager.session("1").unwrap().unwrap();
    assert!(session.join_topic("room").await.is_err());
    assert!(session.snapshot().unwrap().topics.is_empty());
    tokio::time::timeout(Duration::from_secs(1), async {
        while transport.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    transport.permits.add_permits(2);
    session.join_topic("room").await.unwrap();
    assert_eq!(transport.active.load(Ordering::SeqCst), 2);
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), async {
        while session.snapshot().unwrap().phase != SessionPhase::Detached {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(transport.active.load(Ordering::SeqCst), 2);
    let starts = transport.starts.load(Ordering::SeqCst);
    let mut resumed = fixture.connect().await;
    resumed.resume(login.resume_token).await;
    assert!(matches!(resumed.read().await, Content::Recovered(_)));
    assert_eq!(transport.starts.load(Ordering::SeqCst), starts);
    session.close();
    session.wait_finished().await;
    fixture.stop().await;
    assert_eq!(transport.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn trusted_initial_topics_are_ready_before_login_and_failure_closes_unpublished_session() {
    for failure in [false, true] {
        let transport = Arc::new(topic_support::TopicTransport::new());
        transport.permits.add_permits(2);
        if failure {
            transport.fail_at.store(2, Ordering::SeqCst);
        }
        let fixture = Fixture::configured_policy(
            None,
            false,
            Some(Arc::clone(&transport)),
            Some(vec!["realm:trusted".into()]),
        )
        .await;
        let mut client = fixture.connect().await;
        client
            .send(Content::Login(protocol::LoginRequest {
                request_id: 1,
                credential: "1".into(),
                resume_token: String::new(),
            }))
            .await;
        let Content::LoggedIn(response) = client.read().await else {
            panic!()
        };
        if failure {
            assert_ne!(response.error_code, 0);
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if fixture.ownership.lookup("1").await.unwrap().is_none()
                        && transport.active.load(Ordering::SeqCst) == 0
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            assert_eq!(response.error_code, 0);
            assert_eq!(transport.active.load(Ordering::SeqCst), 2);
            assert_eq!(
                fixture
                    .manager
                    .session("1")
                    .unwrap()
                    .unwrap()
                    .snapshot()
                    .unwrap()
                    .topics,
                vec!["realm:trusted"]
            );
        }
        fixture.stop().await;
        assert_eq!(transport.active.load(Ordering::SeqCst), 0);
    }
}
