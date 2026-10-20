//! 不依赖 Player 协议的运行时验证：自定义传输、容量、任务监督和取消清理。

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tok_gateway::transport::{AcceptTask, GatewayTransport};
use tok_gateway::{Gateway, GatewayConfig, GatewayContext, GatewayIngress, GatewayTask};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

struct Connection {
    id: u32,
    dropped: mpsc::UnboundedSender<u32>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "析构清理通知的接收方允许已退出，不能在 Drop 中再次失败。"
        )]
        let _ = self.dropped.send(self.id);
    }
}
struct Transport(mpsc::Receiver<Connection>);
impl GatewayTransport for Transport {
    type Connection = Connection;
    fn accept(&mut self) -> AcceptTask<'_, Connection> {
        Box::pin(async move {
            self.0
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("接入源已关闭"))
        })
    }
}
struct Ingress {
    started: mpsc::UnboundedSender<u32>,
    stopped: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
}
impl GatewayIngress for Ingress {
    type Connection = Connection;
    fn connection(&self, connection: Connection, shutdown: CancellationToken) -> GatewayTask {
        let started = self.started.clone();
        Box::pin(async move {
            started.send(connection.id).unwrap();
            assert_ne!(connection.id, 666, "自定义组件连接任务崩溃");
            if connection.id == 0 {
                return Err(std::io::Error::other("仅关闭此连接"));
            }
            shutdown.cancelled().await;
            drop(connection);
            Ok(())
        })
    }
    fn stop(&self) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
    fn finish(&self) -> GatewayTask {
        let finished = Arc::clone(&self.finished);
        Box::pin(async move {
            finished.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}
struct Fixture {
    incoming: mpsc::Sender<Connection>,
    started: mpsc::UnboundedReceiver<u32>,
    dropped: mpsc::UnboundedReceiver<u32>,
    drop_sender: mpsc::UnboundedSender<u32>,
    context: GatewayContext,
    stop: CancellationToken,
    stopped: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}
impl Fixture {
    fn new() -> Self {
        let (incoming, receiver) = mpsc::channel(8);
        let (started, starts) = mpsc::unbounded_channel();
        let (drop_sender, dropped) = mpsc::unbounded_channel();
        let stopped = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut context = None;
        let gateway = Gateway::new(
            GatewayConfig {
                max_connections: 1,
                max_tasks: 1,
            },
            |runtime| {
                context = Some(runtime);
                Ok(Ingress {
                    started,
                    stopped: Arc::clone(&stopped),
                    finished: Arc::clone(&finished),
                })
            },
        )
        .unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(gateway.serve(Transport(receiver), stop.clone()));
        Self {
            incoming,
            started: starts,
            dropped,
            drop_sender,
            context: context.unwrap(),
            stop,
            stopped,
            finished,
            task: Some(task),
        }
    }
    async fn connect(&self, id: u32) {
        self.incoming
            .send(Connection {
                id,
                dropped: self.drop_sender.clone(),
            })
            .await
            .unwrap();
    }
    async fn start(&mut self) -> u32 {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.started.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn drop_id(&mut self) -> u32 {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.dropped.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn result(&mut self) -> std::io::Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[tokio::test]
async fn custom_ingress_uses_runtime_capacity_and_clean_shutdown() {
    let mut fixture = Fixture::new();
    fixture.connect(0).await;
    assert_eq!(fixture.start().await, 0);
    assert_eq!(fixture.drop_id().await, 0);
    fixture.connect(1).await;
    assert_eq!(fixture.start().await, 1);
    fixture.connect(2).await;
    assert_eq!(fixture.drop_id().await, 2);
    assert!(fixture.started.try_recv().is_err());
    let signal = fixture.context.cancellation();
    let (began, running) = tokio::sync::oneshot::channel();
    fixture
        .context
        .launch(Box::pin(async move {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "监督任务可能先被取消，启动确认接收方允许已经退出。"
            )]
            let _ = began.send(());
            signal.cancelled().await;
            Ok(())
        }))
        .await
        .unwrap();
    running.await.unwrap();
    assert_eq!(
        fixture
            .context
            .launch(Box::pin(async { Ok(()) }))
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    fixture.stop.cancel();
    fixture.result().await.unwrap();
    assert!(fixture.stopped.load(Ordering::SeqCst) > 0);
    assert!(fixture.finished.load(Ordering::SeqCst));
    assert!(fixture.context.cancellation().is_cancelled());
}

#[tokio::test]
async fn background_failures_and_connection_panics_stop_the_runtime() {
    for failure in 0..3 {
        let mut fixture = Fixture::new();
        match failure {
            0 => fixture
                .context
                .launch(Box::pin(async {
                    Err(std::io::Error::other("后台失败"))
                }))
                .await
                .unwrap(),
            1 => fixture
                .context
                .launch(Box::pin(async { panic!("后台任务崩溃") }))
                .await
                .unwrap(),
            _ => fixture.connect(666).await,
        }
        assert!(fixture.result().await.is_err());
        assert!(fixture.stopped.load(Ordering::SeqCst) > 0);
        assert!(fixture.finished.load(Ordering::SeqCst));
        assert!(fixture.context.cancellation().is_cancelled());
    }
}

#[tokio::test]
async fn dropping_runtime_future_cancels_ingress_and_background_tasks() {
    let mut fixture = Fixture::new();
    let (started, running) = tokio::sync::oneshot::channel();
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Dropped(Arc::clone(&dropped));
    fixture
        .context
        .launch(Box::pin(async move {
            let _guard = guard;
            #[expect(
                clippy::let_underscore_must_use,
                reason = "退出竞争测试允许等待方先退出，任务守卫负责后续清理。"
            )]
            let _ = started.send(());
            std::future::pending::<()>().await;
            Ok(())
        }))
        .await
        .unwrap();
    running.await.unwrap();
    let task = fixture.task.take().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.context.cancellation().is_cancelled());
    assert!(fixture.stopped.load(Ordering::SeqCst) > 0);
    assert!(!fixture.finished.load(Ordering::SeqCst));
}
