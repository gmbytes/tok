//! P2：无端点普通服务和 TCP gateway 复用工厂、监督、阶段及结束等待。
#![deny(clippy::unwrap_used, clippy::expect_used)]
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tok::{
    application::CancellationToken,
    core::{Extensions, service::Identity},
    gateway::{Gateway, GatewayConfig, GatewayIngress, GatewayTask},
    service::{
        Context as ExecutionContext, Layer as ExecutionLayer, Request, Source,
        lifecycle::{Builder, Factory, Init, InitTask},
        task::{BoxError, Task},
    },
};
use tokio::sync::mpsc;
use tower::{Layer, ServiceExt, service_fn};

const TIMEOUT: Duration = Duration::from_secs(2);
#[derive(Clone)]
struct Reports(mpsc::Sender<(&'static str, u64)>);
#[derive(Clone, Default)]
struct Finished(Arc<AtomicBool>);
struct Ingress(Finished);
impl GatewayIngress for Ingress {
    type Connection = tokio::net::TcpStream;
    fn connection(&self, _connection: Self::Connection, stop: CancellationToken) -> GatewayTask {
        Box::pin(async move {
            stop.cancelled().await;
            Ok(())
        })
    }
    fn finish(&self) -> GatewayTask {
        let finished = self.0.clone();
        Box::pin(async move {
            finished.0.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}
fn reports(init: &Init) -> io::Result<Reports> {
    init.extensions
        .get::<Reports>()
        .cloned()
        .ok_or_else(|| io::Error::other("缺少示例结果入口"))
}
fn counter(init: Init) -> InitTask {
    Box::pin(async move {
        // 初始化状态和类型化入口；不要求网络或 session，Layer 用 Rust 组合。
        tokio::task::yield_now().await;
        let mut state = 0_u64;
        let reports = reports(&init)?;
        let rpc = ExecutionLayer.layer(tower::limit::ConcurrencyLimitLayer::new(1).layer(
            tower::util::MapResponseLayer::new(|value| value + 1).layer(service_fn(
                |request: Request<u64>| std::future::ready(Ok::<_, io::Error>(request.input)),
            )),
        ));
        let parent = ExecutionContext::new(
            init.identity.clone(),
            "counter.read",
            Source::Rpc { request_id: 1 },
            TIMEOUT,
            init.service.cancellation(),
        )?;
        let background_stop = init.service.cancellation();
        init.service
            .tasks()
            .launch(
                "background",
                Box::pin(async move {
                    background_stop.cancelled().await;
                    Ok(())
                }),
            )
            .await?;
        Ok(Box::pin(async move {
            let value = rpc.oneshot(Request::new(parent.clone(), 41)).await?;
            let event = parent.child(
                "counter.added",
                Source::Event {
                    event_id: "example:1".into(),
                },
            )?;
            ExecutionLayer
                .layer(service_fn(add))
                .oneshot(Request::new(event, (&mut state, value)))
                .await?;
            reports.0.send(("counter", state)).await?;
            init.service.cancellation().cancelled().await;
            Ok(())
        }) as Task)
    })
}
async fn add(request: Request<(&mut u64, u64)>) -> io::Result<()> {
    tokio::task::yield_now().await;
    *request.input.0 += request.input.1;
    Ok(())
}
fn gateway(init: Init) -> InitTask {
    Box::pin(async move {
        let reports = reports(&init)?;
        let finished = init
            .extensions
            .get::<Finished>()
            .cloned()
            .ok_or_else(|| io::Error::other("缺少示例清理标记"))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let gateway = Gateway::new_in(
            GatewayConfig {
                max_connections: 1,
                max_tasks: 1,
            },
            &init,
            |_| Ok(Ingress(finished)),
        )?;
        let gateway_handle = gateway.handle();
        Ok(Box::pin(async move {
            reports.0.send(("gateway", 0)).await?;
            gateway.serve(listener, init.service.cancellation()).await?;
            gateway_handle.wait_finished().await;
            if gateway_handle.phase() != tok::gateway::Phase::Stopped {
                return Err(io::Error::other("网关未完成退出").into());
            }
            Ok(())
        }) as Task)
    })
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let (sender, mut receiver) = mpsc::channel(2);
    let finished = Finished::default();
    let mut handles = Vec::new();
    let mut running = tokio::task::JoinSet::new();
    let result = async {
        for (name, factory) in [
            ("counter", counter as fn(Init) -> InitTask),
            ("gateway", gateway),
        ] {
            let builder = Builder::new(
                Identity {
                    node_id: "example".into(),
                    service_name: name.into(),
                },
                2,
                CancellationToken::new(),
            )?;
            let handle = builder.handle();
            let mut extensions = Extensions::default();
            extensions.insert(Reports(sender.clone()));
            extensions.insert(finished.clone());
            let factory: Arc<dyn Factory> = Arc::new(factory);
            let task = builder.build(factory, extensions, TIMEOUT).await?;
            let running_service = task.start();
            handles.push(handle);
            running.spawn(running_service.run(TIMEOUT));
        }
        for _ in 0..2 {
            let (name, value) = tokio::time::timeout(TIMEOUT, receiver.recv())
                .await?
                .ok_or_else(|| io::Error::other("示例服务提前退出"))?;
            if name == "counter" && value != 42 {
                return Err(io::Error::other("处理链结果错误").into());
            }
        }
        Ok::<_, BoxError>(())
    }
    .await;
    for handle in &handles {
        handle.request_stop();
    }
    let mut cleanup = Ok(());
    while let Some(result) = running.join_next().await {
        if let Err(error) = result
            .map_err(|e| Box::new(e) as BoxError)
            .and_then(|r| r.map_err(|e| Box::new(e) as BoxError))
        {
            cleanup = Err(error);
        }
    }
    for handle in handles {
        handle.wait_finished().await;
    }
    result?;
    cleanup?;
    if !finished.0.load(Ordering::SeqCst) {
        return Err(io::Error::other("网关未完成清理").into());
    }
    println!("生命周期验证通过：普通服务状态=42，gateway 已完成异步清理，全部任务已释放");
    Ok(())
}
