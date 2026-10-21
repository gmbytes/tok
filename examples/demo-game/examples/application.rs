//! 普通 RPC 与无端点事件服务共用 Factory、Request 和同一个 Layer；无需 gateway。
#![deny(clippy::unwrap_used, clippy::expect_used)]
use demo_game::protocol::{
    GameMessage,
    v1::{GetGuildRequest, GetGuildResponse},
};
use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tok::{
    AppConfig,
    application::Application,
    events::{self, Envelope, Event, EventId, Outcome, Registry},
    game_rpc::{
        CallContext, HandlerRegistry, HandlerReply, RpcClients, RpcService,
        protocol::internal::v1::Payload, tonic::Status,
    },
    service::{
        Context, Request, Source,
        lifecycle::{Init, InitFuture},
        sync_handler,
        task::{BoxError, Task},
    },
};
use tokio::sync::{mpsc, watch};
use tower::{Layer, Service};

// 本地事件无需 protobuf；路由使用默认全局作用域。
struct Audit {
    guild_id: u64,
}
impl Event for Audit {
    const NAME: &'static str = "guild.audit.v1";
}
#[derive(Clone)]
struct Count(Arc<AtomicUsize>);
impl<S> Layer<S> for Count {
    type Service = CountService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CountService {
            inner,
            calls: Arc::clone(&self.0),
        }
    }
}
#[derive(Clone)]
struct CountService<S> {
    inner: S,
    calls: Arc<AtomicUsize>,
}
impl<T, S: Service<Request<T>>> Service<Request<T>> for CountService<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.call(request)
    }
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let config = AppConfig::from_toml(&format!(
        "node_id='example'\ndiscovery='memory'\n[[endpoints]]\nname='rpc'\nprotocol='grpc'\naddress='{address}'\n[[services]]\nservice_name='audit'\n[[services]]\nservice_name='guild'\nendpoint='rpc'"
    ))?;
    let mut app = Application::new(config)?;
    let count = Count(Arc::new(AtomicUsize::new(0)));
    let (events, receiver) = mpsc::channel::<Envelope<Audit>>(4);
    let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
    let (processed, mut progress) = watch::channel(0usize);
    app.register_factory("audit", {
        let count = count.clone();
        move |init: Init| -> InitFuture {
            let receiver = Arc::clone(&receiver); let processed = processed.clone(); let count = count.clone();
            Box::pin(async move {
                let mut receiver = receiver.lock().map_err(|_| std::io::Error::other("事件入口锁不可用"))?
                    .take().ok_or_else(|| std::io::Error::other("事件入口已初始化"))?;
                let mut handlers: Registry<Vec<u64>> = Registry::new(1)?;
                handlers.register_service::<Audit, _>(count.layer(sync_handler(|request: Request<events::Input<'_, Vec<u64>, Audit>>| {
                    request.input.state.push(request.input.event.payload.guild_id);
                    Ok::<_, events::Error>(Outcome::Handled)
                })))?;
                Ok(Box::pin(async move {
                    let mut state = Vec::new();
                    let stop = init.service.cancellation();
                    loop { tokio::select! { biased;
                        () = stop.cancelled() => return Ok(()),
                        event = receiver.recv() => {
                            let event = event.ok_or_else(|| std::io::Error::other("事件入口提前结束"))?;
                            let context = Context::new(init.identity.clone(), "audit", Source::Internal, Duration::from_secs(2), stop.child_token())?;
                            handlers.dispatch(&mut state, Request::new(context, &event)).await?;
                            processed.send_replace(state.len());
                        }
                    } }
                }) as Task)
            })
        }
    })?;
    app.register_rpc_factory(
        "guild",
        {
            let count = count.clone();
            move |init: Init| -> InitFuture {
                let events = events.clone();
                let count = count.clone();
                Box::pin(async move {
                    let mut handlers = HandlerRegistry::new();
                    handlers.register_service(
                        GetGuildRequest::MESSAGE_ID,
                        GetGuildResponse::MESSAGE_ID,
                        count.layer(tower::service_fn(
                            move |request: Request<(CallContext, GetGuildRequest)>| {
                                let events = events.clone();
                                async move {
                                    let guild_id = request.input.1.guild_id;
                                    events
                                        .send(
                                            Envelope::new(EventId::generate(), Audit { guild_id })
                                                .map_err(|error| {
                                                    Status::internal(error.to_string())
                                                })?,
                                        )
                                        .await
                                        .map_err(|_| Status::unavailable("审计服务已停止"))?;
                                    Ok(HandlerReply {
                                        message: GetGuildResponse { guild_id },
                                        timers: None,
                                    })
                                }
                            },
                        )),
                    )?;
                    init.extensions
                        .get::<tok::game_rpc::Binding>()
                        .ok_or_else(|| std::io::Error::other("缺少 RPC 绑定"))?
                        .install(&init, Arc::new(handlers))?;
                    Ok(Box::pin(async move {
                        init.service.cancellation().cancelled().await;
                        Ok(())
                    }) as Task)
                })
            }
        },
        RpcService::into_business,
    )?;
    let directory = app.service_directory();
    app.run_until(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let target = loop {
                if let Some(target) = directory
                    .snapshot("guild")
                    .map_err(std::io::Error::other)?
                    .into_iter()
                    .next()
                {
                    break target;
                }
                tokio::task::yield_now().await;
            };
            if !directory
                .snapshot("audit")
                .map_err(std::io::Error::other)?
                .is_empty()
            {
                return Err(std::io::Error::other("无端点服务不应发布地址"));
            }
            let client = tok::game_rpc::business::Client::new(Arc::new(
                RpcClients::new(Duration::from_secs(2), 4).map_err(std::io::Error::other)?,
            ));
            let response = client
                .call(
                    &target,
                    CallContext {
                        node_id: "example".into(),
                        service_name: "guild".into(),
                        bid: Some("7".into()),
                        request_id: 1,
                        session: None,
                        timer: None,
                    },
                    Payload {
                        message_id: GetGuildRequest::MESSAGE_ID,
                        body: GetGuildRequest { guild_id: 7 }.encode_to_vec(),
                    },
                )
                .await
                .map_err(std::io::Error::other)?;
            if response.error.is_some() {
                return Err(std::io::Error::other("RPC 业务执行失败"));
            }
            let payload = response
                .payload
                .ok_or_else(|| std::io::Error::other("RPC 缺少回应正文"))?;
            let guild =
                GetGuildResponse::decode(payload.body.as_slice()).map_err(std::io::Error::other)?;
            if payload.message_id != GetGuildResponse::MESSAGE_ID || guild.guild_id != 7 {
                return Err(std::io::Error::other("RPC 回应与请求不匹配"));
            }
            while *progress.borrow_and_update() < 1 {
                progress.changed().await.map_err(std::io::Error::other)?;
            }
            if count.0.load(Ordering::SeqCst) != 2 {
                return Err(std::io::Error::other("RPC 和事件没有经过相同 Layer"));
            }
            Ok(())
        })
        .await
        .map_err(std::io::Error::other)?
    })
    .await?;
    println!("应用组装验证通过：RPC 与无端点事件服务共用处理链，事件状态借用有效");
    Ok(())
}
