//! P1：普通调用与最小 gateway 能力共享执行链；不绑定端口、不迁移生产执行循环。
#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::{io, sync::Arc, time::Duration};
use tok::{
    application::CancellationToken,
    core::service::Identity,
    execution::{Context, Layer as ExecutionLayer, Request},
    gateway::{Gateway, player},
    infra::ownership::MemoryOwnership,
    lifecycle::task,
    player::ownership::Owner as SessionOwner,
};
use tower::{Layer, ServiceExt, service_fn};

struct Tick;

#[derive(Default)]
struct State {
    value: u64,
}
struct Added {
    amount: u64,
}
struct ReadOnly;
impl player::Executor for ReadOnly {
    fn execute_request(
        &self,
        input: tok::execution::Request<tok::gateway::player::RequestInput>,
    ) -> player::Task<'_> {
        let tok::gateway::player::RequestInput {
            identity: _identity,
            message: _request,
        } = input.input;
        Box::pin(std::future::ready(Err(
            tok::gateway::ErrorCode::Unavailable.into(),
        )))
    }
}
fn context(service: &str, operation: &str) -> Result<Context, tok::execution::ContextError> {
    Context::new(
        Identity {
            node_id: "local".into(),
            service_name: service.into(),
        },
        operation,
        Duration::from_secs(2),
        CancellationToken::new(),
    )
}
async fn add(request: Request<(&mut State, &Added)>) -> io::Result<()> {
    tokio::task::yield_now().await;
    let (state, event) = request.input;
    state.value = state
        .value
        .checked_add(event.amount)
        .ok_or_else(|| io::Error::other("数值溢出"))?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 普通服务不依赖 session；请求、事件及 timer 的结果保留自己的类型。
    let parent = context("counter", "counter.read")?;
    let rpc = ExecutionLayer.layer(service_fn(|request: Request<u64>| {
        std::future::ready(
            request
                .input
                .checked_add(1)
                .ok_or_else(|| io::Error::other("数值溢出")),
        )
    }));
    let response = rpc.oneshot(Request::new(parent.clone(), 41)).await?;
    let mut state = State::default();
    let event = Added { amount: response };
    let event_context = parent.child("counter.added")?;
    ExecutionLayer
        .layer(service_fn(add))
        .oneshot(Request::new(event_context, (&mut state, &event)))
        .await?;
    let mut timer = parent.child("counter.tick")?;
    timer.extensions_mut().insert(Tick);
    let tick = ExecutionLayer
        .layer(service_fn(|request: Request<()>| {
            std::future::ready(Ok::<_, io::Error>(
                request.context.extensions().get::<Tick>().is_some(),
            ))
        }))
        .oneshot(Request::new(timer, ()))
        .await?;

    // gateway 只添加自身能力，同一执行设施没有 gateway 依赖，也未改现有 worker。
    let config = player::Config::default();
    let gateway = Gateway::new(tok_core_identity(), config.gateway_limits(), |tasks| {
        player::Ingress::new(
            tasks,
            config,
            SessionOwner {
                node_id: "local".into(),
                service_name: "gateway".into(),
            },
            Arc::new(demo_game::gateway::DemoAuthenticator),
            Arc::new(MemoryOwnership::new(32).map_err(|_| tok::gateway::ErrorCode::Internal)?),
            Arc::new(ReadOnly),
        )
    })?;
    let mut gateway_context = context("gateway", "gateway.sessions")?;
    drop(gateway_context.extensions_mut().insert(gateway.tasks()));
    drop(
        gateway_context
            .extensions_mut()
            .insert(gateway.ingress().sessions()),
    );
    let query = ExecutionLayer.layer(service_fn(|request: Request<()>| async move {
        let tasks = request
            .context
            .extensions()
            .get::<task::Handle>()
            .ok_or_else(|| io::Error::other("缺少网关能力"))?;
        let sessions = request
            .context
            .extensions()
            .get::<player::SessionManager>()
            .ok_or_else(|| io::Error::other("缺少会话目录"))?;
        if tasks.cancellation().is_cancelled() {
            return Err(io::Error::other("网关已停止"));
        }
        sessions
            .sessions()
            .map(|sessions| sessions.len())
            .map_err(io::Error::other)
    }));
    let sessions = query.oneshot(Request::new(gateway_context, ())).await?;
    if response != 42 || state.value != 42 || !tick || sessions != 0 {
        return Err(io::Error::other("统一执行示例结果不符合预期").into());
    }
    println!(
        "统一执行验证通过：rpc={response}，本地状态={}，timer={tick}，gateway_sessions={sessions}",
        state.value
    );
    Ok(())
}

fn tok_core_identity() -> tok::core::service::Identity {
    tok::core::service::Identity {
        node_id: "local".into(),
        service_name: "gateway".into(),
    }
}
