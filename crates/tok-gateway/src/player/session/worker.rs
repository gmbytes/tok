//! 会话串行执行、timer 驱动、租约续期与退出清理。
use super::{channel, handle::SessionHandle, state::SessionState, timer};
use crate::{
    ErrorCode, Result,
    player::{Config, Executor, Reply},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tok_execution::{Request, Response};
use tok_player::{LogicalMessage, MessageKind, ownership::Store as OwnershipStore};
use tokio::sync::{mpsc, oneshot};
use tower::{Layer as _, ServiceExt};

pub(super) type SessionTask = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(super) enum Job {
    // 按 FIFO 等待前序任务完成后采集执行快照；停止新业务与暂停 timer 由交接流程提前完成。
    Snapshot(oneshot::Sender<Result<tok_message::rpc::protocol::internal::v1::ExecutionSnapshot>>),
    Request(RequestJob),
    Timer(timer::Ticket),
    Event(EventJob),
}
pub(super) struct EventJob {
    pub(super) event: Arc<tok_events::Message>,
    pub(super) scope: Request<()>,
}
pub(super) struct RequestJob {
    pub(super) request: Request<LogicalMessage>,
    // Some 将回应交给等待方；None 通过会话通信通道发布给游戏客户端。
    pub(super) response_sender: Option<oneshot::Sender<Response<LogicalMessage>>>,
}
struct SessionGuard {
    state: Arc<SessionState>,
    // 持有到任务 future 释放；Drop 归还管理器活动计数。
    _activity: Option<super::state::ActivityGuard>,
}
impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.state.cancel.cancel();
        self.state.channel.close();
        let handle = SessionHandle(Arc::clone(&self.state));
        handle.closing();
        handle.changed(super::extension::Change::Closed);
        self.state.completion.done.store(true, Ordering::Release);
        self.state.completion.notify.notify_waiters();
    }
}

pub(super) fn task(
    session_state: Arc<SessionState>,
    mut job_receiver: mpsc::Receiver<Job>,
    config: Arc<Config>,
    ownership_store: Arc<dyn OwnershipStore>,
    executor: Arc<dyn Executor>,
) -> SessionTask {
    let activity = session_state
        .manager
        .upgrade()
        .map(|manager| manager.activities.guard());
    let guard = SessionGuard {
        state: Arc::clone(&session_state),
        _activity: activity,
    };
    Box::pin(async move {
        // 即使任务尚未首次 poll 就被丢弃，也失效入口并唤醒等待的重复登录。
        let _guard = guard;
        let timeout = Duration::from_millis(config.rpc_timeout_ms);
        let ttl = Duration::from_millis(config.ownership_ttl_ms);
        let mut renewal = tokio::time::interval(Duration::from_millis(config.renew_interval_ms));
        renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let timers = &session_state.timers;
        let work = async {
            let mut last_request = session_state
                .initial_execution
                .as_ref()
                .map(|snapshot| snapshot.last_request_id)
                .unwrap_or(0);
            let mut timer_request = session_state
                .initial_execution
                .as_ref()
                .map(|snapshot| snapshot.timer_request_id)
                .unwrap_or(0);
            let mut event_request = session_state
                .initial_execution
                .as_ref()
                .map(|snapshot| snapshot.event_request_id)
                .unwrap_or(0);
            loop {
                tokio::select! {
                    biased;
                    _ = session_state.cancel.cancelled() => break,
                    _ = renewal.tick() => {
                        if !matches!(tokio::time::timeout(timeout, ownership_store.renew(session_state.ownership.clone(), ttl)).await, Ok(Ok(()))) { break; }
                    }
                    job = job_receiver.recv() => {
                        let Some(job) = job else { break; };
                        let job = match job {
                            Job::Snapshot(reply) => {
                                #[expect(clippy::let_underscore_must_use, reason="交接等待方可能已经取消，快照结果不重试，监督期限负责收束。")]
                                let _ = reply.send(timers.snapshot(last_request, timer_request).map(|mut snapshot| { snapshot.event_request_id = event_request; snapshot }));
                                continue;
                            }
                            Job::Event(job) => {
                                if !renew_ownership(&session_state, ownership_store.as_ref(), timeout, ttl).await { break; }
                                let Some(next) = event_request.checked_add(1) else { session_state.cancel.cancel(); break; };
                                event_request = next;
                                let span = tracing::info_span!(parent: job.scope.context.span(), "player_event", player_id = %session_state.ownership.identity.player_id, event_id = %job.event.event_id, event_type = %job.event.event_type);
                                let result = tokio::time::timeout(timeout, execute_event(&session_state,executor.as_ref(),event_request,job.scope,job.event,span)).await;
                                let success = match result {
                                    Ok(Ok(timers_update)) => {
                                        let mut success = true;
                                        if let Some(update) = timers_update.output && let Err(error) = timers.apply(update) {
                                            tracing::error!(code = ?error.code, "事件计时更新无效，关闭会话");
                                            session_state.cancel.cancel(); success = false;
                                        }
                                        success
                                    },
                                    Ok(Err(error)) => { tracing::error!(code = ?error.code, "内部事件执行失败，不重试"); false },
                                    Err(_) => { tracing::error!("内部事件执行超时，不重试"); false },
                                };
                                metrics::counter!("tok_session_events_total", "result" => if success { "ok" } else { "error" }).increment(1);
                                continue;
                            }
                            Job::Request(job) => job,
                            Job::Timer(ticket) => {
                                if !timers.is_current(&ticket) { continue; }
                                if !renew_ownership(&session_state, ownership_store.as_ref(), timeout, ttl).await { break; }
                                let Some((spec, trigger)) = timers.prepare(&ticket) else { continue; };
                                let Some(next) = timer_request.checked_add(1) else { session_state.cancel.cancel(); break; };
                                timer_request = next;
                                let span = tracing::info_span!("player_timer", player_id = %session_state.ownership.identity.player_id, timer_id = %trigger.timer_id, scheduled_at_ms = trigger.scheduled_at_ms, fired_at_ms = trigger.fired_at_ms);
                                let result = tokio::time::timeout(timeout, execute_timer(&session_state,executor.as_ref(),timer_request,spec,trigger,span)).await;
                                let success = match result {
                                    Ok(Ok(reply)) => {
                                        let reply = reply.output;
                                        let mut success = reply.response.as_ref().is_none_or(|response| response.error_code == 0);
                                        if !success { tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家 timer 返回业务错误，未自动重试"); }
                                        if let Some(update) = reply.timers && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            session_state.cancel.cancel();
                                            success = false;
                                        }
                                        success
                                    }
                                    Ok(Err(error)) => { tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "玩家 timer 调用失败，未自动重试"); false }
                                    Err(_) => { tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家 timer 调用超时，未自动重试"); false }
                                };
                                metrics::counter!("tok_session_timers_total", "result" => if success { "ok" } else { "error" }).increment(1);
                                timers.complete_tick(&ticket);
                                continue;
                            }
                        };
                        let (context, mut request, extensions) = job.request.into_parts();
                        let scope = Request::from_parts(context, (), extensions);
                        let response = if session_state.cancel.is_cancelled() {
                            Response::new(channel::error_response(&request, ErrorCode::SessionClosed))
                        } else if request.request_id <= last_request {
                            Response::new(channel::error_response(&request, ErrorCode::InvalidRequest))
                        } else {
                            last_request = request.request_id;
                            if !renew_ownership(&session_state, ownership_store.as_ref(), timeout, ttl).await {
                                Response::new(channel::error_response(&request, ErrorCode::SessionClosed))
                            } else {
                                let span = tracing::info_span!(parent: scope.context.span(), "client_request", player_id = %session_state.ownership.identity.player_id, request_id = request.request_id, message_id = request.message_id);
                                let result = tokio::time::timeout(timeout, execute_request(&session_state,executor.as_ref(),&scope,request.clone(),span)).await;
                                match result {
                                    Ok(Ok(reply)) => {
                                        let (reply, extensions) = reply.into_parts();
                                        match reply {
                                        Reply { response: Some(mut response), timers: timer_update } if response.kind == MessageKind::Response => {
                                        if let Some(update) = timer_update && let Err(error) = timers.apply(update) {
                                            tracing::error!(player_id = %session_state.ownership.identity.player_id, code = ?error.code, "计时更新无效，关闭会话");
                                            session_state.cancel.cancel();
                                            Response::new(channel::error_response(&request, ErrorCode::Internal))
                                        } else {
                                            response.request_id = request.request_id; response.server_seq = None; Response::from_parts(response, extensions)
                                        }
                                    }
                                        _ => Response::new(channel::error_response(&request, ErrorCode::Internal)),
                                        }
                                    },
                                    Ok(Err(error)) => Response::new(channel::error_response(&request, error.code)),
                                    Err(_) => {
                                        tracing::error!(player_id = %session_state.ownership.identity.player_id, request_id = request.request_id, "玩家调用超时，未自动重试");
                                        Response::new(channel::error_response(&request, ErrorCode::Timeout))
                                    }
                                }
                            }
                        };
                        // 尽早释放请求正文，慢下行不会持有两份业务缓冲。
                        request.body.clear();
                        metrics::counter!("tok_session_requests_total", "result" => if response.output.error_code == 0 { "ok" } else { "error" }).increment(1);
                        if let Some(response_sender) = job.response_sender {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "独立回应等待方可以取消，业务已经执行，不重试回应或业务。"
                            )]
                            let _ = response_sender.send(response);
                        } else {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "publish_response 在发布失败时已关闭会话，不重复处理或重执业务。"
                            )]
                            let _ = SessionHandle(Arc::clone(&session_state))
                                .publish_response(response.output, request.message_id, &scope).await;
                        }
                    }
                }
            }
            session_state.cancel.cancel();
            // 宽限期结束、重复登录或关闭后，未开始的任务只失效，不重新执行。
            while let Ok(job) = job_receiver.try_recv() {
                if let Job::Request(job) = job
                    && let Some(response_sender) = job.response_sender
                {
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "退出时仅尽力通知尚存的回应等待方，接收方允许先取消。"
                    )]
                    let _ = response_sender.send(Response::new(channel::error_response(
                        &job.request.input,
                        ErrorCode::SessionClosed,
                    )));
                }
            }
            if !matches!(
                tokio::time::timeout(
                    timeout,
                    ownership_store.release(session_state.ownership.clone())
                )
                .await,
                Ok(Ok(_))
            ) {
                tracing::error!(player_id = %session_state.ownership.identity.player_id, "玩家会话条件释放失败");
            }
        };
        tokio::join!(
            work,
            timers.run(
                session_state.job_sender.clone(),
                session_state.cancel.clone()
            ),
            session_state.channel.run_grace_timer()
        );
    })
}

pub(super) async fn renew_ownership(
    session_state: &SessionState,
    ownership_store: &dyn OwnershipStore,
    timeout: Duration,
    ttl: Duration,
) -> bool {
    let started = tokio::time::Instant::now();
    let renewed = matches!(
        tokio::time::timeout(
            timeout,
            ownership_store.renew(session_state.ownership.clone(), ttl)
        )
        .await,
        Ok(Ok(()))
    );
    // 本地保守预算检查仍不能排除检查后的进程暂停，不承诺严格 fencing。
    if !renewed || started.elapsed().saturating_add(timeout) >= ttl {
        session_state.cancel.cancel();
    }
    !session_state.cancel.is_cancelled()
}

pub(super) fn execution_error(
    error: tok_execution::Error<crate::GatewayError>,
) -> crate::GatewayError {
    match error {
        tok_execution::Error::Inner(error) => error,
        tok_execution::Error::Stopped(tok_execution::StopReason::Cancelled) => {
            ErrorCode::SessionClosed.into()
        }
        tok_execution::Error::Stopped(tok_execution::StopReason::DeadlineExceeded) => {
            ErrorCode::Timeout.into()
        }
    }
}
async fn execute_request(
    state: &Arc<SessionState>,
    executor: &dyn Executor,
    scope: &Request<()>,
    message: LogicalMessage,
    span: tracing::Span,
) -> Result<Response<Reply>> {
    let context = scope
        .context
        .child(format!("player/request/{}", message.message_id))
        .map_err(|_| ErrorCode::Internal)?;
    let mut request = Request::from_parts(context, (), scope.extensions().clone());
    request.context = request.context.with_span(span);
    request
        .extensions_mut()
        .insert(crate::player::ClientRequest {
            request_id: message.request_id,
            message_id: message.message_id,
        });
    tok_execution::Layer
        .layer(tower::service_fn(|request| {
            executor.execute_request(request)
        }))
        .oneshot(request.map(|()| crate::player::RequestInput {
            identity: state.ownership.identity.clone(),
            message,
        }))
        .await
        .map_err(execution_error)
}
async fn execute_event(
    state: &Arc<SessionState>,
    executor: &dyn Executor,
    request_id: u64,
    mut request: Request<()>,
    event: Arc<tok_events::Message>,
    span: tracing::Span,
) -> Result<Response<Option<tok_player::TimerUpdate>>> {
    request.context = request.context.with_span(span);
    request.extensions_mut().insert(event.event_id.clone());
    request.extensions_mut().insert(event.topic.clone());
    request.extensions_mut().insert(Arc::clone(&event));
    tok_execution::Layer
        .layer(tower::service_fn(|request| executor.execute_event(request)))
        .oneshot(request.map(|()| crate::player::EventInput {
            identity: state.ownership.identity.clone(),
            request_id,
            event,
        }))
        .await
        .map_err(execution_error)
}
async fn execute_timer(
    state: &Arc<SessionState>,
    executor: &dyn Executor,
    request_id: u64,
    spec: tok_player::TimerSpec,
    trigger: tok_player::TimerTrigger,
    span: tracing::Span,
) -> Result<Response<Reply>> {
    let mut request = SessionHandle(Arc::clone(state))
        .execution_scope(format!("player/timer/{}", spec.message_id))?;
    request.context = request.context.with_span(span);
    request.extensions_mut().insert(trigger.clone());
    tok_execution::Layer
        .layer(tower::service_fn(|request| executor.execute_timer(request)))
        .oneshot(request.map(|()| crate::player::TimerInput {
            identity: state.ownership.identity.clone(),
            request_id,
            spec,
            trigger,
        }))
        .await
        .map_err(execution_error)
}
