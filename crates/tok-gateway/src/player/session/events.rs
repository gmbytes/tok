//! 即时事件的本地目标筛选；广播进入下行，转发进入 session 原有队列。
use super::SessionManager;
use crate::player::events::{Action, Delivery, Target};
use crate::{ErrorCode, Result};
use std::{collections::BTreeSet, sync::Arc};

/// 单网关的准入结果，不代表转发业务已完成，也不作为同步业务确认回传发布者。
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Report {
    /// 已发布下行或已进入 session 队列的数量。
    pub admitted: usize,
    /// 已选目标中离线、已退出主题、关闭或冻结而跳过的数量；非主题成员不计入。
    pub skipped: usize,
    /// 容量、帧大小或其他准入错误导致拒绝的数量。
    pub rejected: usize,
}
impl SessionManager {
    /// 异步分发至本地会话；主题只查询反向索引，每 64 个目标让出调度。
    /// 显式玩家列表去重，未知玩家不创建会话或所有权。取消后已准入的投递不回滚。
    pub async fn deliver(&self, request: tok_execution::Request<Delivery>) -> Result<Report> {
        use tower::{Layer as _, ServiceExt as _};
        tok_execution::Layer
            .layer(tower::service_fn(|request| self.deliver_inner(request)))
            .oneshot(request)
            .await
            .map_err(super::worker::execution_error)
    }
    async fn deliver_inner(&self, request: tok_execution::Request<Delivery>) -> Result<Report> {
        let tok_execution::Request {
            context,
            input: delivery,
        } = request;
        delivery
            .validate(self.inner.config.max_frame_size.min(1_048_576))
            .map_err(|_| ErrorCode::InvalidRequest)?;
        let mut report = Report::default();
        let (sessions, topic) = match &delivery.target {
            Target::Players(players) => {
                let mut sessions = Vec::new();
                for player in players.iter().collect::<BTreeSet<_>>() {
                    if let Some(session) = self.session(player)? {
                        sessions.push(session);
                    } else {
                        report.skipped += 1;
                    }
                }
                (sessions, None)
            }
            Target::Topic(topic) => (
                self.inner.subscriptions.members(topic)?,
                Some(topic.as_str()),
            ),
            Target::All => (self.sessions()?, None),
        };
        for (position, session) in sessions.into_iter().enumerate() {
            if position > 0 && position % 64 == 0 {
                tokio::task::yield_now().await;
            }
            context.check().map_err(|reason| {
                super::worker::execution_error(tok_execution::Error::Stopped(reason))
            })?;
            let result = match &delivery.action {
                Action::Notify { message_id, body } => {
                    session
                        .notify_event(*message_id, body, topic, &context)
                        .await
                }
                Action::Forward(event) => {
                    session.enqueue_event(Arc::clone(event), topic, Some(&context))
                }
            };
            match result {
                Ok(()) => report.admitted += 1,
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::InvalidState
                            | ErrorCode::SessionClosed
                            | ErrorCode::SessionReplaced
                    ) =>
                {
                    report.skipped += 1
                }
                Err(_) => report.rejected += 1,
            }
        }
        metrics::counter!("tok_gateway_events_total", "result" => "admitted")
            .increment(report.admitted as u64);
        metrics::counter!("tok_gateway_events_total", "result" => "skipped")
            .increment(report.skipped as u64);
        metrics::counter!("tok_gateway_events_total", "result" => "rejected")
            .increment(report.rejected as u64);
        if report.rejected > 0 {
            tracing::error!(rejected = report.rejected, "即时事件准入失败，不重试");
        }
        Ok(report)
    }
}
