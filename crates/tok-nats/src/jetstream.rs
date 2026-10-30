//! JetStream 持久事件消费。stream 由部署方预先创建和管理。
//!
//! 仅当处理链返回 Handled 才确认；忽略、拒绝、部分完成、超时及错误会请求重投。
//! 处理者必须确保成功结果代表本次职责已完成，不能把进入内存队列当作业务完成。
//! 消费确认失败或进程崩溃仍可能重投，业务必须保证幂等。达到最大投递次数后需运维处理。
//! 不自动删除 durable，也不提供玩家离线加载或事务性去重。
//! 同一 durable 的过滤集合固定；动态兴趣需要业务设计独立的 durable 生命周期，
//! 不应直接用同一 Consumer 绑定多个动态 topic。
use crate::nats;
use async_nats::jetstream::consumer::{AckPolicy, pull};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tok_events::{self as events, Error, Message, Outcome, Subscriber, Task, interest::Transport};
use tok_execution::Request;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// 持久消费配置；durable 名称决定消费进度和实例间共享关系。
#[derive(Clone, Debug)]
pub struct Config {
    /// 已存在的 stream 名称；须覆盖 `tok.persistent.<namespace>.>`。
    pub stream: String,
    /// 稳定 durable 名称；相同名称的订阅共享进度并分担消息。
    pub consumer: String,
    /// 未确认消息重投的等待时间，应大于处理预算。
    pub ack_wait: Duration,
    /// 单条消息最多投递次数；达到上限后需由运维处理。
    pub max_deliver: i64,
    /// 处理失败后发出 NAK 前的重投延迟。
    pub retry_delay: Duration,
}

/// 使用同一 NATS 连接的持久订阅适配；不自动创建或删除 stream。
#[derive(Clone)]
pub struct Consumer {
    bus: nats::Bus,
    config: Config,
}
impl Consumer {
    /// 配置一个稳定 durable；stream 及保留策略由部署方负责。
    pub fn new(bus: nats::Bus, config: Config) -> Result<Self, Error> {
        if config.stream.is_empty()
            || config.consumer.is_empty()
            || !config
                .stream
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || !config
                .consumer
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || config.stream.len() > 128
            || config.consumer.len() > 128
            || config.ack_wait <= Duration::from_millis(bus.config.handler_timeout_ms)
            || config.max_deliver < 1
            || config.retry_delay.is_zero()
        {
            return Err(Error::InvalidInput("JetStream 消费配置无效"));
        }
        Ok(Self { bus, config })
    }
}
impl Transport for Consumer {
    fn subscribe(
        &self,
        subscriber: Subscriber,
        stop: CancellationToken,
    ) -> Task<'_, Task<'static, ()>> {
        Box::pin(async move {
            self.bus.healthy()?;
            let names = subscriber.event_types();
            if names.is_empty() || names.len() > self.bus.config.max_subscriptions {
                return Err(Error::InvalidInput("订阅事件类型不能为空"));
            }
            let _setup = self
                .bus
                .setups
                .try_acquire()
                .map_err(|_| Error::CapacityExceeded)?;
            let count = u32::try_from(names.len()).map_err(|_| Error::CapacityExceeded)?;
            let bytes = names
                .len()
                .checked_mul(self.bus.config.subscription_capacity)
                .and_then(|n| n.checked_mul(self.bus.config.max_message_bytes))
                .and_then(|n| u32::try_from(n).ok())
                .ok_or(Error::CapacityExceeded)?;
            let capacity = Arc::clone(&self.bus.subscription)
                .try_acquire_many_owned(count)
                .map_err(|_| Error::CapacityExceeded)?;
            let buffer = Arc::clone(&self.bus.buffer)
                .try_acquire_many_owned(bytes)
                .map_err(|_| Error::CapacityExceeded)?;
            if subscriber
                .group()
                .is_some_and(|group| group != self.config.consumer)
            {
                return Err(Error::InvalidInput("消费组必须与 durable 名称一致"));
            }
            let mut filters = Vec::new();
            for name in names {
                events::validate_name(name)?;
                let filter = self.bus.interest(subscriber.topics(), name).replacen(
                    "tok.events.",
                    "tok.persistent.",
                    1,
                );
                if filters.contains(&filter) {
                    return Err(Error::InvalidInput("订阅事件类型重复"));
                }
                filters.push(filter);
            }
            filters.sort();
            let context = async_nats::jetstream::new(self.bus.client.clone());
            let stream = tokio::time::timeout(
                self.bus.config.timeout(),
                context.get_stream(&self.config.stream),
            )
            .await
            .map_err(|_| Error::Unavailable("JetStream stream 查询超时"))?
            .map_err(|_| Error::Unavailable("JetStream stream 不可用"))?;
            let options = pull::Config {
                durable_name: Some(self.config.consumer.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: self.config.ack_wait,
                max_deliver: self.config.max_deliver,
                max_ack_pending: 1,
                filter_subjects: filters.clone(),
                ..Default::default()
            };
            let consumer = tokio::time::timeout(
                self.bus.config.timeout(),
                stream.get_or_create_consumer(&self.config.consumer, options),
            )
            .await
            .map_err(|_| Error::Unavailable("JetStream 消费者建立超时"))?
            .map_err(|_| Error::Unavailable("JetStream 消费者建立失败"))?;
            let actual = &consumer.cached_info().config;
            let mut existing = actual.filter_subjects.clone();
            if !actual.filter_subject.is_empty() {
                existing.push(actual.filter_subject.clone());
            }
            existing.sort();
            if existing != filters
                || actual.ack_policy != AckPolicy::Explicit
                || actual.ack_wait != self.config.ack_wait
                || actual.max_deliver != self.config.max_deliver
                || actual.max_ack_pending != 1
            {
                return Err(Error::InvalidInput("已有 JetStream 消费者配置不匹配"));
            }
            let messages = tokio::time::timeout(self.bus.config.timeout(), consumer.messages())
                .await
                .map_err(|_| Error::Unavailable("JetStream 拉取消费启动超时"))?
                .map_err(|_| Error::Unavailable("JetStream 拉取消费启动失败"))?;
            let bus = self.bus.clone();
            let retry_delay = self.config.retry_delay;
            Ok(Box::pin(async move {
                let (_capacity, _buffer) = (capacity, buffer);
                run(bus, subscriber, messages, stop, retry_delay).await
            }) as Task<'static, ()>)
        })
    }
}

async fn run(
    bus: nats::Bus,
    subscriber: Subscriber,
    mut messages: pull::Stream,
    stop: CancellationToken,
    retry_delay: Duration,
) -> Result<(), Error> {
    loop {
        let received = tokio::select! { biased;
            () = stop.cancelled() => return Ok(()),
            () = bus.failed.cancelled() => return Err(Error::Unavailable("NATS 连接失效")),
            item = messages.next() => item.ok_or(Error::Unavailable("JetStream 消费结束"))?
                .map_err(|_| Error::Unavailable("JetStream 拉取消费失败"))?,
        };
        if received.length > bus.config.max_message_bytes {
            return Err(Error::InvalidInput("JetStream 事件超出上限"));
        }
        let mut message = Message::decode(&received.payload, bus.config.max_message_bytes)?;
        let subject = bus.subject(&message.topic, &message.event_type).replacen(
            "tok.events.",
            "tok.persistent.",
            1,
        );
        if !subscriber.topics().matches(&message.topic) || subject != received.subject.as_str() {
            return Err(Error::InvalidInput("JetStream 事件路由不匹配"));
        }
        message.trace_context = events::TraceContext {
            traceparent: received
                .headers
                .as_ref()
                .and_then(|h| h.get("traceparent"))
                .map(ToString::to_string),
            tracestate: received
                .headers
                .as_ref()
                .and_then(|h| h.get("tracestate"))
                .map(ToString::to_string),
        };
        message.trace_context.validate()?;
        let message = Arc::new(message);
        let span = tracing::info_span!("persistent_event", event_id = %message.event_id, event_type = %message.event_type);
        if let Ok(parent) = message.trace_context.extract() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "追踪关联失败不阻断事件处理。"
            )]
            let _ = span.set_parent(parent);
        }
        let context = tok_execution::Context::new(
            subscriber.service().clone(),
            format!("event/{}", message.event_type),
            Duration::from_millis(bus.config.handler_timeout_ms),
            stop.child_token(),
        )
        .map_err(|_| Error::InvalidInput("事件执行上下文无效"))?
        .with_span(span.clone());
        let mut request = Request::new(context, Arc::clone(&message))
            .with_extensions(subscriber.extensions().clone());
        request.extensions_mut().insert(message.event_id.clone());
        request.extensions_mut().insert(message.topic.clone());
        request.extensions_mut().insert(Arc::clone(&message));
        let outcome = tokio::select! { biased;
            () = stop.cancelled() => return Ok(()),
            () = bus.failed.cancelled() => return Err(Error::Unavailable("NATS 连接失效")),
            result = tokio::time::timeout(Duration::from_millis(bus.config.handler_timeout_ms), subscriber.handle(request)).instrument(span) => result,
        };
        let kind = match outcome {
            Ok(Ok(response)) if matches!(response.output, Outcome::Handled) => {
                async_nats::jetstream::AckKind::Ack
            }
            _ => async_nats::jetstream::AckKind::Nak(Some(retry_delay)),
        };
        tokio::time::timeout(bus.config.timeout(), received.double_ack_with(kind))
            .await
            .map_err(|_| Error::Unavailable("JetStream 消费确认超时，结果未知"))?
            .map_err(|_| Error::Unavailable("JetStream 消费确认失败"))?;
    }
}
