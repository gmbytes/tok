//! Core NATS 通用服务器事件；按事件类型订阅，可显式使用 group 分担消费。

use crate::{Error, Handler, Message};
use async_nats::{Client, Subscriber};
use futures_util::{StreamExt, stream::SelectAll};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// 独立于发现与所有权的 NATS 开发配置；构造和校验不建立网络连接。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// NATS URL，Debug 隐藏凭据。
    pub url: String,
    /// 环境隔离名，只允许 ASCII 字母、数字、短横线和下划线。
    pub namespace: String,
    /// 连接、订阅确认、发布及退出操作的期限，单位毫秒。
    pub operation_timeout_ms: u64,
    /// 单条事件正文与 NATS 追踪头的总字节上限，不超过 1 MiB。
    pub max_message_bytes: usize,
    /// 每个事件类型订阅的待处理条数；慢消费者停用本连接的消费任务，发布仍可使用。
    pub subscription_capacity: usize,
    /// 客户端命令缓冲和同时发布操作各自的容量上限。
    pub client_capacity: usize,
    /// 连接上活动的事件类型订阅总数上限，包含不同 group 的独立订阅。
    pub max_subscriptions: usize,
    /// 订阅缓冲预算，按 类型数 × subscription_capacity × max_message_bytes 预留。
    /// 约束配置范围内消息的正文缓冲，不包含 SDK 元数据、临时编码及无效超大输入。
    pub max_buffer_bytes: usize,
    /// 串行 handler 的等待期限；超时结果未知，不重试，单位毫秒。
    pub handler_timeout_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".into(),
            namespace: "dev".into(),
            operation_timeout_ms: 2000,
            max_message_bytes: 1_048_576,
            subscription_capacity: 64,
            client_capacity: 128,
            max_subscriptions: 32,
            max_buffer_bytes: 128 * 1024 * 1024,
            handler_timeout_ms: 5000,
        }
    }
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("url", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("subscription_capacity", &self.subscription_capacity)
            .field("client_capacity", &self.client_capacity)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("max_buffer_bytes", &self.max_buffer_bytes)
            .field("handler_timeout_ms", &self.handler_timeout_ms)
            .finish()
    }
}
impl Config {
    /// 验证 URL、命名空间和有限容量，不访问 NATS。
    pub fn validate(&self) -> Result<(), Error> {
        if self.url.parse::<async_nats::ServerAddr>().is_err()
            || self.namespace.is_empty()
            || self.namespace.len() > 64
            || !self
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !(1..=60_000).contains(&self.operation_timeout_ms)
            || !(1024..=1_048_576).contains(&self.max_message_bytes)
            || !(1..=60_000).contains(&self.handler_timeout_ms)
            || !(1024..=1_073_741_824).contains(&self.max_buffer_bytes)
            || [
                self.subscription_capacity,
                self.client_capacity,
                self.max_subscriptions,
            ]
            .iter()
            .any(|capacity| !(1..=65536).contains(capacity))
        {
            return Err(Error::InvalidInput("NATS 配置无效"));
        }
        Ok(())
    }
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }
}
/// 一个受有限缓冲和期限约束的连接；克隆共享连接、发布容量和永久失效状态。
///
/// 连接中断后不接受新发布，需重新组装；已发出的消息结果可能未知，不重试业务消息。
#[derive(Clone)]
pub struct Bus {
    client: Client,
    config: Config,
    failed: CancellationToken,
    publishing: Arc<tokio::sync::Semaphore>,
    subscription: Arc<tokio::sync::Semaphore>,
    buffer: Arc<tokio::sync::Semaphore>,
    consumers_failed: CancellationToken,
}
impl Bus {
    /// 连接并等待初始握手；首次失败直接返回，不后台等待初次连接。
    pub async fn connect(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let failed = CancellationToken::new();
        let signal = failed.clone();
        let consumers_failed = CancellationToken::new();
        let consumer_signal = consumers_failed.clone();
        let options = async_nats::ConnectOptions::new()
            .client_capacity(config.client_capacity)
            .subscription_capacity(config.subscription_capacity)
            .connection_timeout(config.timeout())
            .max_reconnects(1)
            .event_callback(move |event| {
                let signal = signal.clone();
                let consumer_signal = consumer_signal.clone();
                async move {
                    if matches!(event, async_nats::Event::SlowConsumer(_)) {
                        consumer_signal.cancel();
                    }
                    if matches!(
                        event,
                        async_nats::Event::Disconnected
                            | async_nats::Event::Closed
                            | async_nats::Event::ServerError(_)
                    ) {
                        signal.cancel();
                    }
                }
            });
        let client = tokio::time::timeout(
            config.timeout(),
            Box::pin(options.connect(config.url.as_str())),
        )
        .await
        .map_err(|_| Error::Unavailable("NATS 连接超时"))?
        .map_err(|_| Error::Unavailable("NATS 连接失败"))?;
        let publishing = Arc::new(tokio::sync::Semaphore::new(config.client_capacity));
        let subscription = Arc::new(tokio::sync::Semaphore::new(config.max_subscriptions));
        let buffer = Arc::new(tokio::sync::Semaphore::new(config.max_buffer_bytes));
        Ok(Self {
            client,
            config,
            failed,
            publishing,
            subscription,
            buffer,
            consumers_failed,
        })
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.failed.is_cancelled()
            || self.client.connection_state() != async_nats::connection::State::Connected
        {
            return Err(Error::Unavailable("NATS 连接已失效"));
        }
        Ok(())
    }
    fn subject(&self, event_type: &str) -> String {
        format!("tok.events.{}.{}", self.config.namespace, event_type)
    }
    // async-nats 0.50 的 flush 只等待本地写出。仅在订阅初始化时通过临时 inbox
    // 自回环确认该连接的订阅命令已被 broker 处理；不逐条确认发布，也不等待业务处理。
    async fn confirm_subscriptions(&self) -> Result<(), Error> {
        let inbox = self.client.new_inbox();
        let mut probe = self
            .client
            .subscribe(inbox.clone())
            .await
            .map_err(|_| Error::Unavailable("NATS 回环订阅失败"))?;
        self.client
            .publish(inbox, Vec::new().into())
            .await
            .map_err(|_| Error::Unavailable("NATS 回环发布失败"))?;
        probe
            .next()
            .await
            .ok_or(Error::Unavailable("NATS 回环确认结束"))?;
        probe
            .unsubscribe()
            .await
            .map_err(|_| Error::Unavailable("NATS 回环清理失败"))?;
        Ok(())
    }
    /// 发布强类型事件；先占用发布容量再编码，成功仅表示客户端接受发布操作。
    pub async fn publish_event<E: crate::Event + prost::Message>(
        &self,
        envelope: crate::Envelope<E>,
    ) -> Result<(), Error> {
        self.healthy()?;
        let _permit = self
            .publishing
            .try_acquire()
            .map_err(|_| Error::CapacityExceeded)?;
        self.send(&Message::new(&envelope)?).await
    }
    /// 发布已有编码事件；成功仅表示客户端接受发布操作，不等待 broker 或订阅者确认。
    /// 断线仍可能丢失事件，不自动重试。
    pub async fn publish(&self, message: &Message) -> Result<(), Error> {
        self.healthy()?;
        let _permit = self
            .publishing
            .try_acquire()
            .map_err(|_| Error::CapacityExceeded)?;
        self.send(message).await
    }
    async fn send(&self, message: &Message) -> Result<(), Error> {
        let bytes = message.encode(self.config.max_message_bytes)?;
        let headers_len = if message.trace_context.traceparent.is_none()
            && message.trace_context.tracestate.is_none()
        {
            0
        } else {
            12
        } + message
            .trace_context
            .traceparent
            .as_ref()
            .map_or(0, |value| value.len() + 15)
            + message
                .trace_context
                .tracestate
                .as_ref()
                .map_or(0, |value| value.len() + 14);
        if bytes.len().saturating_add(headers_len) > self.config.max_message_bytes {
            return Err(Error::InvalidInput("事件与追踪头总字节数超出上限"));
        }
        let mut operation = Operation {
            failed: self.failed.clone(),
            done: false,
        };
        let result = tokio::time::timeout(self.config.timeout(), async {
            let mut headers = async_nats::HeaderMap::new();
            if let Some(value) = &message.trace_context.traceparent {
                headers.insert("traceparent", value.as_str());
            }
            if let Some(value) = &message.trace_context.tracestate {
                headers.insert("tracestate", value.as_str());
            }
            self.client
                .publish_with_headers(self.subject(&message.event_type), headers, bytes.into())
                .await
                .map_err(|_| Error::Unavailable("NATS 发布失败"))
        })
        .await
        .unwrap_or(Err(Error::Unavailable("NATS 发布入队超时")));
        operation.done = result.is_ok();
        result
    }
    /// 安装独立订阅组件，按其处理者声明的事件类型与消费组建立订阅。
    /// 返回前确认该连接上的订阅命令已被 broker 处理，不等待 handler 执行。
    /// 相同 group 的实例分担匹配事件，不提供 bid 路由或业务去重。
    pub async fn subscribe(&self, subscriber: crate::Subscriber) -> Result<Subscription, Error> {
        let crate::Subscriber {
            service,
            extensions,
            handler,
            group,
        } = subscriber;
        let group = group.as_deref();
        self.healthy()?;
        if self.consumers_failed.is_cancelled() {
            return Err(Error::Unavailable(
                "连接的消费缓冲曾溢出，需重新组装消费者连接",
            ));
        }
        if let Some(group) = group {
            crate::validate_name(group)?;
        }
        let names = handler.event_types();
        if names.is_empty() || names.len() > self.config.max_subscriptions {
            return Err(Error::InvalidInput("订阅类型集合为空或超出上限"));
        }
        let mut unique = std::collections::BTreeSet::new();
        for name in &names {
            crate::validate_name(name)?;
            if !unique.insert(*name) {
                return Err(Error::InvalidInput("订阅事件类型重复"));
            }
        }
        let count = u32::try_from(names.len()).map_err(|_| Error::CapacityExceeded)?;
        let bytes = names
            .len()
            .checked_mul(self.config.subscription_capacity)
            .and_then(|count| count.checked_mul(self.config.max_message_bytes))
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(Error::CapacityExceeded)?;
        let permit = Arc::clone(&self.subscription)
            .try_acquire_many_owned(count)
            .map_err(|_| Error::CapacityExceeded)?;
        let buffer = Arc::clone(&self.buffer)
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::CapacityExceeded)?;
        let mut subscribers = SelectAll::new();
        tokio::time::timeout(self.config.timeout(), async {
            for name in names {
                let subject = self.subject(name);
                let subscriber = if let Some(group) = group {
                    self.client.queue_subscribe(subject, group.to_owned()).await
                } else {
                    self.client.subscribe(subject).await
                }
                .map_err(|_| Error::Unavailable("NATS 订阅失败"))?;
                subscribers.push(subscriber);
            }
            self.confirm_subscriptions().await
        })
        .await
        .map_err(|_| Error::Unavailable("NATS 订阅超时"))??;
        Ok(Subscription {
            service,
            extensions,
            bus: self.clone(),
            subscribers,
            handler,
            _permit: permit,
            _buffer: buffer,
        })
    }
    /// 永久停用共享连接并尝试有界 drain；已发布事件不回滚。
    pub async fn close(&self) -> Result<(), Error> {
        self.failed.cancel();
        tokio::time::timeout(self.config.timeout(), self.client.drain())
            .await
            .map_err(|_| Error::Unavailable("NATS 退出超时"))?
            .map_err(|_| Error::Unavailable("NATS 退出失败"))
    }
}
/// 已完成当前连接订阅就绪确认的独立订阅；必须交给运行时监督，不自行 spawn。
pub struct Subscription {
    service: tok_core::service::Identity,
    extensions: tok_core::execution::Extensions,
    bus: Bus,
    subscribers: SelectAll<Subscriber>,
    handler: Arc<dyn Handler>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    _buffer: tokio::sync::OwnedSemaphorePermit,
}
// 发布入队失败或被取消时保守停用连接；Drop 不承诺完成异步 drain。
struct Operation {
    failed: CancellationToken,
    done: bool,
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.done {
            self.failed.cancel();
        }
    }
}
impl Subscription {
    /// 有界接收并串行等待处理；错误记录后跳过，连接异常上报监督者。
    /// 退出或连接失效时取消在途 handler；业务若需原子性应自行保证。
    pub async fn run(mut self, shutdown: CancellationToken) -> Result<(), Error> {
        let result = loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break Ok(()),
                () = self.bus.failed.cancelled() => break Err(Error::Unavailable("NATS 连接失效")),
                () = self.bus.consumers_failed.cancelled() => break Err(Error::Unavailable("NATS 消费缓冲溢出")),
                message = self.subscribers.next() => {
                    let Some(message) = message else { break Err(Error::Unavailable("NATS 订阅结束")); };
                    if message.length > self.bus.config.max_message_bytes { tracing::error!("拒绝超大 NATS 事件"); continue; }
                    let headers = message.headers.as_ref();
                    let trace = crate::TraceContext {
                        traceparent: headers.and_then(|headers| headers.get("traceparent")).map(ToString::to_string),
                        tracestate: headers.and_then(|headers| headers.get("tracestate")).map(ToString::to_string),
                    };
                    if trace.validate().is_err() { tracing::error!("拒绝无效的 NATS 追踪头"); continue; }
                    let wire_subject = message.subject.as_str();
                    let mut message = match Message::decode(&message.payload, self.bus.config.max_message_bytes) {
                        Ok(message) => message,
                        Err(_) => { tracing::error!("拒绝无效的 NATS 事件，不重试"); continue; }
                    };
                    if message.event_type.is_empty() || self.bus.subject(&message.event_type) != wire_subject {
                        tracing::error!("事件正文类型与订阅 subject 不匹配"); continue;
                    }
                    message.trace_context = trace;
                    let message = Arc::new(message);
                    let span = tracing::info_span!("server_event", event_id = %message.event_id, event_type = %message.event_type);
                    if let Ok(parent) = message.trace_context.extract() {
                        #[expect(clippy::let_underscore_must_use, reason="追踪关联失败不能阻断有效业务事件。")]
                        let _ = span.set_parent(parent);
                    }
                    let mut context = tok_core::execution::Context::new(
                        self.service.clone(), format!("event/{}", message.event_type),
                        tok_core::execution::Source::Event { event_id: message.event_id.to_string() },
                        Duration::from_millis(self.bus.config.handler_timeout_ms), shutdown.child_token(),
                    ).map_err(|_| Error::InvalidInput("事件执行上下文无效"))?.with_span(span.clone());
                    *context.extensions_mut() = self.extensions.clone();
                    let request = tok_service::Request::new(context, message);
                    let result = tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break Ok(()),
                        () = self.bus.failed.cancelled() => break Err(Error::Unavailable("NATS 连接失效")),
                        () = self.bus.consumers_failed.cancelled() => break Err(Error::Unavailable("NATS 消费缓冲溢出")),
                        result = tokio::time::timeout(Duration::from_millis(self.bus.config.handler_timeout_ms), self.handler.handle(request)).instrument(span.clone()) => result,
                    };
                    let outcome = match result {
                        Ok(Ok(outcome)) => outcome.label(),
                        Ok(Err(error)) => { tracing::error!(parent: &span, %error, "服务器事件处理失败，不重试"); "error" },
                        Err(_) => { tracing::error!(parent: &span, "服务器事件处理超时，结果未知，不重试"); "timeout" },
                    };
                    metrics::counter!("tok_events_received_total", "stage" => "handler", "result" => outcome).increment(1);
                }
            }
        };
        // 只取消本订阅；共享连接和其他订阅由持有者继续使用。
        let cleanup = tokio::time::timeout(self.bus.config.timeout(), async {
            for subscriber in self.subscribers.iter_mut() {
                subscriber
                    .unsubscribe()
                    .await
                    .map_err(|_| Error::Unavailable("NATS 退订失败"))?;
            }
            Ok::<_, Error>(())
        })
        .await;
        if !matches!(cleanup, Ok(Ok(()))) {
            tracing::error!("NATS 退订未完成，交由订阅析构继续清理");
        }
        result
    }
}
