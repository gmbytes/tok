use std::sync::{Arc, OnceLock};

use opentelemetry::Context;
use tracing::{Dispatch, Event, Subscriber, dispatcher::WeakDispatch};
use tracing_subscriber::{Layer, filter::LevelFilter, registry::LookupSpan};

// 事件回调中再次 get_default 会得到空 dispatcher，因此在订阅者注册时保存弱引用。
// 弱引用不延长 subscriber 的生命周期，也不会形成 subscriber -> Layer -> subscriber 环。
#[derive(Clone, Default)]
pub(crate) struct Resolver(Arc<OnceLock<WeakDispatch>>);

struct Registration(Arc<OnceLock<WeakDispatch>>);

impl<S: Subscriber> Layer<S> for Registration {
    fn on_register_dispatch(&self, dispatch: &Dispatch) {
        drop(self.0.set(dispatch.downgrade()));
    }
}

impl Resolver {
    // 仅注册 dispatcher，不消费埋点；独立关闭此层，准入由日志和追踪层决定。
    pub fn layer<S>(self) -> impl Layer<S>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        Registration(self.0).with_filter(LevelFilter::OFF)
    }

    // 直接使用完整 subscriber 的父级，避免日志层的过滤隐藏实际 span。
    // 调用前释放 span 扩展锁；读取 OTel context 可能启动延迟构建的 SDK span。
    pub fn event_context(&self, event: &Event<'_>) -> Context {
        if event.is_root() {
            return Context::new();
        }
        if let Some(dispatch) = self.0.get().and_then(WeakDispatch::upgrade) {
            let id = event.parent().cloned().or_else(|| {
                event
                    .is_contextual()
                    .then(|| dispatch.current_span().id().cloned())
                    .flatten()
            });
            if let Some(id) = id {
                return tracing_opentelemetry::get_otel_context(&id, &dispatch).unwrap_or_default();
            }
        }
        // 显式父级缺失不回退到其他 span；无 tracing 父级的普通事件可关联原生 OTel 上下文。
        if event.is_contextual() {
            Context::current()
        } else {
            Context::new()
        }
    }
}
