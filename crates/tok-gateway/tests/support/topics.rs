//! 可控主题传输；所有建立和消费都由真实通用监督范围驱动。
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tok_events::{
    Error, Handler, Message, Outcome, Subscriber, Task, TopicFilter,
    interest::{Pool, Transport},
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
pub struct TopicTransport {
    pub starts: AtomicUsize,
    pub active: Arc<AtomicUsize>,
    pub permits: Semaphore,
    pub entered: Notify,
    pub fail_at: AtomicUsize,
}
impl TopicTransport {
    pub fn new() -> Self {
        Self {
            starts: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            permits: Semaphore::new(0),
            entered: Notify::new(),
            fail_at: AtomicUsize::new(0),
        }
    }
    pub fn pool(self: &Arc<Self>, tasks: tok_service::task::Handle) -> Pool {
        let components = ["a", "b"].map(|name| {
            (
                name.into(),
                Subscriber::new(
                    tok_core::service::Identity {
                        node_id: "test".into(),
                        service_name: "topics".into(),
                    },
                    Arc::new(Ignore),
                    TopicFilter::All,
                    None,
                )
                .unwrap(),
            )
        });
        Pool::new(
            tok_events::interest::Config {
                timeout: std::time::Duration::from_millis(500),
                ..Default::default()
            },
            Arc::<Self>::clone(self),
            tasks,
            components,
        )
        .unwrap()
    }
}
struct Ignore;
impl Handler for Ignore {
    fn event_types(&self) -> Vec<&str> {
        vec!["test.topic"]
    }
    fn handle(&self, _: tok_service::Request<Arc<Message>>) -> Task<'_> {
        Box::pin(async { Ok(Outcome::Ignored) })
    }
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Transport for TopicTransport {
    fn subscribe(&self, _: Subscriber, stop: CancellationToken) -> Task<'_, Task<'static, ()>> {
        Box::pin(async move {
            let n = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            self.entered.notify_one();
            self.permits.acquire().await.unwrap().forget();
            if self.fail_at.load(Ordering::SeqCst) == n {
                return Err(Error::Unavailable("injected topic setup failure"));
            }
            self.active.fetch_add(1, Ordering::SeqCst);
            let active = Active(Arc::clone(&self.active));
            Ok(Box::pin(async move {
                let _active = active;
                stop.cancelled().await;
                Ok(())
            }) as Task<'static, ()>)
        })
    }
}
