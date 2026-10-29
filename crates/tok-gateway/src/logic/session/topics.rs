//! 会话成员与所选订阅组件的绑定；不依赖具体 NATS 适配。
use crate::{ErrorCode, Result};
use tok_events::interest::{Lease, Pool, WeakPool};

#[derive(Clone)]
pub(super) struct Bindings {
    pool: WeakPool,
    components: Vec<String>,
}
impl Bindings {
    pub(super) fn new(pool: &Pool, components: Vec<String>) -> Result<Self> {
        let unique: std::collections::BTreeSet<_> = components.iter().collect();
        if components.is_empty()
            || unique.len() != components.len()
            || components.iter().any(|name| !pool.contains(name))
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(Self {
            pool: pool.downgrade(),
            components,
        })
    }
    pub(super) async fn acquire(&self, name: &str) -> Result<Vec<Lease>> {
        let topic = tok_events::Topic::named(name).map_err(|_| ErrorCode::InvalidRequest)?;
        let pool = self.pool.upgrade().ok_or(ErrorCode::Unavailable)?;
        let mut leases = Vec::with_capacity(self.components.len());
        for component in &self.components {
            leases.push(pool.acquire(component, topic.clone()).await.map_err(
                |error| match error {
                    tok_events::Error::CapacityExceeded => ErrorCode::CapacityExceeded,
                    _ => ErrorCode::Unavailable,
                },
            )?);
        }
        Ok(leases)
    }
}
