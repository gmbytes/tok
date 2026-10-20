use super::{RETRY, unavailable, validate};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::{
    Error, Result,
    coordination::{CoordinationTask, LockBackend, LockKey, LockLease},
};
use tokio::{sync::Semaphore, time::Instant};

type Key = (String, String);
struct Entry {
    token: String,
    deadline: Instant,
}
struct Shared {
    entries: Mutex<BTreeMap<Key, Entry>>,
    capacity: usize,
    pending: Semaphore,
}

/// 有界的单进程请求锁；克隆共享状态，进程之间不互斥。
#[derive(Clone)]
pub struct Memory(Arc<Shared>);
impl Memory {
    /// 设置活动键和同时获取操作各自的容量；零容量无效。
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidConfig("请求锁容量无效".into()));
        }
        Ok(Self(Arc::new(Shared {
            entries: Mutex::new(BTreeMap::new()),
            capacity,
            pending: Semaphore::new(capacity),
        })))
    }
    fn attempt(&self, key: &Key, token: &str, ttl: Duration) -> Result<bool> {
        let mut entries = self.0.entries.lock().map_err(|_| unavailable())?;
        let now = Instant::now();
        entries.retain(|_, entry| entry.deadline > now);
        if entries.contains_key(key) {
            return Ok(false);
        }
        if entries.len() >= self.0.capacity {
            return Err(Error::LockCapacity);
        }
        entries.insert(
            key.clone(),
            Entry {
                token: token.into(),
                deadline: now + ttl,
            },
        );
        Ok(true)
    }
}
impl LockBackend for Memory {
    fn acquire(
        &self,
        key: LockKey,
        wait: Duration,
        ttl: Duration,
    ) -> CoordinationTask<'_, Box<dyn LockLease>> {
        Box::pin(async move {
            validate(&key, wait, ttl)?;
            let _permit = self
                .0
                .pending
                .try_acquire()
                .map_err(|_| Error::LockCapacity)?;
            let key = (key.namespace, key.bid);
            let token = uuid::Uuid::new_v4().to_string();
            let deadline = Instant::now() + wait;
            loop {
                if self.attempt(&key, &token, ttl)? {
                    return Ok(Box::new(Lease {
                        shared: Arc::clone(&self.0),
                        key,
                        token,
                    }) as Box<dyn LockLease>);
                }
                if Instant::now() >= deadline {
                    return Err(Error::LockTimeout);
                }
                tokio::time::sleep_until(deadline.min(Instant::now() + RETRY)).await;
                if Instant::now() >= deadline {
                    return Err(Error::LockTimeout);
                }
            }
        })
    }
}
struct Lease {
    shared: Arc<Shared>,
    key: Key,
    token: String,
}
impl LockLease for Lease {
    fn token(&self) -> &str {
        &self.token
    }
    fn release(self: Box<Self>) -> CoordinationTask<'static, ()> {
        Box::pin(async move {
            let mut entries = self.shared.entries.lock().map_err(|_| unavailable())?;
            if entries
                .get(&self.key)
                .is_some_and(|entry| entry.token == self.token)
            {
                entries.remove(&self.key);
            }
            Ok(())
        })
    }
}
