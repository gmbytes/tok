//! 有界传输兴趣引用；订阅组件按主题共享，不创建独立的任务运行时。
use crate::{Error, Subscriber, Task, Topic, TopicFilter};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tok_lifecycle::task;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

/// 传输适配：返回值表示订阅已就绪；消费 future 必须响应取消并清理订阅。
/// 不定义新的业务处理协议，Subscriber 内仍使用共同 Request / Tower 链。
pub trait Transport: Send + Sync {
    /// 建立订阅并返回待监督的消费 future，不自行 spawn。
    fn subscribe(
        &self,
        subscriber: Subscriber,
        stop: CancellationToken,
    ) -> Task<'_, Task<'static, ()>>;
}
/// 兴趣协调的有限容量；每个实际 SDK 订阅及缓冲另受传输配置限制。
#[derive(Clone, Debug)]
pub struct Config {
    /// 可绑定的订阅组件数。
    pub max_components: usize,
    /// 建立中、活动及退出中的组件/主题组合数。
    pub max_interests: usize,
    /// 同时等待 acquire 的操作数。
    pub max_pending: usize,
    /// 一次引用获取与订阅建立/清理的上限。
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_components: 32,
            max_interests: 128,
            max_pending: 32,
            timeout: Duration::from_secs(5),
        }
    }
}
/// 兴趣协调范围的只读快照；一次观察不保证后续获取必然成功。
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// 注册组件数。
    pub components: usize,
    /// 建立中、活动及退出中的兴趣数。
    pub interests: usize,
    /// 包含固定兴趣、会话及等待者的引用数。
    pub references: usize,
    /// 当前等待操作数。
    pub pending: usize,
}
type Key = (String, Topic);
#[derive(Clone)]
enum Readiness {
    Opening,
    Ready,
    Failed(Error),
    Closed,
}
struct Entry {
    stop: CancellationToken,
    ready: watch::Sender<Readiness>,
}
struct Record {
    entry: Arc<Entry>,
    references: usize,
}
struct PoolState {
    config: Config,
    components: BTreeMap<String, Subscriber>,
    entries: Mutex<BTreeMap<Key, Record>>,
    pending: Semaphore,
    transport: Arc<dyn Transport>,
    tasks: task::Handle,
}
impl Drop for PoolState {
    fn drop(&mut self) {
        if let Ok(entries) = self.entries.get_mut() {
            for record in entries.values() {
                record.entry.stop.cancel();
            }
        }
    }
}
/// 共享订阅协调句柄；任务提交到调用方持续驱动的通用监督范围。
/// 固定兴趣持有 Lease，会话兴趣分别持有 Lease，不把成员引用等同于固定引用。
#[derive(Clone)]
pub struct Pool(Arc<PoolState>);
/// 不延长兴趣范围生命周期的绑定，用于避免订阅处理者与会话管理器互相强持有。
#[derive(Clone)]
pub struct WeakPool(Weak<PoolState>);
impl WeakPool {
    /// 升级仍存活的协调范围；范围释放后不自动重建。
    pub fn upgrade(&self) -> Option<Pool> {
        self.0.upgrade().map(Pool)
    }
}
impl Pool {
    /// 注册组件模板，获取引用时仅替换模板的 topic，保留类型、group、身份和能力。
    /// 初始化期获取固定兴趣应使用正在驱动的 Init.service.tasks()，避免等待尚未 serve 的网关任务。
    pub fn new(
        config: Config,
        transport: Arc<dyn Transport>,
        tasks: task::Handle,
        components: impl IntoIterator<Item = (String, Subscriber)>,
    ) -> Result<Self, Error> {
        if [
            config.max_components,
            config.max_interests,
            config.max_pending,
        ]
        .iter()
        .any(|n| !(1..=65536).contains(n))
            || config.timeout.is_zero()
            || config.timeout > Duration::from_secs(60)
        {
            return Err(Error::InvalidInput("兴趣容量或期限无效"));
        }
        let mut registered = BTreeMap::new();
        for (name, subscriber) in components {
            crate::validate_name(&name)?;
            if registered.len() >= config.max_components {
                return Err(Error::CapacityExceeded);
            }
            if registered.insert(name, subscriber).is_some() {
                return Err(Error::InvalidInput("兴趣组件重复"));
            }
        }
        if registered.is_empty() {
            return Err(Error::InvalidInput("兴趣组件不能为空"));
        }
        Ok(Self(Arc::new(PoolState {
            pending: Semaphore::new(config.max_pending),
            config,
            components: registered,
            entries: Mutex::default(),
            transport,
            tasks,
        })))
    }
    /// 为组件绑定创建弱引用，不延长服务生命期。
    pub fn downgrade(&self) -> WeakPool {
        WeakPool(Arc::downgrade(&self.0))
    }
    /// 查询容量占用，不暴露内部引用表。
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let entries = self
            .0
            .entries
            .lock()
            .map_err(|_| Error::Unavailable("兴趣锁不可用"))?;
        Ok(Snapshot {
            components: self.0.components.len(),
            interests: entries.len(),
            references: entries.values().map(|record| record.references).sum(),
            pending: self.0.config.max_pending - self.0.pending.available_permits(),
        })
    }
    /// 校验绑定的组件名，组装阶段调用，不建立订阅。
    pub fn contains(&self, component: &str) -> bool {
        self.0.components.contains_key(component)
    }
    /// 取得独立引用；并发获取共享建立过程，取消一个等待者不撤销其他需求。
    /// 最后一个引用释放后的新获取等待旧订阅清理，避免新旧 SDK 订阅重叠。
    pub async fn acquire(&self, component: &str, topic: Topic) -> Result<Lease, Error> {
        let template = self
            .0
            .components
            .get(component)
            .ok_or(Error::InvalidInput("兴趣组件未注册"))?;
        let _pending = self
            .0
            .pending
            .try_acquire()
            .map_err(|_| Error::CapacityExceeded)?;
        let deadline = tokio::time::Instant::now() + self.0.config.timeout;
        let key = (component.to_owned(), topic.clone());
        loop {
            let (entry, reservation, counted) = {
                let mut entries = self
                    .0
                    .entries
                    .lock()
                    .map_err(|_| Error::Unavailable("兴趣锁不可用"))?;
                if let Some(record) = entries.get_mut(&key) {
                    if record.entry.stop.is_cancelled() {
                        (Arc::clone(&record.entry), None, false)
                    } else {
                        record.references = record
                            .references
                            .checked_add(1)
                            .ok_or(Error::CapacityExceeded)?;
                        (Arc::clone(&record.entry), None, true)
                    }
                } else {
                    if entries.len() >= self.0.config.max_interests {
                        return Err(Error::CapacityExceeded);
                    }
                    let reservation = self
                        .0
                        .tasks
                        .reserve()
                        .map_err(|_| Error::CapacityExceeded)?;
                    let (ready, _) = watch::channel(Readiness::Opening);
                    let entry = Arc::new(Entry {
                        stop: self.0.tasks.cancellation().child_token(),
                        ready,
                    });
                    entries.insert(
                        key.clone(),
                        Record {
                            entry: Arc::clone(&entry),
                            references: 1,
                        },
                    );
                    (entry, Some(reservation), true)
                }
            };
            if !counted {
                wait_closed(&entry, deadline).await?;
                continue;
            }
            let lease = Lease {
                pool: Arc::downgrade(&self.0),
                timeout: self.0.config.timeout,
                key: key.clone(),
                entry: Arc::clone(&entry),
            };
            if let Some(reservation) = reservation {
                let cleanup = Cleanup {
                    pool: Arc::downgrade(&self.0),
                    key: key.clone(),
                    entry: Arc::clone(&entry),
                };
                let transport = Arc::clone(&self.0.transport);
                let subscriber = template
                    .clone()
                    .with_topics(TopicFilter::Exact(topic.clone()));
                let timeout = self.0.config.timeout;
                reservation.launch("event-interest", Box::pin(async move {
                    let _cleanup = cleanup;
                    let consumer = tokio::select! {
                        biased;
                        () = entry.stop.cancelled() => return Ok(()),
                        result = tokio::time::timeout(timeout, transport.subscribe(subscriber, entry.stop.clone())) => match result {
                            Ok(Ok(consumer)) => consumer,
                            result => {
                                let error = match result { Ok(Err(error)) => error, _ => Error::Unavailable("兴趣建立超时") };
                                entry.ready.send_replace(Readiness::Failed(error));
                                return Ok(());
                            }
                        }
                    };
                    entry.ready.send_replace(Readiness::Ready);
                    let mut consumer = consumer;
                    let result = tokio::select! {
                        result = &mut consumer => result,
                        () = entry.stop.cancelled() => tokio::time::timeout(timeout, &mut consumer).await
                            .unwrap_or(Err(Error::Unavailable("兴趣清理超时"))),
                    };
                    if entry.stop.is_cancelled() { return result.map_err(|error| Box::new(error) as task::BoxError); }
                    result.and(Err(Error::Unavailable("兴趣消费意外结束"))).map_err(|error| Box::new(error) as task::BoxError)
                })).await.map_err(|_| Error::Unavailable("兴趣任务范围已停止"))?;
            }
            let mut ready = lease.entry.ready.subscribe();
            loop {
                let state = ready.borrow_and_update().clone();
                match state {
                    Readiness::Ready if lease.entry.stop.is_cancelled() => {
                        return Err(Error::Unavailable("兴趣已停止"));
                    }
                    Readiness::Ready => return Ok(lease),
                    Readiness::Failed(error) => return Err(error),
                    Readiness::Closed => return Err(Error::Unavailable("兴趣已关闭")),
                    Readiness::Opening => {}
                }
                tokio::time::timeout_at(deadline, ready.changed())
                    .await
                    .map_err(|_| Error::Unavailable("兴趣就绪超时"))?
                    .map_err(|_| Error::Unavailable("兴趣已关闭"))?;
            }
        }
    }
}
/// 一份固定或会话兴趣引用；Drop 撤销自身引用，最后一个引用取消消费任务。
/// 不可 Clone，重复需求应显式 acquire；引用不自动重试或复活失败连接。
/// 不延长 Pool 生命周期，组装层必须持有 Pool；避免处理者与会话目录形成引用环。
pub struct Lease {
    pool: Weak<PoolState>,
    timeout: Duration,
    key: Key,
    entry: Arc<Entry>,
}
impl Lease {
    /// 观察当前引用的订阅是否仍就绪；后续网络操作仍可能失败。
    pub fn is_ready(&self) -> bool {
        !self.entry.stop.is_cancelled() && matches!(*self.entry.ready.borrow(), Readiness::Ready)
    }

    /// 释放引用；最后一个引用等待有限退订清理，其他引用仍存在时立即返回。
    pub async fn release(self) -> Result<(), Error> {
        let entry = Arc::clone(&self.entry);
        let timeout = self.timeout;
        drop(self);
        if entry.stop.is_cancelled() {
            wait_closed(&entry, tokio::time::Instant::now() + timeout).await?;
        }
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade()
            && let Ok(mut entries) = pool.entries.lock()
            && let Some(record) = entries.get_mut(&self.key)
            && Arc::ptr_eq(&record.entry, &self.entry)
        {
            record.references -= 1;
            if record.references == 0 {
                record.entry.stop.cancel();
            }
        }
    }
}
struct Cleanup {
    pool: Weak<PoolState>,
    key: Key,
    entry: Arc<Entry>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        // 先删除旧代次，再发布 Closed；等待者醒来后不会反复观察同一退出条目。
        if let Some(pool) = self.pool.upgrade()
            && let Ok(mut entries) = pool.entries.lock()
            && entries
                .get(&self.key)
                .is_some_and(|record| Arc::ptr_eq(&record.entry, &self.entry))
        {
            entries.remove(&self.key);
        }
        self.entry.stop.cancel();
        if !matches!(*self.entry.ready.borrow(), Readiness::Failed(_)) {
            self.entry.ready.send_replace(Readiness::Closed);
        }
    }
}
async fn wait_closed(entry: &Entry, deadline: tokio::time::Instant) -> Result<(), Error> {
    let mut ready = entry.ready.subscribe();
    loop {
        if matches!(
            *ready.borrow_and_update(),
            Readiness::Closed | Readiness::Failed(_)
        ) {
            return Ok(());
        }
        tokio::time::timeout_at(deadline, ready.changed())
            .await
            .map_err(|_| Error::Unavailable("兴趣退出超时"))?
            .map_err(|_| Error::Unavailable("兴趣已关闭"))?;
    }
}
