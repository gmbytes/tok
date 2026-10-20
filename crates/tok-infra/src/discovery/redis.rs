//! 开发环境 Redis 注册租约与周期性本地快照同步，不在业务查询路径访问 Redis。

use redis::{Client, Script, aio::MultiplexedConnection};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tok_core::{
    DirectoryWatch, Error, Result, ServiceDescriptor, ServiceDirectory,
    discovery::{RegistrationTask, ServiceRegistry},
};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

// 一次节点操作只修改一个 key；实例 token 防止旧进程续租或删除重启后的记录。
const WRITE: &str = r#"
local owner = redis.call('HGET', KEYS[1], 'owner')
if owner and owner ~= ARGV[1] then return 0 end
if not owner and ARGV[4] == 'renew' then return 0 end
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'data', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
"#;
const REMOVE: &str = r#"
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"#;

/// Redis 开发发现配置；连接 URL 的 Debug 输出隐藏凭据。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[expect(
    clippy::module_name_repetitions,
    reason = "公开类型保留后端或所有权角色名称，便于调用方区分 Memory 与 Redis 实现。"
)]
pub struct RedisDiscoveryConfig {
    /// Redis 连接 URL，开发默认连接本机 6379；不写入日志。
    pub url: String,
    /// 开发环境隔离名称，只允许 ASCII 字母、数字、短横线与下划线。
    pub namespace: String,
    /// 节点注册租期，也作为本地快照的最大有效时间。
    pub lease_ttl_ms: u64,
    /// 后台续租和完整目录刷新间隔。
    pub refresh_interval_ms: u64,
    /// 每次完整 Redis 操作的总期限，包含连接建立。
    pub operation_timeout_ms: u64,
    /// 单个 namespace 能容纳的最大节点数。
    pub max_nodes: usize,
}

impl std::fmt::Debug for RedisDiscoveryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisDiscoveryConfig")
            .field("url", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("refresh_interval_ms", &self.refresh_interval_ms)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .field("max_nodes", &self.max_nodes)
            .finish()
    }
}
impl Default for RedisDiscoveryConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".into(),
            namespace: "dev".into(),
            lease_ttl_ms: 15_000,
            refresh_interval_ms: 1_000,
            operation_timeout_ms: 2_000,
            max_nodes: 256,
        }
    }
}
impl RedisDiscoveryConfig {
    /// 仅验证配置，不连接 Redis；租期须覆盖刷新间隔和两次操作期限。
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty()
            || self.namespace.len() > 64
            || !self
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || self.max_nodes == 0
            || self.max_nodes > 4096
            || self.refresh_interval_ms == 0
            || self.operation_timeout_ms == 0
            || self.lease_ttl_ms > 86_400_000
            || self.refresh_interval_ms > 86_400_000
            || self.operation_timeout_ms > 86_400_000
            || self.lease_ttl_ms
                <= self
                    .refresh_interval_ms
                    .saturating_add(self.operation_timeout_ms.saturating_mul(2))
            || Client::open(self.url.as_str()).is_err()
        {
            return Err(Error::InvalidConfig(
                "Redis 发现配置、容量或租期无效".into(),
            ));
        }
        Ok(())
    }
}

type Services = BTreeMap<(String, String), ServiceDescriptor>;
struct Cache {
    services: Services,
    refreshed: Option<Instant>,
}
#[derive(Default)]
struct Owned {
    services: BTreeMap<String, ServiceDescriptor>,
    acquired: bool,
}

/// 每个 Application 独占的 Redis 注册实例，快照查询只读取本地缓存。
///
/// 构造不产生任务；先 initialize，再由组装层监督 run。运行错误应停止应用，
/// run future 被丢弃后不再续租；未能正常撤销的记录由 TTL 清理。
#[expect(
    clippy::module_name_repetitions,
    reason = "公开类型保留后端或所有权角色名称，便于调用方区分 Memory 与 Redis 实现。"
)]
pub struct RedisDirectory {
    config: RedisDiscoveryConfig,
    client: Client,
    node_id: String,
    key: String,
    token: String,
    owned: Mutex<Owned>,
    cache: RwLock<Cache>,
    updates: watch::Sender<u64>,
}
impl RedisDirectory {
    /// 创建独立实例 token；相同 node_id 的不同进程不能同时持有注册租约。
    pub fn new(config: RedisDiscoveryConfig, node_id: impl Into<String>) -> Result<Arc<Self>> {
        config.validate()?;
        let node_id = node_id.into();
        if node_id.trim().is_empty() || node_id.len() > 256 {
            return Err(Error::InvalidConfig("Redis 发现 node_id 长度无效".into()));
        }
        let client = Client::open(config.url.as_str()).map_err(|_| Error::DiscoveryUnavailable)?;
        let encoded: String = node_id.bytes().map(|b| format!("{b:02x}")).collect();
        Ok(Arc::new(Self {
            key: format!("tok:discovery:{{{}}}:node:{encoded}", config.namespace),
            token: uuid::Uuid::new_v4().to_string(),
            config,
            client,
            node_id,
            owned: Mutex::new(Owned::default()),
            cache: RwLock::new(Cache {
                services: BTreeMap::new(),
                refreshed: None,
            }),
            updates: watch::channel(0).0,
        }))
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.operation_timeout_ms)
    }

    /// 启动前连接并读取初始快照；失败时不发布业务服务。
    pub async fn initialize(&self) -> Result<()> {
        self.refresh().await
    }

    async fn write(
        &self,
        services: &BTreeMap<String, ServiceDescriptor>,
        acquired: bool,
    ) -> Result<()> {
        let data = serde_json::to_string(&services.values().collect::<Vec<_>>())
            .map_err(|_| Error::DiscoveryUnavailable)?;
        if data.len() > 65_536 {
            return Err(Error::InvalidInput("节点发现信息超过 64 KiB".into()));
        }
        tokio::time::timeout(self.timeout(), async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            let success: i64 = Script::new(WRITE)
                .key(&self.key)
                .arg(&self.token)
                .arg(data)
                .arg(self.config.lease_ttl_ms)
                .arg(if acquired { "renew" } else { "claim" })
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            if success != 1 {
                return Err(Error::DuplicateService(self.node_id.clone()));
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::DiscoveryUnavailable)?
    }

    async fn remove(&self) -> Result<bool> {
        tokio::time::timeout(self.timeout(), async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            let removed: i64 = Script::new(REMOVE)
                .key(&self.key)
                .arg(&self.token)
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            Ok(removed == 1)
        })
        .await
        .map_err(|_| Error::DiscoveryUnavailable)?
    }

    /// 条件撤销本实例的全部服务；旧实例不能删除新实例记录。
    pub async fn withdraw(&self) -> Result<()> {
        let mut owned = self.owned.lock().await;
        let result = self.remove().await;
        owned.services.clear();
        owned.acquired = false;
        self.invalidate();
        result.map(|_| ())
    }

    async fn read_snapshot(&self, connection: &mut MultiplexedConnection) -> Result<Services> {
        let mut cursor = 0u64;
        let mut keys = BTreeSet::new();
        let mut services = BTreeMap::new();
        let pattern = format!("tok:discovery:{{{}}}:node:*", self.config.namespace);
        let mut pages = 0;
        loop {
            let (next, page): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(128)
                .query_async(connection)
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            pages += 1;
            if pages > 4096 {
                return Err(Error::DiscoveryUnavailable);
            }
            for key in page {
                if !keys.insert(key.clone()) {
                    continue;
                }
                if keys.len() > self.config.max_nodes {
                    return Err(Error::DiscoveryUnavailable);
                }
                let (data, ttl): (Option<String>, i64) = redis::pipe()
                    .cmd("HGET")
                    .arg(&key)
                    .arg("data")
                    .cmd("PTTL")
                    .arg(&key)
                    .query_async(connection)
                    .await
                    .map_err(|_| Error::DiscoveryUnavailable)?;
                let Some(data) = data.filter(|data| data.len() <= 65_536 && ttl > 0) else {
                    continue;
                };
                let entries: Vec<ServiceDescriptor> =
                    serde_json::from_str(&data).map_err(|_| Error::DiscoveryUnavailable)?;
                if entries.len() > 64 {
                    return Err(Error::DiscoveryUnavailable);
                }
                for service in entries {
                    if service.node_id.is_empty()
                        || service.service_name.is_empty()
                        || service.protocol.is_empty()
                        || service.address.port() == 0
                        || service.address.ip().is_unspecified()
                        || services
                            .insert(
                                (service.node_id.clone(), service.service_name.clone()),
                                service,
                            )
                            .is_some()
                    {
                        return Err(Error::DiscoveryUnavailable);
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                return Ok(services);
            }
        }
    }

    async fn refresh(&self) -> Result<()> {
        let result = tokio::time::timeout(self.timeout(), async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| Error::DiscoveryUnavailable)?;
            self.read_snapshot(&mut connection).await
        })
        .await
        .map_err(|_| Error::DiscoveryUnavailable)?;
        let services = result?;
        let mut cache = self
            .cache
            .write()
            .map_err(|_| Error::DirectoryUnavailable)?;
        let changed = cache.refreshed.is_none() || cache.services != services;
        cache.services = services;
        cache.refreshed = Some(Instant::now());
        if changed {
            self.updates
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    fn invalidate(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.services.clear();
            cache.refreshed = None;
        }
        self.updates
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// 持续续租与刷新；后端错误使任务失败并失效本地缓存，由 Application 统一退出。
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let result = async {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.config.refresh_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = interval.tick() => {}
                }
                {
                    let owned = self.owned.lock().await;
                    if owned.acquired {
                        self.write(&owned.services, true).await?;
                    }
                }
                self.refresh().await?;
            }
        }
        .await;
        self.invalidate();
        result
    }
}

impl ServiceRegistry for RedisDirectory {
    fn register(&self, service: ServiceDescriptor) -> RegistrationTask<'_, ()> {
        Box::pin(async move {
            if service.node_id != self.node_id
                || service.address.ip().is_unspecified()
                || service.address.port() == 0
                || service.service_name.trim().is_empty()
                || service.protocol.trim().is_empty()
            {
                return Err(Error::InvalidInput("注册身份或发布地址无效".into()));
            }
            let mut owned = self.owned.lock().await;
            if owned.services.contains_key(&service.service_name) || owned.services.len() >= 64 {
                return Err(Error::DuplicateService(service.service_name));
            }
            let mut next = owned.services.clone();
            next.insert(service.service_name.clone(), service);
            self.write(&next, owned.acquired).await?;
            owned.services = next;
            owned.acquired = true;
            Ok(())
        })
    }
    fn unregister<'a>(
        &'a self,
        node_id: &'a str,
        service_name: &'a str,
    ) -> RegistrationTask<'a, bool> {
        Box::pin(async move {
            let mut owned = self.owned.lock().await;
            if node_id != self.node_id || !owned.services.contains_key(service_name) {
                return Ok(false);
            }
            let mut next = owned.services.clone();
            next.remove(service_name);
            if next.is_empty() {
                self.remove().await?;
            } else {
                self.write(&next, true).await?;
            }
            owned.acquired = !next.is_empty();
            owned.services = next;
            Ok(true)
        })
    }
}
struct Changes(watch::Receiver<u64>);
impl DirectoryWatch for Changes {
    fn changed(&mut self) -> RegistrationTask<'_, ()> {
        Box::pin(async move {
            self.0
                .changed()
                .await
                .map_err(|_| Error::DiscoveryUnavailable)
        })
    }
}
impl ServiceDirectory for RedisDirectory {
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>> {
        let cache = self.cache.read().map_err(|_| Error::DirectoryUnavailable)?;
        if !cache.refreshed.is_some_and(|updated| {
            updated.elapsed() < Duration::from_millis(self.config.lease_ttl_ms)
        }) {
            return Err(Error::DiscoveryUnavailable);
        }
        Ok(cache
            .services
            .values()
            .filter(|entry| entry.service_name == service_name)
            .cloned()
            .collect())
    }
    fn subscribe(&self) -> Result<Box<dyn DirectoryWatch>> {
        Ok(Box::new(Changes(self.updates.subscribe())))
    }
}
