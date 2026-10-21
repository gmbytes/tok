use super::{MAX_DURATION, RETRY, unavailable, validate};
use redis::{Client, Script, aio::MultiplexedConnection};
use std::{sync::Arc, time::Duration};
use tok_core::BoxFuture;
use tok_core::lock::{Backend as LockBackend, Key as LockKey, Lease as LockLease};
use tok_core::lock::{Error, Result};
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout},
};

const RELEASE: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";

/// Redis 请求锁配置，与 session 所有权和服务发现独立。
#[derive(Clone)]
pub struct RedisConfig {
    /// Redis URL；Debug 隐藏连接凭据，构造时不连接。
    pub url: String,
    /// 环境隔离名，仅允许 ASCII 字母、数字、短横线和下划线。
    pub namespace: String,
    /// 每次连接、获取尝试或释放操作的最大时长。
    pub operation_timeout: Duration,
    /// 本后端同时进行的获取操作上限，超限直接拒绝。
    pub max_pending: usize,
}
impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".into(),
            namespace: "dev".into(),
            operation_timeout: Duration::from_secs(2),
            max_pending: 1024,
        }
    }
}
impl std::fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_pending", &self.max_pending)
            .finish()
    }
}
/// 单 Redis 节点请求锁；唯一持有者标识与 Lua 条件释放防止删除新锁。
///
/// 不提供自动续租或 Cluster 客户端。获取结果不确定时不重试写操作，取消后依靠 TTL。
/// Redis 故障切换可能回退状态，不承诺严格分布式互斥。
#[derive(Clone)]
pub struct Redis {
    client: Client,
    config: RedisConfig,
    pending: Arc<Semaphore>,
}
impl Redis {
    /// 检查配置并构造后端，不访问网络。
    pub fn new(config: RedisConfig) -> Result<Self> {
        if config.namespace.is_empty()
            || config.namespace.len() > 64
            || !config
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || config.operation_timeout.is_zero()
            || config.operation_timeout > MAX_DURATION
            || config.max_pending == 0
            || config.max_pending > Semaphore::MAX_PERMITS
        {
            return Err(Error::InvalidConfig("Redis 请求锁配置无效".into()));
        }
        let client = Client::open(config.url.as_str())
            .map_err(|_| Error::InvalidConfig("Redis 请求锁 URL 无效".into()))?;
        let pending = Arc::new(Semaphore::new(config.max_pending));
        Ok(Self {
            client,
            config,
            pending,
        })
    }
    fn key(&self, key: &LockKey) -> String {
        // 十六进制分段编码避免分隔符碰撞及用户输入注入 Cluster hash tag。
        let hex = |value: &str| {
            value
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        format!(
            "tok:lock:{{{}:{}:{}}}",
            self.config.namespace,
            hex(&key.namespace),
            hex(&key.bid)
        )
    }
}
impl LockBackend for Redis {
    fn acquire(
        &self,
        key: LockKey,
        wait: Duration,
        ttl: Duration,
    ) -> BoxFuture<'_, tok_core::lock::Result<Box<dyn LockLease>>> {
        Box::pin(async move {
            validate(&key, wait, ttl)?;
            let _permit = self
                .pending
                .try_acquire()
                .map_err(|_| Error::CapacityExceeded)?;
            let started = Instant::now();
            let deadline = started + wait;
            let operation = self.config.operation_timeout;
            let mut connection = timeout(operation, self.client.get_multiplexed_async_connection())
                .await
                .map_err(|_| unavailable())?
                .map_err(|_| unavailable())?;
            let key = self.key(&key);
            let token = uuid::Uuid::new_v4().to_string();
            let milliseconds = u64::try_from(ttl.as_nanos().div_ceil(1_000_000))
                .map_err(|_| Error::InvalidInput("锁租期溢出".into()))?;
            let mut first = true;
            loop {
                // wait=0 仍允许一次有限获取；后续竞争尝试不能超出总等待期限。
                if !first && Instant::now() >= deadline {
                    return Err(Error::WaitTimeout);
                }
                let budget = if first && wait.is_zero() {
                    operation
                } else {
                    operation.min(deadline.saturating_duration_since(Instant::now()))
                };
                first = false;
                if budget.is_zero() {
                    return Err(Error::WaitTimeout);
                }
                let attempt_started = Instant::now();
                let acquired: Option<String> = timeout(
                    budget,
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(&token)
                        .arg("NX")
                        .arg("PX")
                        .arg(milliseconds)
                        .query_async(&mut connection),
                )
                .await
                .map_err(|_| unavailable())?
                .map_err(|_| unavailable())?;
                if acquired.is_some() {
                    if attempt_started.elapsed() >= ttl {
                        return Err(unavailable());
                    }
                    return Ok(Box::new(Lease {
                        connection,
                        key,
                        token,
                        operation,
                    }) as Box<dyn LockLease>);
                }
                if Instant::now() >= deadline {
                    return Err(Error::WaitTimeout);
                }
                tokio::time::sleep_until(deadline.min(Instant::now() + RETRY)).await;
            }
        })
    }
}
struct Lease {
    connection: MultiplexedConnection,
    key: String,
    token: String,
    operation: Duration,
}
impl LockLease for Lease {
    fn token(&self) -> &str {
        &self.token
    }
    fn release(mut self: Box<Self>) -> BoxFuture<'static, tok_core::lock::Result<()>> {
        Box::pin(async move {
            let _: i64 = timeout(
                self.operation,
                Script::new(RELEASE)
                    .key(&self.key)
                    .arg(&self.token)
                    .invoke_async(&mut self.connection),
            )
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
            Ok(())
        })
    }
}
