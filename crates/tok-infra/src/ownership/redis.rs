//! 原子 Redis 会话租约；epoch 是所有权代次，不强制业务存储使用 fencing。

use redis::{Client, Script, aio::MultiplexedConnection};
use serde::Deserialize;
use std::{future::Future, time::Duration};
use tok_core::{
    Error, Result, SessionIdentity,
    coordination::{CoordinationTask, OwnershipStore, SessionOwner, SessionOwnership},
};

// epoch 必须按十进制字符串比较和返回，不能经过 Lua number（超过 2^53 会失真）。
const ACQUIRE: &str = r#"
local current = redis.call('HGET', KEYS[1], 'epoch')
if (current or '') ~= ARGV[1] then return {0, ''} end
redis.call('INCR', KEYS[2])
local epoch = redis.call('GET', KEYS[2])
redis.call('HSET', KEYS[1], 'epoch', epoch, 'session', ARGV[2], 'node', ARGV[3], 'service', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {1, epoch}
"#;
const RENEW: &str = r#"
local values = redis.call('HMGET', KEYS[1], 'epoch', 'session', 'node', 'service')
for i = 1, 4 do if values[i] ~= ARGV[i] then return 0 end end
return redis.call('PEXPIRE', KEYS[1], ARGV[5])
"#;
const RELEASE: &str = r#"
local values = redis.call('HMGET', KEYS[1], 'epoch', 'session', 'node', 'service')
for i = 1, 4 do if values[i] ~= ARGV[i] then return 0 end end
return redis.call('DEL', KEYS[1])
"#;

/// Redis 所有权配置，与 discovery 独立；Debug 隐藏连接凭据。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisOwnershipConfig {
    /// Redis 连接 URL；构造后端不建立网络连接。
    pub url: String,
    /// 所有权环境隔离名，只允许 ASCII 字母、数字、短横线和下划线。
    pub namespace: String,
    /// 每次完整操作的期限，包含连接建立；超时不自动重试。
    pub operation_timeout_ms: u64,
}
impl Default for RedisOwnershipConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".into(),
            namespace: "dev".into(),
            operation_timeout_ms: 2000,
        }
    }
}
impl std::fmt::Debug for RedisOwnershipConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisOwnershipConfig")
            .field("url", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish()
    }
}
impl RedisOwnershipConfig {
    /// 验证环境名、连接格式和有限期限，不连接 Redis。
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty()
            || self.namespace.len() > 64
            || !self
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || self.operation_timeout_ms == 0
            || self.operation_timeout_ms > 60_000
            || Client::open(self.url.as_str()).is_err()
        {
            return Err(Error::InvalidConfig(
                "Redis 所有权配置或操作期限无效".into(),
            ));
        }
        Ok(())
    }
}

/// 跨进程会话所有权；每玩家的租约和持久 epoch 计数器使用相同 Redis Cluster hash tag。
///
/// 租约键随 TTL 过期，epoch 键不自动过期，防止租约释放或进程重启导致代次复用。
/// Redis 持久化与故障切换可能回退状态，不承诺严格互斥；调用方必须停止失效入口，
/// 已发出的 RPC 可能继续执行。操作超时的远端结果未知，不自动重试。
pub struct RedisOwnership {
    config: RedisOwnershipConfig,
    client: Client,
}
impl RedisOwnership {
    /// 创建可共享后端；配置检查不依赖 Redis 是否在线。
    pub fn new(config: RedisOwnershipConfig) -> Result<Self> {
        config.validate()?;
        let client = Client::open(config.url.as_str()).map_err(|_| unavailable())?;
        Ok(Self { config, client })
    }
    fn keys(&self, player_id: &str) -> Result<(String, String)> {
        identity(player_id, 256)?;
        let encoded: String = player_id.bytes().map(|b| format!("{b:02x}")).collect();
        let prefix = format!("tok:ownership:{{{}:{encoded}}}", self.config.namespace);
        Ok((format!("{prefix}:lease"), format!("{prefix}:epoch")))
    }
    async fn operation<T, F: Future<Output = Result<T>> + Send>(
        &self,
        operation: impl FnOnce(MultiplexedConnection) -> F + Send,
    ) -> Result<T> {
        tokio::time::timeout(
            Duration::from_millis(self.config.operation_timeout_ms),
            async {
                let connection = self
                    .client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| unavailable())?;
                operation(connection).await
            },
        )
        .await
        .map_err(|_| unavailable())?
    }
    async fn timely<T>(
        &self,
        ttl: Duration,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let started = tokio::time::Instant::now();
        let result = operation.await?;
        // 使用操作开始时刻作保守下界，不把明显超过租期的延迟回应视为有效租约。
        if started.elapsed() >= ttl {
            return Err(Error::OwnershipConflict);
        }
        Ok(result)
    }
}
fn unavailable() -> Error {
    Error::CoordinationUnavailable("Redis 所有权操作失败或超时".into())
}
fn identity(value: &str, limit: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > limit {
        return Err(Error::InvalidInput("所有权身份长度无效".into()));
    }
    Ok(())
}
fn ttl_millis(ttl: Duration) -> Result<u64> {
    if ttl < Duration::from_millis(1) || ttl > Duration::from_secs(86_400) {
        return Err(Error::InvalidInput("所有权租期必须为 1 毫秒至 1 天".into()));
    }
    Ok(ttl.as_millis() as u64)
}
fn validate(ownership: &SessionOwnership) -> Result<()> {
    identity(&ownership.identity.player_id, 256)?;
    identity(&ownership.identity.session_id, 1024)?;
    identity(&ownership.owner.node_id, 256)?;
    identity(&ownership.owner.service_name, 256)?;
    if ownership.identity.epoch == 0 {
        return Err(Error::OwnershipConflict);
    }
    Ok(())
}

impl OwnershipStore for RedisOwnership {
    fn lookup(&self, player_id: &str) -> CoordinationTask<'_, Option<SessionOwnership>> {
        let player_id = player_id.to_owned();
        Box::pin(async move {
            let (lease, _) = self.keys(&player_id)?;
            self.operation(|mut connection| async move {
                let (values, ttl): (Vec<Option<String>>, i64) = redis::pipe()
                    .cmd("HMGET")
                    .arg(lease.clone())
                    .arg(&["epoch", "session", "node", "service"])
                    .cmd("PTTL")
                    .arg(lease)
                    .query_async(&mut connection)
                    .await
                    .map_err(|_| unavailable())?;
                if values.iter().all(Option::is_none) || ttl == -2 {
                    return Ok(None);
                }
                if ttl <= 0 || values.len() != 4 {
                    return Err(unavailable());
                }
                let mut values = values.into_iter();
                let epoch: u64 = values
                    .next()
                    .flatten()
                    .ok_or_else(unavailable)?
                    .parse()
                    .map_err(|_| unavailable())?;
                let ownership = SessionOwnership {
                    identity: SessionIdentity {
                        player_id,
                        epoch,
                        session_id: values.next().flatten().ok_or_else(unavailable)?,
                    },
                    owner: SessionOwner {
                        node_id: values.next().flatten().ok_or_else(unavailable)?,
                        service_name: values.next().flatten().ok_or_else(unavailable)?,
                    },
                };
                validate(&ownership).map_err(|_| unavailable())?;
                Ok(Some(ownership))
            })
            .await
        })
    }
    fn acquire(
        &self,
        player_id: String,
        session_id: String,
        owner: SessionOwner,
        expected_epoch: Option<u64>,
        ttl: Duration,
    ) -> CoordinationTask<'_, SessionOwnership> {
        Box::pin(async move {
            identity(&session_id, 1024)?;
            identity(&owner.node_id, 256)?;
            identity(&owner.service_name, 256)?;
            let milliseconds = ttl_millis(ttl)?;
            let (lease, epoch_key) = self.keys(&player_id)?;
            self.timely(
                Duration::from_millis(milliseconds),
                self.operation(|mut connection| async move {
                    let (success, epoch): (i64, String) = Script::new(ACQUIRE)
                        .key(lease)
                        .key(epoch_key)
                        .arg(
                            expected_epoch
                                .map(|epoch| epoch.to_string())
                                .unwrap_or_default(),
                        )
                        .arg(&session_id)
                        .arg(&owner.node_id)
                        .arg(&owner.service_name)
                        .arg(milliseconds)
                        .invoke_async(&mut connection)
                        .await
                        .map_err(|_| unavailable())?;
                    if success != 1 {
                        return Err(Error::OwnershipConflict);
                    }
                    Ok(SessionOwnership {
                        identity: SessionIdentity {
                            player_id,
                            session_id,
                            epoch: epoch.parse().map_err(|_| unavailable())?,
                        },
                        owner,
                    })
                }),
            )
            .await
        })
    }
    fn renew(&self, ownership: SessionOwnership, ttl: Duration) -> CoordinationTask<'_, ()> {
        Box::pin(async move {
            validate(&ownership)?;
            let milliseconds = ttl_millis(ttl)?;
            let (lease, _) = self.keys(&ownership.identity.player_id)?;
            self.timely(
                Duration::from_millis(milliseconds),
                self.operation(|mut connection| async move {
                    let success: i64 = Script::new(RENEW)
                        .key(lease)
                        .arg(ownership.identity.epoch.to_string())
                        .arg(ownership.identity.session_id)
                        .arg(ownership.owner.node_id)
                        .arg(ownership.owner.service_name)
                        .arg(milliseconds)
                        .invoke_async(&mut connection)
                        .await
                        .map_err(|_| unavailable())?;
                    if success == 1 {
                        Ok(())
                    } else {
                        Err(Error::OwnershipConflict)
                    }
                }),
            )
            .await
        })
    }
    fn release(&self, ownership: SessionOwnership) -> CoordinationTask<'_, bool> {
        Box::pin(async move {
            validate(&ownership)?;
            let (lease, _) = self.keys(&ownership.identity.player_id)?;
            self.operation(|mut connection| async move {
                let removed: i64 = Script::new(RELEASE)
                    .key(lease)
                    .arg(ownership.identity.epoch.to_string())
                    .arg(ownership.identity.session_id)
                    .arg(ownership.owner.node_id)
                    .arg(ownership.owner.service_name)
                    .invoke_async(&mut connection)
                    .await
                    .map_err(|_| unavailable())?;
                Ok(removed == 1)
            })
            .await
        })
    }
}
