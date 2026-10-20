//! 有限租期的请求级锁；取消后依靠 TTL 兜底，不保证异常路径绝对互斥。

mod memory;
mod redis;

pub use memory::Memory;
pub use redis::{Redis, RedisConfig};

use std::time::Duration;
use tok_core::{Error, Result, coordination::LockKey};

const RETRY: Duration = Duration::from_millis(10);
const MAX_DURATION: Duration = Duration::from_secs(86_400);

fn validate(key: &LockKey, wait: Duration, ttl: Duration) -> Result<()> {
    if key.namespace.trim().is_empty()
        || key.namespace.len() > 256
        || key.bid.trim().is_empty()
        || key.bid.len() > 1024
        || ttl < Duration::from_millis(1)
        || ttl > MAX_DURATION
        || wait > MAX_DURATION
    {
        return Err(Error::InvalidInput("锁键或等待、租期无效".into()));
    }
    Ok(())
}
fn unavailable() -> Error {
    Error::CoordinationUnavailable("请求级锁操作失败或超时".into())
}
