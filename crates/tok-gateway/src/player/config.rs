//! Ingress 的协议、session 与租约参数，兼容原有 player_gateway 配置布局。

use serde::Deserialize;
use tok_core::{Error, Result};

/// Player 接入组件的 TCP、串行 session、租约、恢复与跨网关交接参数。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// 显式开启跨网关交接，需组装本地共用密钥及内部端点。
    pub handoff_enabled: bool,
    /// 目录中的网关内部 RPC 服务名，同节点按 owner.service_name 分发。
    pub gateway_rpc_service_name: String,
    /// 整次交接的总期限，单位毫秒。
    pub handoff_timeout_ms: u64,
    /// 每节点保留的交接记录数量上限。
    pub max_handoffs: usize,
    /// 单次交接快照的 protobuf 编码字节上限。
    pub max_handoff_bytes: usize,
    /// 同时持有的 TCP 连接数。
    pub max_connections: usize,
    /// 玩家执行目录的最大条目数，关闭且无人使用的条目可回收。
    pub max_sessions: usize,
    /// 每个玩家等待执行的请求数，不包含正在执行的请求。
    pub queue_capacity: usize,
    /// 每条连接等待发送的控制、回应与通知数。
    pub outbound_capacity: usize,
    /// TCP 正文最大字节数，不包含 4 字节长度前缀。
    pub max_frame_size: usize,
    /// 完整帧读写期限，单位毫秒。
    pub io_timeout_ms: u64,
    /// 认证扩展点期限，单位毫秒。
    pub auth_timeout_ms: u64,
    /// player 执行与所有权后端操作期限，单位毫秒。
    pub rpc_timeout_ms: u64,
    /// 所有权租期，单位毫秒。
    pub ownership_ttl_ms: u64,
    /// 空闲 session 的续租间隔，单位毫秒。
    pub renew_interval_ms: u64,
    /// 每个逻辑 session 加入的事件主题上限，最多 1024。
    pub max_topics: usize,
    /// 每个 session 的 timer 数量上限。
    pub max_timers: usize,
    /// 每个 session 所有 timer 的 ID 与正文总字节上限。
    pub max_timer_bytes: usize,
    /// 下行缓存从原始发布时间起的保留时长，单位毫秒。
    pub replay_ttl_ms: u64,
    /// 每个 session 的下行缓存条数上限，允许淘汰未确认记录。
    pub replay_capacity: usize,
    /// 每个 session 的编码下行正文总字节上限，不小于 max_frame_size。
    pub replay_max_bytes: usize,
    /// 断线后保留 session、租约、timer 与缓存的时长；零表示立即关闭，单位毫秒。
    pub disconnect_grace_ms: u64,
    /// 恢复凭证的身份有效期，独立于会话宽限期，单位毫秒。
    pub resume_token_ttl_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            handoff_enabled: false,
            gateway_rpc_service_name: "gateway-rpc".into(),
            handoff_timeout_ms: 15000,
            max_handoffs: 16,
            max_handoff_bytes: 8 * 1024 * 1024,
            max_connections: 128,
            max_sessions: 1024,
            queue_capacity: 64,
            outbound_capacity: 64,
            max_frame_size: 1_048_576,
            io_timeout_ms: 15_000,
            auth_timeout_ms: 5000,
            rpc_timeout_ms: 5000,
            ownership_ttl_ms: 30_000,
            renew_interval_ms: 10_000,
            max_timers: 128,
            max_topics: 64,
            max_timer_bytes: 1_048_576,
            replay_ttl_ms: 30_000,
            replay_capacity: 256,
            replay_max_bytes: 4_194_304,
            disconnect_grace_ms: 30_000,
            resume_token_ttl_ms: 3_600_000,
        }
    }
}

impl Config {
    /// 从玩家参数提取通用连接和后台任务容量；session 数量限制由玩家目录同时执行。
    pub fn runtime_limits(&self) -> crate::GatewayConfig {
        crate::GatewayConfig {
            max_connections: self.max_connections,
            max_tasks: self
                .max_sessions
                .saturating_add(if self.handoff_enabled {
                    self.max_handoffs
                } else {
                    0
                })
                .min(65536),
        }
    }
    /// 验证正数、容量上限及租期对调用和续租间隔的覆盖。
    pub fn validate(&self) -> Result<()> {
        if self.gateway_rpc_service_name.trim().is_empty()
            || !(1..=60000).contains(&self.handoff_timeout_ms)
            || !(1..=4096).contains(&self.max_handoffs)
            || !(65536..=134217728).contains(&self.max_handoff_bytes)
        {
            return Err(Error::InvalidConfig("跨网关交接参数无效".into()));
        }
        if [
            self.max_connections,
            self.max_sessions,
            self.queue_capacity,
            self.outbound_capacity,
            self.max_timers,
            self.max_topics,
            self.replay_capacity,
        ]
        .iter()
        .any(|value| *value == 0 || *value > 65_536)
            || self.max_topics > 1024
            || self.max_timer_bytes == 0
            || self.max_timer_bytes > 16 * 1024 * 1024
            || self.max_frame_size == 0
            || self.max_frame_size > 16 * 1024 * 1024
            || self.replay_max_bytes < self.max_frame_size
            || self.replay_max_bytes > 64 * 1024 * 1024
            || [
                self.io_timeout_ms,
                self.auth_timeout_ms,
                self.rpc_timeout_ms,
                self.ownership_ttl_ms,
                self.renew_interval_ms,
                self.replay_ttl_ms,
                self.resume_token_ttl_ms,
            ]
            .iter()
            .any(|value| *value == 0 || *value > 86_400_000)
            || self.disconnect_grace_ms > 86_400_000
            || self.ownership_ttl_ms <= self.rpc_timeout_ms * 2 + self.renew_interval_ms
        {
            return Err(Error::InvalidConfig(
                "玩家网关需要有限的正数容量和期限，租期必须覆盖续租间隔与两次调用期限".into(),
            ));
        }
        Ok(())
    }
}
