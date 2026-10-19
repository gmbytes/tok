//! TOML 配置解析与校验。端点描述不自动创建监听。

use std::{collections::BTreeSet, net::SocketAddr};

use serde::Deserialize;
use tok_core::{Error, Result, ServiceDescriptor};

/// 配置选定的服务发现后端。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryBackend {
    /// 单进程内存目录。
    Memory,
    /// 多进程开发用 Redis 租约注册与本地缓存。
    Redis,
    /// 配置可校验，运行后端尚未实现。
    Kubernetes,
}

/// session 所有权后端，独立于服务发现选择。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OwnershipBackend {
    /// 默认单进程内存所有权。
    #[default]
    Memory,
    /// 多网关共享 Redis 所有权，跨网关登录当前仍拒绝接管。
    Redis,
}

/// 业务服务身份、可选初始化工厂及端点引用；实例加锁策略由组装层决定。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// 节点内唯一的业务服务名，由游戏定义。
    pub service_name: String,
    /// 引用 endpoints 中的端点名称。
    pub endpoint: String,
    /// 可选的初始化工厂名称；省略时使用 service_name 查找注册工厂。
    #[serde(default)]
    pub factory: Option<String>,
}

impl ServiceConfig {
    /// 返回实际使用的工厂名称；显式指定优先，否则使用服务名。
    pub fn factory_name(&self) -> &str {
        self.factory.as_deref().unwrap_or(&self.service_name)
    }
}

/// 节点内可共享的监听端点；协议名由传输实现解释。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    /// 节点内唯一的端点名称。
    pub name: String,
    /// 由传输实现解释，例如 grpc 或 tcp。
    pub protocol: String,
    /// 监听地址；发现后端应提供远端可连接地址。
    pub address: SocketAddr,
    /// 发现目录中发布的可连接地址，省略时使用 address。
    #[serde(default)]
    pub advertise_address: Option<SocketAddr>,
}

/// 一个游戏进程的配置；未知字段、重复名称与无效引用均被拒绝。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// 部署节点身份，节点内的所有业务服务共享此 ID。
    pub node_id: String,
    /// 服务发现后端选择。
    pub discovery: DiscoveryBackend,
    /// 仅在 discovery=redis 时配置的开发发现后端。
    #[serde(default)]
    pub redis_discovery: Option<tok_infra::discovery::redis::RedisDiscoveryConfig>,
    /// session 所有权后端；省略时使用 Memory，选择不随 discovery 隐式变化。
    #[serde(default)]
    pub ownership: OwnershipBackend,
    /// ownership=redis 时必需的共享所有权配置。
    #[serde(default)]
    pub redis_ownership: Option<tok_infra::ownership::redis::RedisOwnershipConfig>,
    /// 节点声明的端点列表，实际监听需显式组装。
    pub endpoints: Vec<EndpointConfig>,
    /// 本进程启用的业务服务与端点绑定。
    pub services: Vec<ServiceConfig>,
    /// 进程级日志、指标和追踪配置。
    #[serde(default)]
    pub observability: tok_observability::Config,
    /// 玩家 TCP 网关的容量与期限；未组装网关时不创建监听。
    #[serde(default)]
    pub player_gateway: tok_gateway::player::PlayerConfig,
}

impl AppConfig {
    /// 解析并立即验证 TOML，包括可观测参数。
    pub fn from_toml(source: &str) -> Result<Self> {
        let config: Self =
            toml::from_str(source).map_err(|error| Error::InvalidConfig(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// 验证身份、端点和服务绑定，不连接外部依赖。
    pub fn validate(&self) -> Result<()> {
        match (self.ownership, &self.redis_ownership) {
            (OwnershipBackend::Redis, Some(redis)) => {
                redis.validate()?;
                if redis.operation_timeout_ms > self.player_gateway.rpc_timeout_ms {
                    return Err(Error::InvalidConfig(
                        "所有权操作期限不能大于网关调用期限".into(),
                    ));
                }
            }
            (OwnershipBackend::Redis, None) => {
                return Err(Error::InvalidConfig(
                    "Redis 所有权需要 redis_ownership 配置".into(),
                ));
            }
            (_, Some(_)) => {
                return Err(Error::InvalidConfig(
                    "redis_ownership 仅适用于 Redis 所有权".into(),
                ));
            }
            _ => {}
        }
        match (self.discovery, &self.redis_discovery) {
            (DiscoveryBackend::Redis, Some(redis)) => redis.validate()?,
            (DiscoveryBackend::Redis, None) => {
                return Err(Error::InvalidConfig(
                    "Redis 发现需要 redis_discovery 配置".into(),
                ));
            }
            (_, Some(_)) => {
                return Err(Error::InvalidConfig(
                    "redis_discovery 仅适用于 Redis 发现".into(),
                ));
            }
            _ => {}
        }
        self.player_gateway.validate()?;
        self.observability
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        if self.node_id.trim().is_empty() {
            return Err(Error::InvalidConfig("node_id 不能为空".into()));
        }
        if self.discovery == DiscoveryBackend::Redis
            && (self.node_id.len() > 256 || self.services.len() > 64)
        {
            return Err(Error::InvalidConfig(
                "Redis 发现限制 node_id 为 256 字节、每节点最多 64 个服务".into(),
            ));
        }
        if self.services.is_empty() {
            return Err(Error::InvalidConfig("至少需要配置一个服务".into()));
        }
        let mut endpoint_names = BTreeSet::new();
        for endpoint in &self.endpoints {
            let advertised = endpoint.advertise_address.unwrap_or(endpoint.address);
            if advertised.port() == 0
                || (self.discovery == DiscoveryBackend::Redis && advertised.ip().is_unspecified())
            {
                return Err(Error::InvalidConfig(
                    "Redis 发现需要可连接的发布地址".into(),
                ));
            }
            if endpoint.name.trim().is_empty()
                || endpoint.protocol.trim().is_empty()
                || endpoint.address.port() == 0
            {
                return Err(Error::InvalidConfig("端点需要名称、协议和非零端口".into()));
            }
            if !endpoint_names.insert(&endpoint.name) {
                return Err(Error::InvalidConfig(format!(
                    "重复的端点名称：{}",
                    endpoint.name
                )));
            }
        }
        let mut identities = BTreeSet::new();
        for service in &self.services {
            if service
                .factory
                .as_ref()
                .is_some_and(|name| name.trim().is_empty() || name.len() > 64)
            {
                return Err(Error::InvalidConfig("工厂名称无效".into()));
            }
            if service.service_name.trim().is_empty() {
                return Err(Error::InvalidConfig("服务名不能为空".into()));
            }
            if !endpoint_names.contains(&service.endpoint) {
                return Err(Error::InvalidConfig(format!(
                    "服务 {} 引用了不存在的端点：{}",
                    service.service_name, service.endpoint
                )));
            }
            if !identities.insert(&service.service_name) {
                return Err(Error::InvalidConfig(format!(
                    "节点内重复的服务名：{}",
                    service.service_name
                )));
            }
        }
        Ok(())
    }

    /// 返回配置描述，不表示服务已经启动或就绪。
    pub fn service_descriptors(&self) -> Result<Vec<ServiceDescriptor>> {
        self.services
            .iter()
            .map(|service| {
                let endpoint = self
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.name == service.endpoint)
                    .ok_or_else(|| {
                        Error::InvalidConfig(format!("端点不存在：{}", service.endpoint))
                    })?;
                Ok(ServiceDescriptor {
                    node_id: self.node_id.clone(),
                    service_name: service.service_name.clone(),
                    protocol: endpoint.protocol.clone(),
                    address: endpoint.advertise_address.unwrap_or(endpoint.address),
                })
            })
            .collect()
    }
}
