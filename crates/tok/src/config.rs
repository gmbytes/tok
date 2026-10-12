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
    /// 配置可校验，运行后端尚未实现。
    Kubernetes,
}

/// 业务服务身份及其端点引用，不决定业务执行或加锁策略。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// 节点内唯一的业务服务名，由游戏定义。
    pub service_name: String,
    /// 引用 endpoints 中的端点名称。
    pub endpoint: String,
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
}

/// 一个游戏进程的配置；未知字段、重复名称与无效引用均被拒绝。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// 部署节点身份，节点内的所有业务服务共享此 ID。
    pub node_id: String,
    /// 服务发现后端选择。
    pub discovery: DiscoveryBackend,
    /// 节点声明的端点列表，实际监听需显式组装。
    pub endpoints: Vec<EndpointConfig>,
    /// 本进程启用的业务服务与端点绑定。
    pub services: Vec<ServiceConfig>,
    /// 进程级日志、指标和追踪配置。
    #[serde(default)]
    pub observability: tok_observability::Config,
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
        self.observability
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        if self.node_id.trim().is_empty() {
            return Err(Error::InvalidConfig("node_id 不能为空".into()));
        }
        if self.services.is_empty() {
            return Err(Error::InvalidConfig("至少需要配置一个服务".into()));
        }
        let mut endpoint_names = BTreeSet::new();
        for endpoint in &self.endpoints {
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
                    address: endpoint.address,
                })
            })
            .collect()
    }
}
