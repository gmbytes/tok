use std::{collections::BTreeSet, net::SocketAddr};

use serde::Deserialize;
use tok_core::{Error, Result, ServiceDescriptor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryBackend {
    Memory,
    Kubernetes,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub service_name: String,
    pub endpoint: String,
}

/// 节点内可共享的监听端点；协议名由传输实现解释。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    pub name: String,
    pub protocol: String,
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub node_id: String,
    pub discovery: DiscoveryBackend,
    pub endpoints: Vec<EndpointConfig>,
    pub roles: Vec<RoleConfig>,
    #[serde(default)]
    pub observability: tok_observability::Config,
}

impl AppConfig {
    pub fn from_toml(source: &str) -> Result<Self> {
        let config: Self =
            toml::from_str(source).map_err(|error| Error::InvalidConfig(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.observability
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        if self.node_id.trim().is_empty() {
            return Err(Error::InvalidConfig("node_id 不能为空".into()));
        }
        if self.roles.is_empty() {
            return Err(Error::InvalidConfig("至少需要配置一个角色".into()));
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
        for role in &self.roles {
            if role.service_name.trim().is_empty() {
                return Err(Error::InvalidConfig("服务名不能为空".into()));
            }
            if !endpoint_names.contains(&role.endpoint) {
                return Err(Error::InvalidConfig(format!(
                    "服务 {} 引用了不存在的端点：{}",
                    role.service_name, role.endpoint
                )));
            }
            if !identities.insert(&role.service_name) {
                return Err(Error::InvalidConfig(format!(
                    "节点内重复的服务名：{}",
                    role.service_name
                )));
            }
        }
        Ok(())
    }

    /// 返回配置描述，不表示角色已经启动或就绪。
    pub fn service_descriptors(&self) -> Result<Vec<ServiceDescriptor>> {
        self.roles
            .iter()
            .map(|role| {
                let endpoint = self
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.name == role.endpoint)
                    .ok_or_else(|| {
                        Error::InvalidConfig(format!("端点不存在：{}", role.endpoint))
                    })?;
                Ok(ServiceDescriptor {
                    node_id: self.node_id.clone(),
                    service_name: role.service_name.clone(),
                    protocol: endpoint.protocol.clone(),
                    address: endpoint.address,
                })
            })
            .collect()
    }
}
