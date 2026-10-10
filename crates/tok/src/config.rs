use std::{collections::BTreeSet, net::SocketAddr};

use serde::Deserialize;
use tok_core::{Error, Result, ServiceDescriptor, ServiceKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryBackend {
    Memory,
    Kubernetes,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub kind: ServiceKind,
    pub instance_id: String,
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub node_id: String,
    pub discovery: DiscoveryBackend,
    pub roles: Vec<RoleConfig>,
}

impl AppConfig {
    pub fn from_toml(source: &str) -> Result<Self> {
        let config: Self =
            toml::from_str(source).map_err(|error| Error::InvalidConfig(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.node_id.trim().is_empty() {
            return Err(Error::InvalidConfig("node_id 不能为空".into()));
        }
        if self.roles.is_empty() {
            return Err(Error::InvalidConfig("至少需要配置一个角色".into()));
        }
        let mut identities = BTreeSet::new();
        for role in &self.roles {
            if role.instance_id.trim().is_empty() || role.address.port() == 0 {
                return Err(Error::InvalidConfig("角色需要实例 ID 和非零端口".into()));
            }
            if !identities.insert((role.kind, &role.instance_id)) {
                return Err(Error::InvalidConfig(format!(
                    "重复的服务实例：{}",
                    role.instance_id
                )));
            }
        }
        Ok(())
    }

    /// 返回配置描述，不表示角色已经启动或就绪。
    pub fn service_descriptors(&self) -> Vec<ServiceDescriptor> {
        self.roles
            .iter()
            .map(|role| ServiceDescriptor {
                instance_id: role.instance_id.clone(),
                kind: role.kind,
                address: role.address,
            })
            .collect()
    }
}
