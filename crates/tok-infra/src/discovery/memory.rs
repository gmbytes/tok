use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, RwLock},
};

use tok_core::{Error, Result, ServiceDescriptor, ServiceDirectory};

type Services = BTreeMap<(String, String), ServiceDescriptor>;

/// 单进程多角色共享的服务目录，不访问外部注册后端。
#[derive(Debug, Clone, Default)]
pub struct MemoryDirectory {
    services: Arc<RwLock<Services>>,
}

impl MemoryDirectory {
    pub fn register(&self, service: ServiceDescriptor) -> Result<()> {
        let mut services = self
            .services
            .write()
            .map_err(|_| Error::DirectoryUnavailable)?;
        let key = (service.node_id.clone(), service.service_name.clone());
        match services.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(service);
                Ok(())
            }
            Entry::Occupied(_) => Err(Error::DuplicateService(format!(
                "{}/{}",
                service.node_id, service.service_name
            ))),
        }
    }

    pub fn unregister(&self, node_id: &str, service_name: &str) -> Result<bool> {
        let mut services = self
            .services
            .write()
            .map_err(|_| Error::DirectoryUnavailable)?;
        Ok(services
            .remove(&(node_id.to_owned(), service_name.to_owned()))
            .is_some())
    }
}

impl ServiceDirectory for MemoryDirectory {
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>> {
        let services = self
            .services
            .read()
            .map_err(|_| Error::DirectoryUnavailable)?;
        Ok(services
            .values()
            .filter(|service| service.service_name == service_name)
            .cloned()
            .collect())
    }
}
