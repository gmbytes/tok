//! 单进程共享的内存服务目录与合并变更通知。

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, RwLock},
};
use tok_core::BoxFuture;

use tok_core::{DirectoryWatch, Error, Result, ServiceDescriptor, ServiceDirectory};
use tokio::sync::watch;

type Services = BTreeMap<(String, String), ServiceDescriptor>;

/// 单进程多服务共享的服务目录，不访问外部注册后端。
#[derive(Debug, Clone)]
#[expect(
    clippy::module_name_repetitions,
    reason = "公开类型保留后端或所有权角色名称，便于调用方区分 Memory 与 Redis 实现。"
)]
pub struct MemoryDirectory {
    services: Arc<RwLock<Services>>,
    updates: Arc<watch::Sender<u64>>,
}

impl Default for MemoryDirectory {
    fn default() -> Self {
        Self {
            services: Arc::new(RwLock::new(BTreeMap::new())),
            updates: Arc::new(watch::channel(0).0),
        }
    }
}

struct Changes(watch::Receiver<u64>);
impl DirectoryWatch for Changes {
    fn changed(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0
                .changed()
                .await
                .map_err(|_| Error::DirectoryStateUnavailable)
        })
    }
}

impl MemoryDirectory {
    /// 注册节点业务服务并通知订阅者，重复身份保留原条目并返回错误。
    pub fn register(&self, service: ServiceDescriptor) -> Result<()> {
        let mut services = self
            .services
            .write()
            .map_err(|_| Error::DirectoryStateUnavailable)?;
        let key = (service.node_id.clone(), service.service_name.clone());
        match services.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(service);
                self.updates
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                Ok(())
            }
            Entry::Occupied(_) => Err(Error::RegistrationConflict(format!(
                "节点 {} 的服务 {} 已注册",
                service.node_id, service.service_name
            ))),
        }
    }

    /// 移除指定节点业务服务，返回是否存在；实际移除后才发送变更通知。
    pub fn unregister(&self, node_id: &str, service_name: &str) -> Result<bool> {
        let mut services = self
            .services
            .write()
            .map_err(|_| Error::DirectoryStateUnavailable)?;
        let removed = services
            .remove(&(node_id.to_owned(), service_name.to_owned()))
            .is_some();
        if removed {
            self.updates
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(removed)
    }
}

impl ServiceDirectory for MemoryDirectory {
    fn subscribe(&self) -> Result<Box<dyn DirectoryWatch>> {
        Ok(Box::new(Changes(self.updates.subscribe())))
    }
    fn snapshot(&self, service_name: &str) -> Result<Vec<ServiceDescriptor>> {
        let services = self
            .services
            .read()
            .map_err(|_| Error::DirectoryStateUnavailable)?;
        Ok(services
            .values()
            .filter(|service| service.service_name == service_name)
            .cloned()
            .collect())
    }
}

impl tok_core::discovery::ServiceRegistry for MemoryDirectory {
    fn register(&self, service: ServiceDescriptor) -> BoxFuture<'_, tok_core::Result<()>> {
        Box::pin(async move { MemoryDirectory::register(self, service) })
    }

    fn unregister<'a>(
        &'a self,
        node_id: &'a str,
        service_name: &'a str,
    ) -> BoxFuture<'a, tok_core::Result<bool>> {
        Box::pin(async move { MemoryDirectory::unregister(self, node_id, service_name) })
    }
}
