//! 配置驱动的 RPC 组装声明；沿用原有 Factory，不建立平行生命周期。
use super::{Application, Error};
use std::convert::Infallible;
use std::{collections::BTreeMap, sync::Arc};
use tok_game_rpc::RpcService;
use tok_lifecycle as lifecycle;
use tok_lifecycle::Factory;
use tok_rpc::{
    Listener,
    tonic::{body::Body, codegen::http, server::NamedService},
    tower::Service,
};

type EndpointBinding = Box<dyn FnOnce(Listener) -> Result<Listener, tok_rpc::Error> + Send>;
pub(super) struct Registration {
    pub(super) endpoint: String,
    pub(super) service: String,
    protocol: &'static str,
    bind: EndpointBinding,
}
struct FactoryBinding {
    factory: Arc<dyn Factory>,
    bindings: BTreeMap<String, tok_game_rpc::Binding>,
}
impl Factory for FactoryBinding {
    fn initialize(&self, mut init: lifecycle::Init) -> lifecycle::InitFuture {
        let binding = self.bindings.get(&init.identity.service_name).cloned();
        let factory = Arc::clone(&self.factory);
        Box::pin(async move {
            let binding =
                binding.ok_or_else(|| std::io::Error::other("初始化服务缺少 RPC 绑定"))?;
            init.extensions.insert(binding.clone());
            let task = factory.initialize(init).await?;
            if !binding.is_installed() {
                return Err(std::io::Error::other("RPC 工厂没有安装处理链").into());
            }
            Ok(task)
        })
    }
}
impl Application {
    fn validate_rpc(&self, service_name: &str, protocol: &'static str) -> Result<String, Error> {
        let service = self
            .config
            .services
            .iter()
            .find(|service| service.service_name == service_name)
            .ok_or_else(|| {
                crate::config::Error::Invalid(format!("RPC 服务未配置：{service_name}"))
            })?;
        let name = service.endpoint.as_deref().ok_or_else(|| {
            crate::config::Error::Invalid(format!("RPC 服务缺少端点：{service_name}"))
        })?;
        if !self
            .config
            .endpoints
            .iter()
            .any(|endpoint| endpoint.name == name && endpoint.protocol == "grpc")
            || self.rpc_endpoints.contains_key(name)
            || self.rpc_registrations.iter().any(|entry| {
                entry.service == service_name
                    || (entry.endpoint == name && entry.protocol == protocol)
            })
        {
            return Err(crate::config::Error::Invalid(format!(
                "RPC 端点协议或绑定冲突：{service_name} / {protocol}"
            ))
            .into());
        }
        Ok(name.to_owned())
    }
    /// 将自定义 protobuf 服务绑定到配置中业务服务的 grpc 端点；生命周期仍由其 Factory 负责。
    /// 同一端点的各项绑定可分别声明；拒绝与整端点 register_rpc_endpoint 混用。
    pub fn bind_rpc<S>(&mut self, service_name: impl Into<String>, service: S) -> Result<(), Error>
    where
        S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let service_name = service_name.into();
        let endpoint = self.validate_rpc(&service_name, S::NAME)?;
        self.rpc_registrations.push(Registration {
            endpoint,
            service: service_name.clone(),
            protocol: S::NAME,
            bind: Box::new(move |endpoint| endpoint.add_service(service_name, service)),
        });
        Ok(())
    }
    /// 注册统一 RPC 工厂；按配置创建入口并将一次性 game_rpc::Binding 注入 Init.extensions。
    /// 工厂必须在初始化中安装实际状态和处理链；protocol 只选择协议，不推导业务锁或中间件。
    /// 参数校验失败不保留部分工厂或绑定；没有匹配配置时仅注册工厂。
    pub fn register_rpc_factory<S>(
        &mut self,
        name: impl Into<String>,
        factory: impl Factory + 'static,
        protocol: impl Fn(RpcService) -> S,
    ) -> Result<(), Error>
    where
        S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let name = name.into();
        if self.factories.contains_key(&name) {
            return Err(Error::DuplicateFactory(name));
        }
        let services: Vec<_> = self
            .config
            .services
            .iter()
            .filter(|service| service.factory_name() == name)
            .map(|service| service.service_name.clone())
            .collect();
        let mut endpoints = std::collections::BTreeSet::new();
        for service in &services {
            let endpoint = self.validate_rpc(service, S::NAME)?;
            if !endpoints.insert(endpoint) {
                return Err(crate::config::Error::Invalid(format!(
                    "同一端点不能绑定重复 protobuf 服务：{}",
                    S::NAME
                ))
                .into());
            }
        }
        let mut bindings = BTreeMap::new();
        let mut registrations = Vec::new();
        for service in services {
            let (rpc, binding) = RpcService::deferred(&self.config.node_id, &service)?;
            let endpoint = self.validate_rpc(&service, S::NAME)?;
            let protocol = protocol(rpc);
            bindings.insert(service.clone(), binding);
            registrations.push(Registration {
                endpoint,
                service: service.clone(),
                protocol: S::NAME,
                bind: Box::new(move |endpoint| endpoint.add_service(service, protocol)),
            });
        }
        self.factories.insert(
            name,
            Arc::new(FactoryBinding {
                factory: Arc::new(factory),
                bindings,
            }),
        );
        self.rpc_registrations.extend(registrations);
        Ok(())
    }
    pub(super) fn assemble_rpc(&mut self) -> Result<(), Error> {
        let mut endpoints = BTreeMap::new();
        for registration in std::mem::take(&mut self.rpc_registrations) {
            let endpoint = endpoints
                .remove(&registration.endpoint)
                .unwrap_or_else(Listener::new);
            endpoints.insert(registration.endpoint, (registration.bind)(endpoint)?);
        }
        for (name, endpoint) in endpoints {
            self.register_rpc_endpoint(name, endpoint)?;
        }
        Ok(())
    }
}
