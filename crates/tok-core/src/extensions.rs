use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::Arc,
};

/// 类型化元信息与能力集合；克隆共享已有值，不强制业务状态进入共享锁。
#[derive(Clone, Default)]
pub struct Extensions {
    values: BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>,
}
impl Extensions {
    /// 安装同类型能力并返回旧值；克隆集合后替换其中的值不影响原集合。
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<Arc<T>> {
        self.values
            .insert(TypeId::of::<T>(), Arc::new(value))
            .and_then(|old| old.downcast::<T>().ok())
    }
    /// 合并本进程能力，同类型由传入集合覆盖；不修改其他克隆的集合。
    pub fn extend(&mut self, other: &Self) {
        self.values.extend(
            other
                .values
                .iter()
                .map(|(key, value)| (*key, Arc::clone(value))),
        );
    }
    /// 移除当前集合的指定能力；不修改其他克隆集合。
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<Arc<T>> {
        self.values
            .remove(&TypeId::of::<T>())
            .and_then(|value| value.downcast::<T>().ok())
    }
    /// 查询能力；可变状态应由能力自身的类型与并发规则表达。
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }
}
impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("types", &self.values.len())
            .finish()
    }
}
