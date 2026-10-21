use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::Arc,
};

type Values = BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>;

/// 类型化元信息与能力集合；克隆以 O(1) 共享整张表，修改时写时复制。
/// 空集合不分配表；表复制只克隆值的 Arc；不深拷贝能力对象，也不强制业务状态进入共享锁。
#[derive(Clone, Default)]
pub struct Extensions {
    values: Option<Arc<Values>>,
}
impl Extensions {
    /// 安装同类型能力并返回旧值；克隆集合后替换其中的值不影响原集合。
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<Arc<T>> {
        self.values_mut()
            .insert(TypeId::of::<T>(), Arc::new(value))
            .and_then(|old| old.downcast::<T>().ok())
    }
    /// 合并本进程能力，同类型由传入集合覆盖；不修改其他克隆的集合。
    pub fn extend(&mut self, other: &Self) {
        let Some(values) = &other.values else {
            return;
        };
        if self.values.is_none() {
            self.values = Some(Arc::clone(values));
        } else {
            self.values_mut()
                .extend(values.iter().map(|(key, value)| (*key, Arc::clone(value))));
        }
    }
    /// 移除当前集合的指定能力；不修改其他克隆集合。
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<Arc<T>> {
        let key = TypeId::of::<T>();
        if !self.values.as_ref()?.contains_key(&key) {
            return None;
        }
        let values = self.values_mut();
        let old = values
            .remove(&key)
            .and_then(|value| value.downcast::<T>().ok());
        if values.is_empty() {
            self.values = None;
        }
        old
    }
    /// 查询能力；可变状态应由能力自身的类型与并发规则表达。
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.values
            .as_ref()?
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }
    fn values_mut(&mut self) -> &mut Values {
        Arc::make_mut(self.values.get_or_insert_with(|| Arc::new(Values::new())))
    }
}
impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field(
                "types",
                &self.values.as_ref().map_or(0, |values| values.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clones_share_the_table_until_mutation_and_keep_values_shared() {
        let mut original = Extensions::default();
        assert!(original.values.is_none());
        assert!(original.clone().values.is_none());
        original.insert(String::from("original"));
        original.insert(7u32);
        let mut cloned = original.clone();
        assert!(Arc::ptr_eq(
            original.values.as_ref().unwrap(),
            cloned.values.as_ref().unwrap()
        ));
        assert!(cloned.remove::<bool>().is_none());
        cloned.extend(&Extensions::default());
        assert!(Arc::ptr_eq(
            original.values.as_ref().unwrap(),
            cloned.values.as_ref().unwrap()
        ));
        cloned.insert(String::from("changed"));
        assert!(!Arc::ptr_eq(
            original.values.as_ref().unwrap(),
            cloned.values.as_ref().unwrap()
        ));
        assert!(std::ptr::eq(
            original.get::<u32>().unwrap(),
            cloned.get::<u32>().unwrap()
        ));
        assert_eq!(original.get::<String>().unwrap(), "original");
        cloned.remove::<u32>().unwrap();
        assert_eq!(original.get::<u32>(), Some(&7));
        let mut merged = original.clone();
        merged.extend(&cloned);
        assert_eq!(merged.get::<String>().unwrap(), "changed");
        assert_eq!(original.get::<String>().unwrap(), "original");
    }
}
