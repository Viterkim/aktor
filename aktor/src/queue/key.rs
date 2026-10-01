use std::{any::Any, sync::Arc};

#[derive(Clone)]
pub struct LatestKey {
    pub operation: &'static str,
    pub value: Arc<dyn Key>,
}
impl LatestKey {
    pub fn same(&self, other: &Self) -> bool {
        self.operation == other.operation && self.value.same(&*other.value)
    }

    pub fn identical(&self, other: &Self) -> bool {
        self.operation == other.operation && Arc::ptr_eq(&self.value, &other.value)
    }
}

pub trait Key: Send + Sync {
    fn value(&self) -> &dyn Any;
    fn same(&self, other: &dyn Key) -> bool;
}
impl<K: Eq + Send + Sync + 'static> Key for K {
    fn value(&self) -> &dyn Any {
        self
    }

    fn same(&self, other: &dyn Key) -> bool {
        other
            .value()
            .downcast_ref::<K>()
            .is_some_and(|value| value == self)
    }
}
