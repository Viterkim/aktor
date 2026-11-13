use aktor::{
    message::LocalFuture,
    worker::{Operations, Registry, WorkerFailure},
};

type Run<S> = for<'a> fn(
    &'a Registry,
    &'a mut S,
    String,
    Vec<u8>,
) -> LocalFuture<'a, Result<Vec<u8>, WorkerFailure>>;

pub struct Service<S> {
    pub registry: Registry,
    pub operations: Operations,
    pub run: Run<S>,
    #[cfg(test)]
    pub pending: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pub cancelled: std::sync::Arc<tokio::sync::Notify>,
}

impl<S> core::ops::Deref for Service<S> {
    type Target = Registry;

    fn deref(&self) -> &Registry {
        &self.registry
    }
}
impl<S> Service<S> {
    pub async fn dispatch(
        &self,
        state: &mut S,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, WorkerFailure> {
        (self.run)(&self.registry, state, name, input).await
    }
}
