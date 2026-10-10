use super::Lifecycle;
use crate::{
    AktorFailure, AktorSetupError,
    listener::{Actor, LifecycleError, ReplaceError},
};
use core::{future::Future, ops::Deref};

impl<S: 'static> Lifecycle<S> {
    pub async fn resume<F>(&self, setup: F) -> Result<(), LifecycleError>
    where
        F: FnOnce() -> Result<S, AktorSetupError> + Send + 'static,
    {
        self.resume_async(move || core::future::ready(setup()))
            .await
    }

    pub async fn resume_async<F, Fut>(&self, setup: F) -> Result<(), LifecycleError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, AktorSetupError>> + 'static,
    {
        self.inner.resume_async(setup).await
    }

    pub async fn replace<F>(&self, setup: F) -> Result<(), ReplaceError>
    where
        F: FnOnce() -> Result<S, AktorSetupError> + Send + 'static,
    {
        self.replace_async(move || core::future::ready(setup()))
            .await
    }

    pub async fn replace_async<F, Fut>(&self, setup: F) -> Result<(), ReplaceError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, AktorSetupError>> + 'static,
    {
        self.inner.replace_async(setup).await
    }
}
impl<S> Lifecycle<S> {
    pub fn new_controller(&self) -> Self {
        Self {
            inner: self.inner.new_controller(),
        }
    }
}
impl<S> Clone for Lifecycle<S> {
    fn clone(&self) -> Self {
        self.new_controller()
    }
}
impl<S> Deref for Lifecycle<S> {
    type Target = Actor<S, AktorSetupError, AktorFailure>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
