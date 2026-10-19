use super::{Lifecycle, LifecycleError, ReplaceError};
use crate::{
    AktorDiagnostic, AktorFailure, AktorSetupError,
    listener::{Actor, LifecycleError as RawLifecycleError, ReplaceError as RawReplaceError},
};
use core::{convert::Infallible, fmt, future::Future, ops::Deref};
use std::sync::Arc;

impl<S: 'static> Lifecycle<S> {
    pub async fn pause(&self) -> Result<(), LifecycleError<Arc<AktorFailure>>> {
        self.inner.pause().await.map_err(LifecycleError::from)
    }

    pub async fn shutdown(&self) -> Result<(), LifecycleError<Arc<AktorFailure>>> {
        self.inner.shutdown().await.map_err(LifecycleError::from)
    }

    pub async fn wait_running(&self) -> Result<(), LifecycleError<Infallible>> {
        self.inner
            .wait_running()
            .await
            .map_err(LifecycleError::from)
    }

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
        self.inner
            .resume_async(setup)
            .await
            .map_err(LifecycleError::from)
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
        self.inner
            .replace_async(setup)
            .await
            .map_err(ReplaceError::from)
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

impl<E> LifecycleError<E> {
    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostics {
            Some(diagnostics) => f.write_str(&diagnostics.report),
            None => fmt::Display::fmt(&self.error, f),
        }
    }
}
impl From<RawLifecycleError> for LifecycleError {
    fn from(error: RawLifecycleError) -> Self {
        let diagnostics = match &error {
            RawLifecycleError::Failed(failure) => Some(AktorDiagnostic::new(
                "actor setup failed",
                failure.diagnostics.clone(),
            )),
            RawLifecycleError::Closed | RawLifecycleError::AlreadyRunning => None,
        };

        Self { error, diagnostics }
    }
}
impl From<RawLifecycleError<Arc<AktorFailure>>> for LifecycleError<Arc<AktorFailure>> {
    fn from(error: RawLifecycleError<Arc<AktorFailure>>) -> Self {
        let diagnostics = match &error {
            RawLifecycleError::Failed(failure) => Some(AktorDiagnostic::new(
                "actor cleanup failed",
                failure.diagnostics.clone(),
            )),
            RawLifecycleError::Closed | RawLifecycleError::AlreadyRunning => None,
        };

        Self { error, diagnostics }
    }
}
impl From<RawLifecycleError<Infallible>> for LifecycleError<Infallible> {
    fn from(error: RawLifecycleError<Infallible>) -> Self {
        Self {
            error,
            diagnostics: None,
        }
    }
}
impl From<RawReplaceError> for ReplaceError {
    fn from(error: RawReplaceError) -> Self {
        let diagnostics = match &error {
            RawReplaceError::Cleanup(failure) => Some(AktorDiagnostic::new(
                "actor cleanup failed",
                failure.diagnostics.clone(),
            )),
            RawReplaceError::Setup(failure) => Some(AktorDiagnostic::new(
                "actor setup failed",
                failure.diagnostics.clone(),
            )),
            RawReplaceError::Closed => None,
        };

        Self { error, diagnostics }
    }
}
impl ReplaceError {
    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostics {
            Some(diagnostics) => f.write_str(&diagnostics.report),
            None => fmt::Display::fmt(&self.error, f),
        }
    }
}
