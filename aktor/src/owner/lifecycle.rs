use super::{CleanupError, Lifecycle};
use crate::{
    AktorFailure,
    listener::{self, AktorControl},
};
use core::ops::Deref;

impl Lifecycle {
    pub async fn shutdown(&self) -> Result<(), (CleanupError, String)> {
        self.inner.shutdown().await.map_err(cleanup_error)
    }
}

pub(super) fn cleanup_error(error: listener::CleanupError<AktorFailure>) -> (CleanupError, String) {
    match error {
        listener::CleanupError::Returned(failure) => {
            (CleanupError::Failed, failure.diagnostics.clone())
        }
        listener::CleanupError::Unconfirmed => (
            CleanupError::Unconfirmed,
            CleanupError::Unconfirmed.to_string(),
        ),
    }
}

impl Clone for Lifecycle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl Deref for Lifecycle {
    type Target = AktorControl<AktorFailure>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
