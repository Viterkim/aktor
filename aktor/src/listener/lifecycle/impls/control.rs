use super::super::*;

impl<C> AktorControl<C> {
    pub fn close_admission(&self) {
        self.admission.shutdown();
    }

    pub fn request_shutdown(&self) {
        // Subscriber wakes can panic. The owner must already have its stop.
        self.shutdown.send_replace(true);
        self.close_admission();
    }

    /// Cancel running async work after the shutdown budget is spent. Cleanup keeps running.
    pub fn cancel(&self) {
        if self
            .admission
            .group()
            .is_some_and(|group| !group.is_stopping())
        {
            self.admission.lost();
        }

        self.force.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.force.borrow()
    }

    pub fn is_running(&self) -> bool {
        self.running.has_changed().is_ok() && *self.running.borrow()
    }

    /// Drain admitted calls, wait for the listener to close and return its cleanup result.
    pub async fn shutdown(&self) -> Result<(), CleanupError<C>> {
        self.request_shutdown();
        self.closed().await;
        wait_cleanup(self.cleanup.clone()).await
    }

    pub async fn closed(&self) {
        let mut running = self.running.clone();

        while running.changed().await.is_ok() {}
    }

    fn new_controller(&self) -> Self {
        Self {
            admission: self.admission.clone(),
            force: self.force.clone(),
            shutdown: self.shutdown.clone(),
            running: self.running.clone(),
            failed_cleanup: self.failed_cleanup.clone(),
            cleanup: self.cleanup.clone(),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn cleanup_receipt(&self) -> CleanupReceipt<C> {
        self.cleanup.clone()
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn cleanup_completed(&self) -> bool {
        self.cleanup.borrow().is_some()
    }

    pub fn cleanup_errors(&self) -> CleanupErrors<C> {
        let saved = self.failed_cleanup.lock();

        CleanupErrors {
            errors: saved.errors.clone(),
            diagnostics: saved.diagnostics.clone(),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn cleanup_ran_out(&self) -> bool {
        self.failed_cleanup.lock().ran_out
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn cut_call(&self) -> bool {
        self.failed_cleanup.lock().cut_call
    }

    #[doc(hidden)]
    pub fn group_primary(&self) -> bool {
        self.failed_cleanup.lock().group_primary
    }
}
impl<C> Clone for AktorControl<C> {
    fn clone(&self) -> Self {
        self.new_controller()
    }
}
impl<C> core::fmt::Debug for AktorControl<C> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("AktorControl")
            .field("running", &self.is_running())
            .finish()
    }
}

pub(crate) async fn wait_cleanup<C>(mut receipt: CleanupReceipt<C>) -> Result<(), CleanupError<C>> {
    let returned = receipt
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|value| value.clone());

    match returned {
        Some(Ok(())) => Ok(()),
        Some(Err(error)) => Err(CleanupError::Returned(error)),
        None => Err(CleanupError::Unconfirmed),
    }
}
