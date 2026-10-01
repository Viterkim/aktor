use super::super::*;

impl<S, E, C> Actor<S, E, C> {
    pub fn is_running(&self) -> bool {
        self.running.has_changed().is_ok() && *self.running.borrow()
    }

    pub async fn wait_running(&self) -> Result<(), LifecycleError<E>> {
        let mut running = self.running.clone();

        loop {
            if *running.borrow_and_update() {
                return Ok(());
            }

            running
                .changed()
                .await
                .map_err(|_| LifecycleError::Closed)?;
        }
    }

    /// Finish the work already queued and clean up. Calls are rejected while paused.
    pub async fn pause(&self) -> Result<(), LifecycleError<Arc<C>>> {
        let (reply, answer) = oneshot::channel();

        self.commands
            .send(Command::Pause(reply))
            .await
            .map_err(|_| LifecycleError::Closed)?;

        answer.await.map_err(|_| LifecycleError::Closed)?
    }

    /// Build new state on the actor thread. Failed setup leaves the actor paused.
    pub async fn resume<F>(&self, setup: F) -> Result<(), LifecycleError<E>>
    where
        F: FnOnce() -> Result<S, E> + Send + 'static,
        S: 'static,
        E: 'static,
    {
        self.resume_async(move || core::future::ready(setup()))
            .await
    }

    /// Finish admitted work and teardown. Paused actors discard queued work.
    pub async fn shutdown(&self) -> Result<(), LifecycleError<Arc<C>>> {
        let (reply, answer) = oneshot::channel();

        self.commands
            .send(Command::Shutdown(reply))
            .await
            .map_err(|_| LifecycleError::Closed)?;

        let result = answer.await;
        self.closed().await;

        result.map_err(|_| LifecycleError::Closed)?
    }

    /// Replace state even if the caller stops waiting.
    pub async fn replace<F>(&self, setup: F) -> Result<(), ReplaceError<E, C>>
    where
        F: FnOnce() -> Result<S, E> + Send + 'static,
        S: 'static,
        E: 'static,
    {
        self.replace_async(move || core::future::ready(setup()))
            .await
    }

    pub async fn resume_async<F, Fut>(&self, setup: F) -> Result<(), LifecycleError<E>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: core::future::Future<Output = Result<S, E>> + 'static,
    {
        let (reply, answer) = oneshot::channel();

        self.commands
            .send(Command::Resume {
                setup: Box::new(move || Box::pin(setup())),
                reply,
            })
            .await
            .map_err(|_| LifecycleError::Closed)?;

        answer
            .await
            .map_err(|_| LifecycleError::Closed)?
            .take()
            .ok_or(LifecycleError::Closed)?
    }

    pub async fn replace_async<F, Fut>(&self, setup: F) -> Result<(), ReplaceError<E, C>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: core::future::Future<Output = Result<S, E>> + 'static,
    {
        let (reply, answer) = oneshot::channel();

        self.commands
            .send(Command::Replace {
                setup: Box::new(move || Box::pin(setup())),
                reply,
            })
            .await
            .map_err(|_| ReplaceError::Closed)?;

        answer
            .await
            .map_err(|_| ReplaceError::Closed)?
            .take()
            .ok_or(ReplaceError::Closed)?
    }

    pub async fn closed(&self) {
        let mut running = self.running.clone();

        while running.changed().await.is_ok() {}
    }

    pub fn new_controller(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            running: self.running.clone(),
            abandoned_setup: self.abandoned_setup.clone(),
        }
    }

    /// Setup errors whose callers stopped waiting after admission.
    pub fn take_abandoned_setup(&self) -> Vec<AbandonedSetup<E>> {
        std::mem::take(&mut *self.abandoned_setup.lock())
    }
}
impl<S, E, C> Clone for Actor<S, E, C> {
    fn clone(&self) -> Self {
        self.new_controller()
    }
}
impl<S, E, C> core::fmt::Debug for Actor<S, E, C> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Actor")
            .field("running", &self.is_running())
            .finish()
    }
}
