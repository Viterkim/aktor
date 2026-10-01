use super::*;
use crate::listener::{DedicatedStartError, SpawnArgs, WeakHandle, spawn_async};
use core::{fmt, future::Future};
use std::sync::atomic::Ordering;

impl<S: 'static, E: Send + 'static, C: Send + Sync + 'static> Aktor<S, E, C> {
    /// Start the actor on its own thread. Returns after setup succeeds.
    pub async fn spawn<Setup, Cleanup>(
        args: SpawnArgs<Setup, Cleanup>,
    ) -> Result<Self, DedicatedStartError<E>>
    where
        Setup: FnOnce() -> Result<S, E> + Send + 'static,
        Cleanup: FnMut(S) -> Result<(), C> + Send + 'static,
    {
        let SpawnArgs {
            name,
            capacity,
            failure,
            setup,
            mut cleanup,
        } = args;

        Self::spawn_async(SpawnArgs {
            name,
            capacity,
            failure,
            setup: move || core::future::ready(setup()),
            cleanup: move |state| core::future::ready(cleanup(state)),
        })
        .await
    }

    /// Async setup and cleanup run on the actor's thread too.
    pub async fn spawn_async<Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
    ) -> Result<Self, DedicatedStartError<E>>
    where
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, E>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), C>>,
    {
        let runtime = runtime::Handle::try_current().map_err(|_| DedicatedStartError::NoRuntime)?;
        let (handle, actor, thread) = spawn_async(args).await?;
        let (completed, result) = watch::channel(None);

        runtime.spawn(async move {
            let outcome = match thread.join_async().await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(Arc::new(OwnerError::Cleanup(error))),
                Err(error) => Err(Arc::new(OwnerError::Panicked(error))),
            };
            completed.send_replace(Some(outcome));
        });

        Ok(Self {
            handle,
            actor,
            completion: OwnerCompletion { result },
            runtime,
            shutdown_started: AtomicBool::new(false),
        })
    }
}

impl<S, E, C, Role> Aktor<S, E, C, Role> {
    pub fn new_handle(&self) -> Handle<S, Role> {
        self.handle.new_handle()
    }

    pub fn downgrade(&self) -> WeakHandle<S, Role> {
        self.handle.downgrade()
    }

    pub fn with_role<NewRole>(self) -> Aktor<S, E, C, NewRole> {
        Aktor {
            handle: self.handle.with_role(),
            actor: self.actor,
            completion: self.completion,
            runtime: self.runtime,
            shutdown_started: self.shutdown_started,
        }
    }
}
impl<S: 'static, E: Send + 'static, C: Send + Sync + 'static, Role> Aktor<S, E, C, Role> {
    /// Finish queued calls and cleanup. Once started, it keeps going if you stop awaiting it.
    /// Keep your Tokio runtime running until it's done.
    pub async fn shutdown(&self) -> Result<(), Arc<OwnerError<C>>> {
        if !self.shutdown_started.swap(true, Ordering::AcqRel) {
            let actor = self.actor.new_controller();
            self.runtime.spawn(async move {
                let _result = actor.shutdown().await;
            });
        }

        self.completion.wait().await
    }
}

impl<C> OwnerCompletion<C> {
    pub fn new_observer(&self) -> Self {
        Self {
            result: self.result.clone(),
        }
    }

    /// Wait for completion. You can get the same result again later.
    pub async fn wait(&self) -> Result<(), Arc<OwnerError<C>>> {
        let mut result = self.result.clone();

        loop {
            if let Some(outcome) = result.borrow_and_update().clone() {
                return outcome;
            }

            if result.changed().await.is_err() {
                return Err(Arc::new(OwnerError::RuntimeStopped));
            }
        }
    }
}

impl<C: fmt::Debug> fmt::Display for OwnerError<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cleanup(error) => {
                write!(
                    formatter,
                    "actor cleanup failed {} time(s)",
                    error.errors.len()
                )?;

                for failure in &error.errors {
                    write!(formatter, ": {failure:?}")?;
                }

                Ok(())
            }
            Self::Panicked(error) => write!(formatter, "actor thread failed: {error}"),
            Self::RuntimeStopped => formatter.write_str("owner runtime stopped before completion"),
        }
    }
}
impl<C: fmt::Debug> core::error::Error for OwnerError<C> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Panicked(error) => Some(error),
            Self::Cleanup(_) | Self::RuntimeStopped => None,
        }
    }
}
