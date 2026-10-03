use super::*;
use crate::listener::{DedicatedStartError, SpawnArgs, WeakHandle, spawn_async};
use core::{fmt, future::Future};
use std::sync::atomic::Ordering;

impl<S: 'static, E: Send + 'static, C: Send + Sync + 'static>
    Aktor<S, crate::AktorSetupError<E>, crate::AktorCleanupError<C>>
{
    /// Start the actor on its own thread. Returns after setup succeeds.
    pub async fn spawn<Setup, Cleanup>(
        args: SpawnArgs<Setup, Cleanup>,
    ) -> Result<Self, DedicatedStartError<crate::AktorSetupError<E>>>
    where
        Setup: FnOnce() -> Result<S, crate::AktorSetupError<E>> + Send + 'static,
        Cleanup: FnMut(S) -> Result<(), crate::AktorCleanupError<C>> + Send + 'static,
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
    ) -> Result<Self, DedicatedStartError<crate::AktorSetupError<E>>>
    where
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<C>>>,
    {
        let runtime = runtime::Handle::try_current().map_err(|_| DedicatedStartError::NoRuntime)?;
        let (handle, actor, thread) = spawn_async(args).await?;
        let (completed, result) = watch::channel(None);
        let observer = actor.new_controller();

        runtime.spawn(async move {
            let outcome = match thread.join_async().await {
                Ok(Ok(())) if observer.is_cancelled() => Err(Arc::new(OwnerError::Cancelled)),
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(Arc::new(OwnerError::Cleanup(error))),
                Err(cause) => {
                    let cleanup = observer.cleanup_errors();
                    let error = if cleanup.errors.is_empty() {
                        OwnerError::Panicked(cause)
                    } else {
                        OwnerError::PanickedWithCleanup { cause, cleanup }
                    };

                    Err(Arc::new(error))
                }
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
    pub fn completion(&self) -> OwnerCompletion<C> {
        self.completion.new_observer()
    }

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
    pub fn shutdown(&self) -> OwnerCompletion<C> {
        if !self.shutdown_started.swap(true, Ordering::AcqRel) {
            self.actor.close_admission();
            let actor = self.actor.new_controller();
            self.runtime.spawn(async move {
                let _result = actor.shutdown().await;
            });
        }

        self.completion.new_observer()
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
            Self::PanickedWithCleanup { cause, cleanup } => {
                write!(formatter, "actor thread failed: {cause}; cleanup failed")?;

                for error in &cleanup.errors {
                    write!(formatter, ": {error:?}")?;
                }

                Ok(())
            }
            Self::Cancelled => formatter.write_str("actor work was cancelled during shutdown"),
            Self::RuntimeStopped => formatter.write_str("owner runtime stopped before completion"),
        }
    }
}
impl<C: fmt::Debug> core::error::Error for OwnerError<C> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Panicked(error) => Some(error),
            Self::PanickedWithCleanup { cause, .. } => Some(cause),
            Self::Cleanup(_) | Self::RuntimeStopped | Self::Cancelled => None,
        }
    }
}

impl<C: Send + Sync + 'static> core::future::IntoFuture for OwnerCompletion<C> {
    type Output = Result<(), Arc<OwnerError<C>>>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, C: Send + Sync + 'static> core::future::IntoFuture for &'a OwnerCompletion<C> {
    type Output = Result<(), Arc<OwnerError<C>>>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}
