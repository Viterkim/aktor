use super::*;
use crate::queue;
use core::future::Future;

impl<S: 'static> Aktor<S> {
    pub(crate) fn closed_handle(kill: crate::AktorKillSwitch) -> Self {
        let (handle, mailbox) = queue::closed_channel();

        handle.admission.manage(kill.clone());
        handle.admission.shutdown();
        drop(mailbox);

        let (_, cleanup) = watch::channel(None);
        let (_, result) = watch::channel(Some((
            Err(completion_error(er_new!(OwnerError::CleanupUnconfirmed))),
            Vec::new(),
        )));

        Self {
            handle: Handle::new(handle),
            completion: OwnerCompletion { result, cleanup },
            kill,
        }
    }
}
impl<S, Role> Aktor<S, Role> {
    /// Run a job beside this actor's calls. Its capacity bounds the number of unfinished jobs.
    #[track_caller]
    pub fn spawn<O, E>(
        &self,
        work: impl Future<Output = O> + Send + 'static,
        policy: crate::AktorJob,
    ) -> Result<crate::AktorOwnerTask<O::Value>, (crate::AktorSpawnError, String)>
    where
        O: crate::AktorJobOutput<E> + 'static,
        O::Value: Send + 'static,
    {
        self.handle.spawn(work, policy)
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub fn run<F: IntoFuture>(&self, work: F) -> crate::AktorRun<F::IntoFuture> {
        let work = work.into_future();

        self.handle.run(work)
    }

    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.kill.registered_id()
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped.
    pub fn group_completion(&self) -> crate::AktorGroupCompletion {
        self.kill.completion()
    }

    /// Keep this to observe the dedicated thread after dropping the actor handle.
    pub fn completion(&self) -> OwnerCompletion {
        self.completion.clone()
    }

    /// A plain handle for calls, without the owner's shutdown and completion.
    #[doc(hidden)]
    pub fn handle(&self) -> Handle<S, Role> {
        self.handle.clone()
    }

    pub fn with_role<NewRole>(self) -> Aktor<S, NewRole> {
        Aktor {
            handle: self.handle.with_role(),
            completion: self.completion,
            kill: self.kill,
        }
    }
}
impl<S, Role> Clone for Aktor<S, Role> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            completion: self.completion.clone(),
            kill: self.kill.clone(),
        }
    }
}
impl<S: 'static, Role> Aktor<S, Role> {
    #[doc(hidden)]
    pub fn killswitch(&self) -> crate::AktorKillSwitch {
        self.kill.clone()
    }

    /// Use this just before returning a final reply when the state can no longer serve calls.
    /// Starts group shutdown. This actor discards queued calls and runs `end` after the current call.
    pub fn retire(&self) {
        self.kill.retire_self();
        self.handle.inner.retire();
    }

    #[doc(hidden)]
    /// Finish queued calls and cleanup. Once started, it keeps going if you stop awaiting it.
    pub fn shutdown(&self) -> OwnerCompletion {
        self.handle.inner.request_close();
        self.completion.clone()
    }
}

impl OwnerCompletion {
    pub fn cleanup_returned(&self) -> bool {
        matches!(
            *self.cleanup.borrow(),
            Some(Ok(()) | Err((CleanupError::Failed(_), _, _)))
        )
    }

    /// Wait for this actor’s `end` to return. An earlier call panic still lets `end` succeed.
    pub async fn wait_cleanup(&self) -> Result<(), (CleanupError, String)> {
        let mut cleanup = self.cleanup.clone();
        let returned = cleanup
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|returned| returned.clone());

        match returned {
            Some(Ok(())) => Ok(()),
            Some(Err((error, delivery, report))) => {
                delivery.receive();
                Err((error, report))
            }
            None => er_bail!(CleanupError::Unconfirmed(None)),
        }
    }

    #[doc(hidden)]
    pub async fn wait(&self) -> Result<(), (Arc<OwnerError>, String)> {
        let mut result = self.result.clone();

        loop {
            if let Some((outcome, returned)) = result.borrow_and_update().clone() {
                if outcome.is_err() {
                    for delivery in returned {
                        delivery.receive();
                    }
                }
                return outcome;
            }

            if let Err(error) = result.changed().await {
                return Err(completion_error(er_new!(OwnerError::RuntimeStopped, error)));
            }
        }
    }
}

pub(super) fn completion_error(error: ErTree<OwnerError>) -> (Arc<OwnerError>, String) {
    let (error, report) = error.er_split();
    (Arc::new(error), report)
}

#[doc(hidden)]
impl core::future::IntoFuture for OwnerCompletion {
    type Output = Result<(), (Arc<OwnerError>, String)>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
#[doc(hidden)]
impl<'a> core::future::IntoFuture for &'a OwnerCompletion {
    type Output = Result<(), (Arc<OwnerError>, String)>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl OwnerError {
    /// Whether the OS thread was joined. Cleanup may have failed or been skipped.
    pub fn physically_joined(&self) -> bool {
        match self {
            OwnerError::Cleanup | OwnerError::CleanupUnconfirmed | OwnerError::Cancelled => true,
            OwnerError::Panicked(cause) | OwnerError::PanickedWithCleanup { cause, .. } => {
                cause.physically_joined
            }
            OwnerError::RuntimeStopped => false,
        }
    }
}

impl Clone for OwnerCompletion {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            cleanup: self.cleanup.clone(),
        }
    }
}
