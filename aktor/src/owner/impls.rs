use super::*;
use crate::listener::{DedicatedStartError, SpawnArgs, WeakHandle};
use core::{fmt, future::Future};

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
        Self::spawn_async_with_hooks(args, crate::listener::hooks::AktorHooks::default()).await
    }

    pub async fn spawn_async_with_hooks<Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
    ) -> Result<Self, DedicatedStartError<crate::AktorSetupError<E>>>
    where
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<C>>>,
    {
        Self::spawn_async_on(args, hooks, false).await
    }

    pub async fn spawn_async_on<Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
        standard: bool,
    ) -> Result<Self, DedicatedStartError<crate::AktorSetupError<E>>>
    where
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<C>>>,
    {
        let runtime = crate::executor::Spawner::current(standard)
            .map_err(|_| DedicatedStartError::NoRuntime)?;
        let (owner, observing) = Self::spawn_observed(args, hooks, standard).await?;
        let observing = Arc::new(parking_lot::Mutex::new(Some(observing)));
        let scheduled = observing.clone();
        let result = runtime.spawn_named("aktor join", async move {
            let observing = scheduled.lock().take();

            if let Some(observing) = observing {
                observing.await;
            }
        });

        if let Err(error) = result {
            owner.actor.request_shutdown();

            let observing = observing.lock().take();

            if let Some(observing) = observing {
                observing.await;
            }

            if let Err(cleanup) = owner.completion.wait().await {
                return Err(DedicatedStartError::Thread(std::io::Error::new(
                    error.kind(),
                    format!("{error}; {cleanup}"),
                )));
            }

            return Err(DedicatedStartError::Thread(error));
        }

        Ok(owner)
    }

    #[doc(hidden)]
    pub async fn spawn_observed<Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
        standard: bool,
    ) -> Result<
        (Self, impl Future<Output = ()> + Send + 'static),
        DedicatedStartError<crate::AktorSetupError<E>>,
    >
    where
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<C>>>,
    {
        let runtime = crate::executor::Spawner::current(standard)
            .map_err(|_| DedicatedStartError::NoRuntime)?;
        let (handle, actor, thread) =
            crate::listener::lifecycle::spawn::spawn_async_on(args, hooks, standard).await?;
        let (completed, result) = watch::channel(None);
        let observer = actor.new_controller();
        let observing = async move {
            let outcome = match runtime.join(thread).await {
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
        };

        Ok((
            Self {
                handle,
                actor,
                completion: OwnerCompletion { result },
            },
            observing,
        ))
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
        }
    }
}
impl<S: 'static, E: Send + 'static, C: Send + Sync + 'static, Role> Aktor<S, E, C, Role> {
    /// Finish queued calls and cleanup. Once started, it keeps going if you stop awaiting it.
    /// Keep your Tokio runtime running until it's done.
    pub fn shutdown(&self) -> OwnerCompletion<C> {
        self.actor.request_shutdown();
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

#[cfg(all(test, feature = "std_thread", feature = "tokio"))]
mod tests {
    use super::*;
    use crate::{ActorArgs, AktorCleanupError, AktorGroup, AktorSetupError, FailurePolicy};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[tokio::test]
    async fn owner_handoff() {
        handoff(false).await;
        notification().await;
        replacement().await;
    }

    #[tokio::test]
    async fn group_handoff() {
        handoff(true).await;
    }

    #[tokio::test]
    async fn join_handoff() {
        for failed_cleanup in [false, true] {
            join_failure(failed_cleanup).await;
        }
    }

    async fn join_failure(failed_cleanup: bool) {
        use tokio::sync::Notify;

        crate::executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor join")));

        let entered = Arc::new(Notify::new());
        let cleanup_entered = entered.clone();
        let release = Arc::new(Notify::new());
        let cleanup_release = release.clone();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();
        let mut starting = Box::pin(Aktor::spawn_async_on(
            SpawnArgs {
                name: "join handoff".into(),
                capacity: 1,
                failure: FailurePolicy::Unwind,
                setup: || core::future::ready(Ok::<_, AktorSetupError>(())),
                cleanup: move |_| {
                    let entered = cleanup_entered.clone();
                    let release = cleanup_release.clone();
                    let cleanup = cleanup.clone();

                    async move {
                        entered.notify_one();
                        release.notified().await;
                        cleanup.fetch_add(1, Ordering::Relaxed);

                        if failed_cleanup {
                            Err(AktorCleanupError::new("cleanup also failed"))
                        } else {
                            Ok(())
                        }
                    }
                },
            },
            Default::default(),
            true,
        ));

        let early = tokio::select! {
            biased;
            result = &mut starting => Some(result),
            _ = entered.notified() => None,
        };

        release.notify_one();

        let result = match early {
            Some(result) => result,
            None => starting.await,
        };

        crate::executor::FAIL_SPAWN.with(|setting| setting.set(None));

        let Err(DedicatedStartError::Thread(error)) = result else {
            panic!("completion scheduling failure was lost");
        };

        assert_eq!(
            error.to_string().contains("cleanup also failed"),
            failed_cleanup
        );
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }

    async fn notification() {
        use crate::latest::Session;
        use futures_util::Stream;
        use std::{
            pin::Pin,
            sync::atomic::AtomicBool,
            task::{Context, Wake, Waker},
        };

        struct PanicWake(AtomicBool);
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                if !self.0.swap(true, Ordering::Relaxed) {
                    panic!("subscriber notification failed");
                }
            }
        }

        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();
        let owner = Aktor::spawn_async_on(
            SpawnArgs {
                name: "notification".into(),
                capacity: 1,
                failure: FailurePolicy::Unwind,
                setup: || core::future::ready(Ok::<_, AktorSetupError>(0_u32)),
                cleanup: move |_| {
                    cleanup.fetch_add(1, Ordering::Relaxed);
                    core::future::ready(Ok::<_, AktorCleanupError>(()))
                },
            },
            Default::default(),
            true,
        )
        .await
        .unwrap();

        let (input, mut output) = (&owner.handle).session(
            crate::operation::Operation {
                name: "notification",
                caller: std::panic::Location::caller(),
            },
            async |_: &mut u32, _: ()| (),
        );

        let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(false))));

        assert!(
            Pin::new(&mut output)
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let _notification =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.shutdown()));
        let result =
            tokio::time::timeout(Duration::from_millis(250), owner.completion.wait()).await;

        if result.is_err() {
            owner.actor.shutdown().await.unwrap();
        }

        drop((input, output));
        assert!(result.is_ok(), "notification lost shutdown progress");
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }

    async fn replacement() {
        use crate::message::call_async;
        use futures_util::FutureExt;
        use tokio::sync::oneshot;

        let cleaned = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let cleanup = cleaned.clone();
        let owner = Aktor::spawn_async_on(
            SpawnArgs {
                name: "replacement handoff".into(),
                capacity: 1,
                failure: FailurePolicy::Unwind,
                setup: || core::future::ready(Ok::<_, AktorSetupError>(1_u32)),
                cleanup: move |state| {
                    cleanup.lock().push(state);
                    core::future::ready(Ok::<_, AktorCleanupError>(()))
                },
            },
            Default::default(),
            true,
        )
        .await
        .unwrap();

        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let reply = call_async(
            &owner.handle,
            async |_: &mut u32, ()| {
                entered.send(()).unwrap();
                released.await.unwrap();
            },
            (),
        )
        .send()
        .await;

        entering.await.unwrap();

        let mut replacing = Box::pin(owner.actor.replace(|| Ok(2)));

        assert!(replacing.as_mut().now_or_never().is_none());
        drop(replacing);

        let completion = owner.shutdown();

        release.send(()).unwrap();
        reply.await;
        tokio::time::timeout(Duration::from_millis(250), completion.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*cleaned.lock(), [1, 2]);
    }

    async fn handoff(grouped: bool) {
        crate::executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor shutdown")));

        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();
        let args = SpawnArgs {
            name: "handoff".into(),
            capacity: 1,
            failure: FailurePolicy::Unwind,
            setup: || core::future::ready(Ok::<_, AktorSetupError>(0_u32)),
            cleanup: move |_| {
                cleanup.fetch_add(1, Ordering::Relaxed);
                core::future::ready(Ok::<_, AktorCleanupError>(()))
            },
        };

        let mut group = AktorGroup::new();
        let owner = if grouped {
            group.start_standard().unwrap();
            group
                .spawn_async_on(
                    ActorArgs::new(args.name, args.setup, args.cleanup),
                    Default::default(),
                    true,
                )
                .await
                .unwrap()
        } else {
            Aktor::spawn_async_on(args, Default::default(), true)
                .await
                .unwrap()
        };

        let completion = if grouped {
            group.killswitch().stop();
            owner.completion()
        } else {
            drop(owner.shutdown());
            owner.shutdown()
        };

        let completed = tokio::time::timeout(Duration::from_millis(250), completion.wait()).await;

        crate::executor::FAIL_SPAWN.with(|setting| setting.set(None));

        // Leave no stuck owner behind when running this against the broken handoff.
        if completed.is_err() {
            owner.actor.shutdown().await.unwrap();
        }

        if grouped {
            assert!(!group.shutdown().wait().await.failed());
        }

        assert!(
            completed.unwrap().is_ok(),
            "shutdown scheduling lost its owner"
        );
        assert!(owner.shutdown().await.is_ok());
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }
}
