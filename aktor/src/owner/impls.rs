use super::*;
use crate::listener::{DedicatedStartError, FailurePolicy, SpawnArgs, WeakHandle};
use crate::{AktorCleanupOutput, AktorExecution, AktorFailure};
use core::{fmt, future::Future};

impl<S: 'static> Aktor<S> {
    /// Start the actor on its own thread. Returns after setup succeeds.
    #[doc(hidden)]
    pub async fn spawn<Setup, Cleanup, Output>(
        args: SpawnArgs<Setup, Cleanup>,
    ) -> Result<Self, (StartError, String)>
    where
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> Result<S, String> + Send + 'static,
        Cleanup: FnMut(S) -> Output + Send + 'static,
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
    #[doc(hidden)]
    pub async fn spawn_async<Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        args: SpawnArgs<Setup, Cleanup>,
    ) -> Result<Self, (StartError, String)>
    where
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        Self::spawn_async_with_hooks(args, crate::listener::hooks::CallHooks::default()).await
    }

    #[doc(hidden)]
    pub async fn spawn_async_with_hooks<Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
    ) -> Result<Self, (StartError, String)>
    where
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        Self::spawn_async_on(args, hooks, false).await
    }

    #[doc(hidden)]
    pub async fn spawn_async_on<Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
        standard: bool,
    ) -> Result<Self, (StartError, String)>
    where
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        let SpawnArgs {
            name,
            capacity,
            failure,
            setup,
            mut cleanup,
        } = args;
        let kill = match &failure {
            FailurePolicy::Group(kill) => Some(kill.clone()),
            FailurePolicy::Unwind | FailurePolicy::Abort | FailurePolicy::Shutdown(_) => None,
        };
        let actor = name.clone();
        let kind = if standard {
            AktorExecution::StdThread
        } else {
            AktorExecution::TokioThread
        };
        let args = SpawnArgs {
            name,
            capacity,
            failure,
            setup: move || async move {
                match setup().await {
                    Ok(state) => Ok(state),
                    Err(report) => {
                        let error = crate::AktorError::new(report, ());

                        Err(match kill {
                            Some(kill) => kill.startup_error(actor, kind, error),
                            None => AktorFailure::from(error),
                        })
                    }
                }
            },
            cleanup: move |state| {
                let future = cleanup(state);
                async move { AktorCleanupOutput::into_cleanup(future.await).map_err(AktorFailure::from) }
            },
        };

        Self::spawn_async_captured(args, hooks, standard)
            .await
            .map_err(|error| StartError::from(error).into_report())
    }
}

impl<S: 'static> Aktor<S> {
    async fn spawn_async_captured<Initial, Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
        standard: bool,
    ) -> Result<Self, DedicatedStartError<Initial>>
    where
        Initial: Send + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, Initial>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorFailure>>,
    {
        let runtime = crate::executor::Spawner::current(standard)
            .map_err(|_| DedicatedStartError::NoRuntime)?;
        let (owner, observing) =
            Self::spawn_observed(args, hooks, standard, None, crate::AktorStuck::Kill).await?;
        let observing = Arc::new(parking_lot::Mutex::new(Some(observing)));
        let scheduled = observing.clone();
        let result = runtime.spawn_named("aktor join", async move {
            let observing = scheduled.lock().take();

            if let Some(observing) = observing {
                observing.await;
            }
        });

        if let Err(error) = result {
            owner.control.request_shutdown();

            let observing = observing.lock().take();

            if let Some(observing) = observing {
                observing.await;
            }

            let completion = owner.completion.observe().await;
            return Err(DedicatedStartError::Observer {
                cause: error,
                completion,
            });
        }

        Ok(owner)
    }

    #[doc(hidden)]
    pub async fn spawn_observed<Initial, Setup, SetupFuture, Cleanup, CleanupFuture>(
        args: SpawnArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
        standard: bool,
        cancelled: Option<watch::Receiver<bool>>,
        stuck: crate::AktorStuck,
    ) -> Result<(Self, impl Future<Output = ()> + Send + 'static), DedicatedStartError<Initial>>
    where
        Initial: Send + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, Initial>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorFailure>>,
    {
        let runtime = crate::executor::Spawner::current(standard)
            .map_err(|_| DedicatedStartError::NoRuntime)?;
        let (handle, actor, thread) = crate::listener::lifecycle::spawn::spawn_async_initial_on::<
            S,
            Initial,
            AktorFailure,
            _,
            _,
            _,
            _,
        >(args, hooks, standard, cancelled, stuck)
        .await?;
        let (completed, result) = watch::channel(None);
        let observer = actor.clone();
        let cleanup = actor.cleanup_receipt();
        let observing = async move {
            let outcome = match runtime.join(thread).await {
                Ok(Ok(())) if !observer.cleanup_completed() => {
                    Err(Arc::new(OwnerError::CleanupUnconfirmed))
                }
                Ok(Ok(())) if observer.cut_call() => Err(Arc::new(OwnerError::Cancelled)),
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(Arc::new(OwnerError::Cleanup(error))),
                Err(cause) => {
                    let cleanup = observer.cleanup_errors();
                    let error = if cleanup.errors.is_empty() && cleanup.diagnostics.is_empty() {
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
                control: Lifecycle { inner: actor },
                completion: OwnerCompletion { result, cleanup },
            },
            observing,
        ))
    }
}
impl<S, Role> Aktor<S, Role> {
    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        self.handle.run(work).await
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor has no group.
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        self.handle.group_completion()
    }

    /// Keep this to observe the dedicated thread after dropping the actor handle.
    pub fn completion(&self) -> OwnerCompletion {
        self.completion.clone()
    }

    /// A plain handle for calls, without the owner's shutdown and completion.
    pub fn handle(&self) -> Handle<S, Role> {
        self.handle.clone()
    }

    pub fn downgrade(&self) -> WeakHandle<S, Role> {
        self.handle.downgrade()
    }

    pub fn with_role<NewRole>(self) -> Aktor<S, NewRole> {
        Aktor {
            handle: self.handle.with_role(),
            control: self.control,
            completion: self.completion,
        }
    }
}
impl<S, Role> Clone for Aktor<S, Role> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            control: self.control.clone(),
            completion: self.completion.clone(),
        }
    }
}
impl<S: 'static, Role> Aktor<S, Role> {
    /// Use this just before returning a final reply when the state can no longer serve calls.
    /// Starts group shutdown. This actor discards queued calls and runs `end` after the current call.
    pub fn retire(&self) {
        let inner = &self.handle.inner;
        if let Some(group) = inner.admission.group() {
            group.stop();
        }

        inner.sender.retire();
        self.control.close_admission();
    }

    #[doc(hidden)]
    /// Finish queued calls and cleanup. Once started, it keeps going if you stop awaiting it.
    /// Owners started with `Aktor::spawn` need their runtime running until completion.
    pub fn shutdown(&self) -> OwnerCompletion {
        self.control.request_shutdown();
        self.completion.clone()
    }
}

impl OwnerCompletion {
    /// Read the current outcome of `end` without waiting or formatting its report.
    pub fn cleanup_status(&self) -> crate::AktorCleanupStatus {
        use crate::AktorCleanupStatus;

        let closed = self.cleanup.has_changed().is_err();
        match &*self.cleanup.borrow() {
            Some(Ok(())) => AktorCleanupStatus::Succeeded,
            Some(Err(_)) => AktorCleanupStatus::Failed,
            None if closed => AktorCleanupStatus::Unconfirmed,
            None => AktorCleanupStatus::Pending,
        }
    }

    /// Wait for this actor’s `end` to return. An earlier call panic still lets `end` succeed.
    pub async fn wait_cleanup(&self) -> Result<(), (CleanupError, String)> {
        crate::listener::lifecycle::wait_cleanup(self.cleanup.clone())
            .await
            .map_err(lifecycle::cleanup_error)
    }

    /// Wait for completion. You can get the same result again later.
    pub async fn wait(&self) -> Result<(), (Arc<OwnerError>, String)> {
        self.observe().await.map_err(|error| {
            let report = error.report();
            (error, report)
        })
    }

    pub(crate) async fn observe(&self) -> Result<(), Arc<OwnerError>> {
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

impl fmt::Display for OwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cleanup(error) => {
                write!(
                    formatter,
                    "actor cleanup failed {} time(s)",
                    error.errors.len() + error.diagnostics.len()
                )?;

                if formatter.alternate() {
                    for failure in &error.errors {
                        write!(formatter, ": {}", failure.diagnostics)?;
                    }

                    for diagnostic in &error.diagnostics {
                        write!(formatter, ": {}", diagnostic.diagnostics)?;
                    }
                }

                Ok(())
            }
            Self::Panicked(error) => {
                formatter.write_str("actor thread failed")?;

                if formatter.alternate() {
                    write!(formatter, ": {error}")?;
                }

                Ok(())
            }
            Self::PanickedWithCleanup { cause, cleanup } => {
                formatter.write_str("actor thread failed")?;

                if formatter.alternate() {
                    write!(formatter, ": {cause}")?;
                }

                if !cleanup.errors.is_empty() || !cleanup.diagnostics.is_empty() {
                    write!(formatter, "; cleanup failed")?;
                }

                if formatter.alternate() {
                    for error in &cleanup.errors {
                        write!(formatter, ": {}", error.diagnostics)?;
                    }

                    for diagnostic in &cleanup.diagnostics {
                        write!(formatter, "; {}", diagnostic.diagnostics)?;
                    }
                }

                Ok(())
            }
            Self::CleanupUnconfirmed => write!(formatter, "{}", CleanupError::Unconfirmed),
            Self::Cancelled => formatter.write_str("actor work was cancelled during shutdown"),
            Self::RuntimeStopped => formatter.write_str("owner runtime stopped before completion"),
        }
    }
}

impl core::future::IntoFuture for OwnerCompletion {
    type Output = Result<(), (Arc<OwnerError>, String)>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a> core::future::IntoFuture for &'a OwnerCompletion {
    type Output = Result<(), (Arc<OwnerError>, String)>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl fmt::Debug for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
impl core::error::Error for OwnerError {}

impl OwnerError {
    pub(crate) fn report(&self) -> String {
        format!("{self:#}")
    }

    /// Whether the OS thread was joined. Cleanup may have failed or been skipped.
    pub fn physically_joined(&self) -> bool {
        match self {
            OwnerError::Cleanup(_) | OwnerError::CleanupUnconfirmed | OwnerError::Cancelled => true,
            OwnerError::Panicked(cause) | OwnerError::PanickedWithCleanup { cause, .. } => {
                cause.physically_joined
            }
            OwnerError::RuntimeStopped => false,
        }
    }
}

#[cfg(all(test, feature = "std_thread", feature = "tokio"))]
mod tests {
    use super::*;
    use crate::{AktorArgs, AktorGroup};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[tokio::test]
    async fn owner_handoff() {
        handoff(false).await;
        notification(false).await;
    }

    #[tokio::test]
    async fn group_handoff() {
        handoff(true).await;
        notification(true).await;
    }

    #[tokio::test]
    async fn join_handoff() {
        for failed_cleanup in [false, true] {
            join_failure(failed_cleanup).await;
        }
    }

    #[tokio::test]
    async fn startup_join() {
        struct Tail;
        impl Drop for Tail {
            fn drop(&mut self) {
                panic!("startup join tail failed");
            }
        }

        for cancelled in [false, true] {
            let mut group = AktorGroup::with_grace(Duration::from_secs(1));
            group.start().unwrap();
            let kill = group.killswitch();
            let tail = Tail;
            let error = group
                .spawn_async(AktorArgs {
                    name: "startup join".into(),
                    capacity: 1,
                    setup: move || async move {
                        if cancelled {
                            kill.stop();
                            core::future::pending::<Result<(), String>>().await
                        } else {
                            Err(String::from("original setup failure"))
                        }
                    },
                    cleanup: move |_: ()| {
                        let _retained = &tail;
                        core::future::ready(Ok::<_, String>(()))
                    },
                })
                .await
                .err()
                .unwrap();

            let DedicatedStartError::Startup {
                join: crate::listener::StartupJoin::Failed(join),
                ..
            } = &error.0.cause
            else {
                panic!("startup lost its failed join");
            };
            assert!(join.physically_joined);

            let report = group.completion().wait().await;
            assert_eq!(
                report.actors[0].retirement,
                crate::AktorRetireStatus::Terminated
            );
            assert!(
                report.to_string().contains("startup join tail failed"),
                "{report}"
            );
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
                setup: || core::future::ready(Ok::<_, String>(())),
                cleanup: move |_| {
                    let entered = cleanup_entered.clone();
                    let release = cleanup_release.clone();
                    let cleanup = cleanup.clone();

                    async move {
                        entered.notify_one();
                        release.notified().await;
                        cleanup.fetch_add(1, Ordering::Relaxed);

                        if failed_cleanup {
                            Err(String::from("cleanup also failed"))
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

        let Err((error, report)) = result else {
            panic!("completion scheduling failure was lost");
        };

        let DedicatedStartError::Observer { cause, completion } = &error.cause else {
            panic!("observer failure lost its typed causes");
        };

        assert!(cause.to_string().contains("injected"));
        match completion {
            Ok(()) => assert!(!failed_cleanup),
            Err(error) => {
                assert!(failed_cleanup);
                assert!(error.physically_joined());
                assert!(report.contains("cleanup also failed"));
            }
        }

        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }

    async fn notification(grouped: bool) {
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
        let args = SpawnArgs {
            name: "notification".into(),
            capacity: 1,
            failure: FailurePolicy::Unwind,
            setup: || core::future::ready(Ok::<_, String>(0_u32)),
            cleanup: move |_| {
                cleanup.fetch_add(1, Ordering::Relaxed);
                core::future::ready(Ok::<_, String>(()))
            },
        };
        let mut group = AktorGroup::new();
        let owner = if grouped {
            group.start_standard().unwrap();
            group
                .spawn_async_on(
                    AktorArgs::new(args.name, args.setup, args.cleanup),
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

        if grouped {
            group.killswitch().stop();
            let report = group.completion().wait().await;
            assert!(
                report
                    .to_string()
                    .contains("subscriber notification failed"),
                "{report}"
            );
            assert_eq!(
                report.actors[0].retirement,
                crate::AktorRetireStatus::Confirmed
            );
        } else {
            let _notification =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.shutdown()));
        }

        let result =
            tokio::time::timeout(Duration::from_millis(250), owner.completion.wait()).await;

        if result.is_err() {
            owner.control.shutdown().await.unwrap();
        }

        drop((input, output));
        assert!(result.is_ok(), "notification lost shutdown progress");
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }

    async fn handoff(grouped: bool) {
        crate::executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor shutdown")));

        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();
        let args = SpawnArgs {
            name: "handoff".into(),
            capacity: 1,
            failure: FailurePolicy::Unwind,
            setup: || core::future::ready(Ok::<_, String>(0_u32)),
            cleanup: move |_| {
                cleanup.fetch_add(1, Ordering::Relaxed);
                core::future::ready(Ok::<_, String>(()))
            },
        };

        let mut group = AktorGroup::new();
        let owner = if grouped {
            group.start_standard().unwrap();
            group
                .spawn_async_on(
                    AktorArgs::new(args.name, args.setup, args.cleanup),
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

        // Stop the owner if the completion observer never finished.
        if completed.is_err() {
            owner.control.shutdown().await.unwrap();
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
impl Clone for OwnerCompletion {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            cleanup: self.cleanup.clone(),
        }
    }
}
