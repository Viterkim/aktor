use super::driver::poll_owners;
use super::*;

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    #[doc(hidden)]
    pub fn register_owner(
        &mut self,
        name: String,
        kind: crate::AktorExecution,
        shutdown: Box<dyn Fn()>,
        owner: LocalFuture<'static, Option<AktorCleanupError>>,
    ) -> Result<(), ActorError> {
        if self.killswitch().is_stopping() {
            return Err(ActorError::Closed);
        }

        if !self.control.listening.get() {
            return Err(ActorError::NotStarted);
        }

        self.control.kinds.borrow_mut().push((name.clone(), kind));
        self.owners.borrow_mut().push(Entry {
            name,
            kind,
            shutdown,
            owner,
            diagnostics: Rc::new(RefCell::new(Vec::new())),
            finished: false,
        });

        self.control.changed.notify();
        Ok(())
    }

    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    pub fn with_grace(grace: Duration) -> Self {
        Self {
            stop_on_drop: false,
            control: Rc::new(Control {
                stopping: Cell::new(false),
                #[cfg(feature = "embassy_cross_core")]
                shared_stopping: Shared::new(AtomicBool::new(false)),
                listening: Cell::new(false),
                grace,
                deadline: Cell::new(None),
                kinds: RefCell::new(Vec::new()),
                failure: RefCell::new(None),
                completed: RefCell::new(None),
                report: RefCell::new(ShutdownReport::default()),
                changed: Event::default(),
            }),
            owners: Rc::new(RefCell::new(Vec::new())),
        }
    }

    #[doc(hidden)]
    pub fn new_registration(&self) -> Self {
        Self {
            stop_on_drop: false,
            control: self.control.clone(),
            owners: self.owners.clone(),
        }
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), AktorError> {
        if !self.control.listening.get() {
            return Err(AktorError::new("actor group has no shutdown listener"));
        }

        Ok(())
    }

    pub fn killswitch(&self) -> KillSwitch<Clock> {
        KillSwitch {
            control: self.control.clone(),
        }
    }

    pub fn completion(&self) -> GroupCompletion<Clock> {
        GroupCompletion {
            control: self.control.clone(),
        }
    }

    pub fn shutdown(&self) -> GroupCompletion<Clock> {
        self.killswitch().stop();
        self.completion()
    }

    pub fn listen(&mut self) -> Result<LocalFuture<'static, ShutdownReport>, AktorError> {
        self.listen_with(async |_| Ok::<_, AktorCleanupError>(()))
    }

    /// Keep an existing value on this executor. Cleanup just drops it.
    pub fn spawn_value<S: 'static, const N: usize>(
        &mut self,
        name: impl Into<String>,
        value: S,
    ) -> Result<Handle<S, N, (), (), Clock>, ActorError> {
        self.spawn(ActorArgs {
            name: name.into(),
            capacity: N,
            setup: async move || Ok(value),
            cleanup: async |_| Ok(()),
        })
    }

    pub fn spawn<S: 'static, const N: usize, E: 'static>(
        &mut self,
        args: ActorArgs<
            impl AsyncFnOnce() -> Result<S, AktorSetupError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorCleanupError<E>> + 'static,
        >,
    ) -> Result<Handle<S, N, E, (), Clock>, ActorError> {
        if args.capacity != N {
            return Err(ActorError::InvalidCapacity);
        }

        self.spawn_in(
            args,
            hooks::AktorHooks::default(),
            Clock::EXECUTION,
            serve_on::<Clock, _, N, E>,
        )
    }

    #[doc(hidden)]
    pub fn spawn_with_hooks<S: 'static, E: 'static>(
        &mut self,
        args: ActorArgs<
            impl AsyncFnOnce() -> Result<S, AktorSetupError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorCleanupError<E>> + 'static,
        >,
        hooks: hooks::AktorHooks<S>,
        kind: crate::AktorExecution,
    ) -> Result<Handle<S, 0, E, (), Clock>, ActorError> {
        self.spawn_in(args, hooks, kind, serve_on::<Clock, _, 0, E>)
    }

    #[doc(hidden)]
    pub fn spawn_with_custom<S: 'static, E: 'static, Runner>(
        &mut self,
        args: ActorArgs<
            impl AsyncFnOnce() -> Result<S, AktorSetupError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorCleanupError<E>> + 'static,
        >,
        hooks: hooks::AktorHooks<S>,
        kind: crate::AktorExecution,
        runner: Runner,
    ) -> Result<Handle<S, 0, E, (), Clock>, ActorError>
    where
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, 0, E>) -> Result<(), AktorError> + 'static,
    {
        self.spawn_in(args, hooks, kind, runner)
    }

    fn spawn_in<S: 'static, const N: usize, E: 'static, Runner>(
        &mut self,
        args: ActorArgs<
            impl AsyncFnOnce() -> Result<S, AktorSetupError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorCleanupError<E>> + 'static,
        >,
        hooks: hooks::AktorHooks<S>,
        kind: crate::AktorExecution,
        runner: Runner,
    ) -> Result<Handle<S, N, E, (), Clock>, ActorError>
    where
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), AktorError> + 'static,
    {
        if self.killswitch().is_stopping() {
            return Err(ActorError::Closed);
        }

        if !self.control.listening.get() {
            return Err(ActorError::NotStarted);
        }

        let (handle, mut owner) = channel_with_clock::<S, N, E, Clock>(args.capacity)?;

        owner.hooks = hooks;
        *handle.inner.group.borrow_mut() = Some((args.name.clone(), self.killswitch().into()));

        let shutdown = handle.new_handle();

        self.control
            .kinds
            .borrow_mut()
            .push((args.name.clone(), kind));
        self.owners.borrow_mut().push(Entry {
            name: args.name,
            diagnostics: owner.inner.completion.diagnostics.clone(),
            kind,
            shutdown: Box::new(move || {
                shutdown.shutdown();
            }),
            owner: Box::pin(async move {
                match owner
                    .run_with_custom(
                        async move || (args.setup)().await,
                        async move |state| (args.cleanup)(state).await,
                        runner,
                    )
                    .await
                {
                    Ok(()) => None,
                    Err(error) => match &*error {
                        OwnerError::Setup(_) => None,
                        OwnerError::Cleanup(error) => Some(error.report()),
                        OwnerError::Runner(error) => Some(error.clone()),
                        OwnerError::Cancelled => Some(AktorError::new("actor owner cancelled")),
                    },
                }
            }),
            finished: false,
        });

        self.control.changed.notify();
        Ok(handle)
    }

    /// Move this future onto your local executor to drive the actors.
    /// Dropping it cancels the actors and publishes a failed report.
    pub fn listen_with<'a, Cleanup, CleanupFuture, E>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<LocalFuture<'a, ShutdownReport>, AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture + 'a,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>> + 'a,
        E: 'a,
    {
        self.claim_listener()?;
        self.stop_on_drop = true;

        let driver = Driver {
            control: self.control.clone(),
            owners: self.owners.clone(),
        };
        let group = Self {
            stop_on_drop: false,
            control: self.control.clone(),
            owners: self.owners.clone(),
        };

        Ok(Box::pin(async move {
            let changed = group.control.changed.listen();

            poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&group.owners, cx, &group.control);

                if group.killswitch().is_stopping() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            drop(changed);

            let report = group.finish(cleanup).await;

            drop(driver);
            report
        }))
    }

    pub(super) fn claim_listener(&self) -> Result<(), AktorError> {
        if self.control.stopping.get() {
            return Err(AktorError::new("this group has already stopped"));
        }

        if self.control.listening.replace(true) {
            Err(AktorError::new(
                "this group already has a shutdown listener",
            ))
        } else {
            Ok(())
        }
    }
}
impl<Clock: AktorGroupClock> Drop for AktorGroup<Clock> {
    fn drop(&mut self) {
        if self.stop_on_drop || !self.control.listening.get() {
            self.killswitch().stop();
        }
    }
}
impl<Clock: AktorGroupClock> Default for AktorGroup<Clock> {
    fn default() -> Self {
        Self::new()
    }
}
impl<Clock: AktorGroupClock> KillSwitch<Clock> {
    #[doc(hidden)]
    pub fn record_diagnostic(&self, error: AktorError) {
        self.control.report.borrow_mut().application.push(error);
    }

    pub fn stop(&self) {
        if !self.control.stopping.replace(true) {
            #[cfg(feature = "embassy_cross_core")]
            self.control.shared_stopping.store(true, Ordering::Release);
            self.control
                .deadline
                .set(Some(Clock::deadline(self.control.grace)));

            if !self.control.listening.get() {
                let mut report = self.control.report.borrow().clone();
                report.failure = self.control.failure.borrow().clone();
                *self.control.completed.borrow_mut() = Some(report);
            }
        }

        self.control.changed.notify();
    }

    pub fn is_stopping(&self) -> bool {
        self.control.stopping.get()
    }

    #[cfg(feature = "embassy_cross_core")]
    #[doc(hidden)]
    pub fn shared_stopping(&self) -> Shared<AtomicBool> {
        self.control.shared_stopping.clone()
    }

    pub async fn wait_stopping(&self) {
        let changed = self.control.changed.listen();

        poll_fn(|cx| {
            changed.register(cx);

            if self.is_stopping() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    pub fn fail(&self, mut reason: ActorFailure) {
        if reason.kind.is_none() && reason.actor != "application" {
            let kinds = self.control.kinds.borrow();
            let mut matching = kinds
                .iter()
                .filter(|(name, _)| name == &reason.actor)
                .map(|(_, kind)| *kind);

            if let Some(first) = matching.next()
                && matching.all(|kind| kind == first)
            {
                reason.kind = Some(first);
            }
        }

        if self.control.failure.borrow().is_none() {
            *self.control.failure.borrow_mut() = Some(reason);
        }

        self.stop();
    }
}

impl<Clock: AktorGroupClock> GroupCompletion<Clock> {
    /// Read the retained report if shutdown has finished.
    pub fn try_report(&self) -> Option<ShutdownReport> {
        self.control.completed.borrow().clone()
    }

    pub async fn wait(&self) -> ShutdownReport {
        let changed = self.control.changed.listen();

        poll_fn(|cx| {
            changed.register(cx);

            match self.control.completed.borrow().clone() {
                Some(report) => Poll::Ready(report),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl<Clock: AktorGroupClock> core::future::IntoFuture for GroupCompletion<Clock> {
    type Output = ShutdownReport;
    type IntoFuture = LocalFuture<'static, ShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, Clock: AktorGroupClock> core::future::IntoFuture for &'a GroupCompletion<Clock> {
    type Output = ShutdownReport;
    type IntoFuture = LocalFuture<'a, ShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl<Clock: AktorGroupClock> Clone for KillSwitch<Clock> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}
impl<Clock: AktorGroupClock> Clone for GroupCompletion<Clock> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}
