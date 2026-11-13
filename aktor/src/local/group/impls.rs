use super::driver::poll_owners;
use super::*;
use crate::AktorError;
use crate::{AktorGroupError, AktorGroupRetirement, AktorShutdownOutput};

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    #[doc(hidden)]
    pub fn register_owner(
        &mut self,
        name: String,
        kind: crate::AktorExecution,
        shutdown: Box<dyn Fn()>,
        cancel: Option<Box<dyn Fn()>>,
        cut_call: Option<Rc<Cell<bool>>>,
        owner: LocalFuture<'static, Option<AktorError>>,
    ) -> Result<(), AktorStateError> {
        self.register_observed_owner(
            name,
            kind,
            shutdown,
            cancel,
            Box::new(move || OwnerStatus {
                ran_past_grace: cut_call.as_ref().is_some_and(|cut| cut.get()),
                ..OwnerStatus::default()
            }),
            owner,
        )
        .map_err(|error| match error {
            AktorGroupError::NotStarted => AktorStateError::NotStarted,
            _ => AktorStateError::Closed,
        })
    }

    pub(crate) fn register_observed_owner(
        &mut self,
        name: String,
        kind: crate::AktorExecution,
        shutdown: Box<dyn Fn()>,
        cancel: Option<Box<dyn Fn()>>,
        status: Box<dyn Fn() -> OwnerStatus>,
        owner: LocalFuture<'static, Option<AktorError>>,
    ) -> Result<(), AktorGroupError> {
        self.check_started()?;

        self.control.kinds.borrow_mut().push((name.clone(), kind));
        self.owners.borrow_mut().push(Entry {
            status,
            name,
            kind,
            shutdown,
            cancel,
            owner,
            diagnostics: Rc::new(RefCell::new(Vec::new())),
            finished: false,
        });

        self.control.changed.notify();
        Ok(())
    }

    #[doc(hidden)]
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    #[doc(hidden)]
    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    pub fn with_grace(grace: Duration) -> Self {
        Self {
            stop_on_drop: false,
            startup_failure: None,
            control: Rc::new(Control {
                stopping: Cell::new(false),
                starting: Cell::new(false),
                #[cfg(feature = "embassy_cross_core")]
                shared_stopping: Shared::new(AtomicBool::new(false)),
                listening: Cell::new(false),
                shutdown_hook: RefCell::new(None),
                callers: RefCell::new(Vec::new()),
                grace,
                deadline: Cell::new(None),
                kinds: RefCell::new(Vec::new()),
                failure: RefCell::new(None),
                completed: RefCell::new(None),
                report: RefCell::new(AktorShutdownReport::default()),
                changed: Event::default(),
            }),
            owners: Rc::new(RefCell::new(Vec::new())),
        }
    }

    #[doc(hidden)]
    pub fn track_startup(&mut self) {
        self.startup_failure = Some(Rc::new(Cell::new(false)));
    }

    #[doc(hidden)]
    pub fn new_registration(&self) -> Self {
        Self {
            stop_on_drop: false,
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
            owners: self.owners.clone(),
        }
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), AktorGroupError> {
        if self.control.completed.borrow().is_some() {
            return Err(AktorGroupError::Stopped);
        }
        if self.control.stopping.get() {
            return Err(AktorGroupError::Stopping);
        }
        if !self.control.listening.get() {
            return Err(AktorGroupError::NotStarted);
        }

        Ok(())
    }

    pub fn killswitch(&self) -> AktorKillSwitch<Clock> {
        AktorKillSwitch {
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
        }
    }

    pub fn completion(&self) -> AktorGroupCompletion<Clock> {
        AktorGroupCompletion {
            control: self.control.clone(),
        }
    }

    #[doc(hidden)]
    /// Your hook gets the report after actor cleanup, before completion is published.
    pub fn on_shutdown<Hook, Output, Mode>(&self, hook: Hook) -> Result<(), AktorGroupError>
    where
        Hook: FnOnce(AktorShutdownReport) -> Output + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'static,
    {
        let error = if self.killswitch().is_stopping() {
            Some(AktorGroupError::Stopping)
        } else if self.control.shutdown_hook.borrow().is_some() {
            Some(AktorGroupError::ShutdownTaken)
        } else {
            None
        };

        if let Some(error) = error {
            if let Some(dropped) = super::driver::contain(|| drop(hook)) {
                self.killswitch().record_diagnostic(dropped);
            }

            return Err(error);
        }

        *self.control.shutdown_hook.borrow_mut() = Some(Box::new(move |report| {
            Box::pin(async move { hook(report).into_shutdown().await })
        }));
        Ok(())
    }

    /// Run dependent application work on this group's local driver.
    pub fn spawn_task<F>(&self, future: F) -> Result<(), AktorStateError>
    where
        F: Future<Output = ()> + 'static,
    {
        if self.killswitch().is_stopping() {
            if let Some(error) = super::driver::contain(|| drop(future)) {
                self.killswitch().record_diagnostic(error);
            }

            return Err(AktorStateError::Closed);
        }

        if !self.control.listening.get() {
            return Err(AktorStateError::NotStarted);
        }

        self.control.callers.borrow_mut().push(Box::pin(future));
        self.control.changed.notify();
        Ok(())
    }

    pub fn shutdown(&self) -> AktorGroupCompletion<Clock> {
        self.killswitch().stop();
        self.completion()
    }

    pub fn listen(&mut self) -> Result<LocalFuture<'static, AktorShutdownReport>, AktorGroupError> {
        self.listen_with(async |_| Ok::<_, String>(()))
    }

    #[doc(hidden)]
    /// Keep an existing value on this executor. Cleanup just drops it.
    pub fn spawn_value<S: 'static, const N: usize>(
        &mut self,
        name: impl Into<String>,
        value: S,
    ) -> Result<Handle<S, N, (), (), Clock>, AktorStateError> {
        self.spawn_args(AktorArgs {
            name: name.into(),
            capacity: N,
            setup: async move || Ok(value),
            cleanup: async |_| Ok(()),
        })
    }

    #[doc(hidden)]
    pub fn spawn_args<S: 'static, const N: usize, E: 'static>(
        &mut self,
        args: AktorArgs<
            impl AsyncFnOnce() -> Result<S, AktorError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorError<E>> + 'static,
        >,
    ) -> Result<Handle<S, N, E, (), Clock>, AktorStateError> {
        if args.capacity != N {
            return Err(AktorStateError::InvalidCapacity);
        }

        self.spawn_in(
            args,
            hooks::CallHooks::default(),
            Clock::EXECUTION,
            serve_on::<Clock, _, N, E>,
        )
    }

    #[doc(hidden)]
    pub fn spawn_with_hooks<S: 'static, E: 'static>(
        &mut self,
        args: AktorArgs<
            impl AsyncFnOnce() -> Result<S, AktorError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorError<E>> + 'static,
        >,
        hooks: hooks::CallHooks<S>,
        kind: crate::AktorExecution,
    ) -> Result<Handle<S, 0, E, (), Clock>, AktorStateError> {
        self.spawn_in(args, hooks, kind, serve_on::<Clock, _, 0, E>)
    }

    #[doc(hidden)]
    pub fn spawn_with_custom<S: 'static, E: 'static, Runner, SetupOutput, CleanupOutput>(
        &mut self,
        args: AktorArgs<
            impl AsyncFnOnce() -> SetupOutput + 'static,
            impl AsyncFnOnce(S) -> CleanupOutput + 'static,
        >,
        hooks: hooks::CallHooks<S>,
        kind: crate::AktorExecution,
        runner: Runner,
    ) -> Result<Handle<S, 0, E, (), Clock>, AktorStateError>
    where
        SetupOutput: crate::local::impls::runner::LocalSetupOutput<S, E>,
        CleanupOutput: crate::local::impls::runner::LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, 0, E>) -> Result<(), AktorError> + 'static,
    {
        self.spawn_in(args, hooks, kind, runner)
    }

    fn spawn_in<S: 'static, const N: usize, E: 'static, Runner, SetupOutput, CleanupOutput>(
        &mut self,
        args: AktorArgs<
            impl AsyncFnOnce() -> SetupOutput + 'static,
            impl AsyncFnOnce(S) -> CleanupOutput + 'static,
        >,
        hooks: hooks::CallHooks<S>,
        kind: crate::AktorExecution,
        runner: Runner,
    ) -> Result<Handle<S, N, E, (), Clock>, AktorStateError>
    where
        SetupOutput: crate::local::impls::runner::LocalSetupOutput<S, E>,
        CleanupOutput: crate::local::impls::runner::LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), AktorError> + 'static,
    {
        if self.killswitch().is_stopping() {
            return Err(AktorStateError::Closed);
        }

        if !self.control.listening.get() {
            return Err(AktorStateError::NotStarted);
        }

        let (handle, mut owner) = channel_with_clock::<S, N, E, Clock>(args.capacity)?;

        owner.hooks = hooks;
        *handle.inner.group.borrow_mut() =
            Some((args.name.as_str().into(), self.killswitch().into()));

        let shutdown = handle.clone();
        let cancel = handle.clone();

        self.control
            .kinds
            .borrow_mut()
            .push((args.name.clone(), kind));
        let completion = owner.inner.completion.clone();
        self.owners.borrow_mut().push(Entry {
            status: Box::new(move || OwnerStatus {
                cleanup_completed: Some(completion.cleanup_completed.get()),
                ran_past_grace: completion.cut_call.get() || completion.ran_out.get(),
                panic_reported: completion.panic_reported.get(),
                cleanup_diagnostic: None,
            }),
            name: args.name,
            diagnostics: owner.inner.completion.diagnostics.clone(),
            kind,
            shutdown: Box::new(move || {
                shutdown.shutdown();
            }),
            cancel: Some(Box::new(move || {
                cancel.cancel_calls();
            })),
            owner: Box::pin(async move {
                match owner
                    .run_prepared(
                        async move || (args.setup)().await,
                        async move |state| (args.cleanup)(state).await,
                        runner,
                    )
                    .await
                {
                    Ok(()) => None,
                    Err((error, _)) => match &error {
                        OwnerError::Setup(_) | OwnerError::SetupPanic(_) => None,
                        OwnerError::SetupDiagnostic(error, primary) => {
                            if *primary {
                                None
                            } else {
                                Some(error.clone())
                            }
                        }
                        OwnerError::Cleanup(error) => {
                            Some(AktorError::new(error.diagnostics.clone(), ()))
                        }
                        OwnerError::Runner(error) => Some(error.clone()),
                        OwnerError::CleanupUnconfirmed => None,
                        OwnerError::CleanupFailure(error) => {
                            if error.primary {
                                None
                            } else {
                                Some(AktorError::new(error.diagnostics.clone(), ()))
                            }
                        }
                        OwnerError::Cancelled => Some(AktorError::new("actor owner cancelled", ())),
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
    pub fn listen_with<'a, Cleanup, Output, Mode>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<LocalFuture<'a, AktorShutdownReport>, AktorGroupError>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + 'a,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'a,
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
            startup_failure: self.startup_failure.clone(),
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

            let report = group
                .finish(move |report| cleanup(report).into_shutdown())
                .await;

            drop(driver);
            report
        }))
    }

    #[doc(hidden)]
    pub fn claim_listener(&self) -> Result<(), AktorGroupError> {
        if self.control.stopping.get() {
            return Err(AktorGroupError::Stopped);
        }

        if self.control.listening.replace(true) {
            Err(AktorGroupError::ListenerTaken)
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

impl<Clock: AktorGroupClock> AktorKillSwitch<Clock> {
    #[doc(hidden)]
    pub fn record_diagnostic(&self, error: AktorError) {
        self.control.report.borrow_mut().application.push(error);
    }

    pub fn stop(&self) {
        if !self.control.stopping.replace(true) {
            self.control.report.borrow_mut().startup |= self.control.starting.get();
            #[cfg(feature = "embassy_cross_core")]
            self.control.shared_stopping.store(true, Ordering::Release);
            // Actors stop one at a time with two graces each, the last grace is for the
            // application and the shutdown closure.
            let actors = u32::try_from(self.control.kinds.borrow().len()).unwrap_or(u32::MAX);
            let budget = self
                .control
                .grace
                .saturating_mul(actors.saturating_mul(2).saturating_add(1));

            self.control.deadline.set(Some(Clock::deadline(budget)));

            if !self.control.listening.get() {
                let hook = self.control.shutdown_hook.borrow_mut().take();

                if let Some(error) = super::driver::contain(|| drop(hook)) {
                    self.control.report.borrow_mut().application.push(error);
                }

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

    /// Run work until it finishes or the group starts closing. If the group starts closing
    /// first, `run` drops the work and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        let mut stopping = core::pin::pin!(self.wait_stopping());
        let mut work = core::pin::pin!(work);

        poll_fn(|cx| {
            if stopping.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }

            work.as_mut().poll(cx).map(Some)
        })
        .await
    }

    #[doc(hidden)]
    pub fn set_starting(&self, starting: bool) -> bool {
        if self.is_stopping() || (starting && self.control.failure.borrow().is_some()) {
            return false;
        }

        self.control.starting.set(starting);
        true
    }

    #[doc(hidden)]
    pub fn has_startup_failure(&self) -> bool {
        self.startup_failure
            .as_ref()
            .is_some_and(|receipt| receipt.get())
    }

    #[doc(hidden)]
    pub fn fail_startup(&self, reason: AktorFailureReport) -> bool {
        self.record_failure(reason, true)
    }

    #[doc(hidden)]
    pub fn fail(&self, reason: AktorFailureReport) -> bool {
        self.record_failure(reason, false)
    }

    #[doc(hidden)]
    pub fn startup_error(
        &self,
        actor: String,
        kind: crate::AktorExecution,
        error: crate::AktorError,
    ) -> crate::AktorFailure {
        let primary = self.fail_startup(AktorFailureReport {
            actor,
            kind: Some(kind),
            phase: "setup".into(),
            message: error.diagnostics.clone(),
        });

        crate::AktorFailure {
            diagnostics: error.diagnostics,
            primary,
        }
    }

    fn record_failure(&self, mut reason: AktorFailureReport, startup: bool) -> bool {
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

        let first = self.control.failure.borrow().is_none();

        if first {
            if startup && let Some(receipt) = &self.startup_failure {
                receipt.set(true);
            }

            self.control.report.borrow_mut().startup |= startup;
            *self.control.failure.borrow_mut() = Some(reason);
        }

        self.stop();
        first
    }
}

impl<Clock: AktorGroupClock> AktorGroupCompletion<Clock> {
    /// Read the latest retirement status without waiting or cloning the shutdown report.
    pub fn retirement_status(&self) -> AktorGroupRetirement {
        match &*self.control.completed.borrow() {
            Some(report) => report.retirement_status(),
            None => AktorGroupRetirement::Pending,
        }
    }

    /// Read the retained report if shutdown has finished.
    pub fn try_report(&self) -> Option<AktorShutdownReport> {
        self.control.completed.borrow().clone()
    }

    pub async fn wait(&self) -> AktorShutdownReport {
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
impl<Clock: AktorGroupClock> core::future::IntoFuture for AktorGroupCompletion<Clock> {
    type Output = AktorShutdownReport;
    type IntoFuture = LocalFuture<'static, AktorShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, Clock: AktorGroupClock> core::future::IntoFuture for &'a AktorGroupCompletion<Clock> {
    type Output = AktorShutdownReport;
    type IntoFuture = LocalFuture<'a, AktorShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl<Clock: AktorGroupClock> Clone for AktorKillSwitch<Clock> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
        }
    }
}

impl<Clock: AktorGroupClock> Clone for AktorGroupCompletion<Clock> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}
