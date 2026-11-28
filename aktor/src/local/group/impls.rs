use super::driver::poll_owners;
use super::*;
use crate::{AktorRetirementError, report::ShutdownCut};

use crate::{AktorGroupError, AktorRetirement, AktorShutdownOutput};

impl<Clock: AktorGroupClock> Control<Clock> {
    pub(super) fn publish(&self) -> &AktorShutdownReport {
        self.published
            .get_or_init(|| self.report.borrow().snapshot())
    }
}

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    #[doc(hidden)]
    pub fn register_owner(
        &mut self,
        name: String,
        kind: crate::AktorExecution,
        shutdown: Box<dyn Fn()>,
        cancel: Option<Box<dyn Fn()>>,
        cut_call: Option<Rc<Cell<bool>>>,
        owner: LocalFuture<'static, Option<String>>,
    ) -> Result<(), AktorStateError> {
        let mut group = self.new_registration();
        if group.killswitch().actor_id().is_none() {
            group.track_member();
        }

        let kill = group.killswitch();
        let label = name.clone();
        match group.register_observed_owner(
            name,
            kind,
            shutdown,
            cancel,
            Box::new(move || {
                let mut status = OwnerStatus {
                    cleanup_completed: None,
                    ran_past_grace: false,
                    init: crate::AktorInitEvidence::Unknown,
                };
                if cut_call.as_ref().is_some_and(|cut| cut.replace(false)) {
                    status.ran_past_grace = true;
                    kill.fail(AktorFailureInput::actor(
                        label.clone(),
                        kind,
                        crate::AktorPhase::Runner,
                        er_new!(ShutdownCut).er_report(),
                    ));
                }

                status
            }),
            owner,
        ) {
            Ok(()) => Ok(()),
            Err((AktorGroupError::NotStarted, _)) => Err(AktorStateError::NotStarted),
            Err(_) => Err(AktorStateError::Closed),
        }
    }

    fn new_actor(&self) -> crate::ActorId {
        crate::ActorId::next()
    }

    fn register_attempt(&self, name: &str, kind: crate::AktorExecution) -> crate::ActorId {
        let id = self
            .killswitch()
            .actor_id()
            .unwrap_or_else(|| self.new_actor());
        self.control
            .owners_count
            .set(self.control.owners_count.get().saturating_add(1));
        self.control
            .report
            .borrow_mut()
            .attempt(id, name, kind, crate::AktorInitEvidence::Unknown)
            .owner = Some(crate::AktorOwnerOutcome {
            retirement: AktorRetirement::StillRunning,
            ran_past_grace: false,
        });
        id
    }

    pub(crate) fn register_observed_owner(
        &mut self,
        name: String,
        kind: crate::AktorExecution,
        shutdown: Box<dyn Fn()>,
        cancel: Option<Box<dyn Fn()>>,
        status: Box<dyn Fn() -> OwnerStatus>,
        owner: LocalFuture<'static, Option<String>>,
    ) -> Result<(), (AktorGroupError, String)> {
        self.check_started()?;

        let id = self.register_attempt(&name, kind);
        self.owners.borrow_mut().push(Entry {
            id,
            status,
            name,
            kind,
            shutdown,
            cancel,
            owner,
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
            stop_on_drop: true,
            spawner: None,
            rejected: None,
            startup_failure: Vec::new(),
            control: Rc::new(Control {
                stopping: Cell::new(false),
                #[cfg(feature = "embassy_cross_core")]
                shared_stopping: Shared::new(AtomicBool::new(false)),
                listening: Cell::new(false),
                driver_loss: Cell::new(None),
                spawner: RefCell::new(None),
                final_hook: RefCell::new(None),
                shutdown_hook: RefCell::new(None),
                callers: RefCell::new(Vec::new()),
                grace,
                deadline: Cell::new(None),
                owners_count: Cell::new(0),
                applications: Cell::new(0),
                published: core::cell::OnceCell::new(),
                report: RefCell::new(crate::Recorder::default()),
                changed: Event::default(),
            }),
            owners: Rc::new(RefCell::new(Vec::new())),
        }
    }

    #[doc(hidden)]
    pub(crate) fn startup_receipt(&self) -> crate::AktorStartup {
        self.control
            .report
            .borrow()
            .startup(self.startup_failure.first().map(|entry| entry.0))
    }

    #[doc(hidden)]
    pub fn track_startup(&mut self) {
        self.startup_failure.clear();
        self.track_member();
    }

    pub(crate) fn fallback_member(&self, index: Option<usize>) -> Self {
        let mut group = self.new_registration();
        let parent = self.startup_failure.last().map(|entry| entry.0);
        match parent
            .zip(index)
            .and_then(|(id, index)| self.control.report.borrow().child(id, index))
        {
            Some(id) => group.startup_failure.push((id, Rc::new(Cell::new(None)))),
            None => group.startup_failure.clear(),
        }
        group
    }

    pub(crate) fn track_member(&mut self) {
        let id = self.new_actor();
        let root = self.startup_failure.first().map_or(id, |entry| entry.0);
        self.control
            .report
            .borrow_mut()
            .scopes
            .push(crate::types::LaunchScope {
                root,
                parent: self.startup_failure.last().map(|entry| entry.0),
                id,
            });
        self.startup_failure.push((id, Rc::new(Cell::new(None))));
    }

    #[doc(hidden)]
    pub fn new_registration(&self) -> Self {
        Self {
            stop_on_drop: false,
            spawner: None,
            rejected: self.rejected.clone(),
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
            owners: self.owners.clone(),
        }
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), (AktorGroupError, String)> {
        if self.control.published.get().is_some() {
            er_bail!(AktorGroupError::Stopped);
        }
        if self.control.stopping.get() {
            er_bail!(AktorGroupError::Stopping);
        }
        if !self.control.listening.get() {
            er_bail!(AktorGroupError::NotStarted);
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
            rejected: self.rejected.clone(),
            control: self.control.clone(),
        }
    }

    #[doc(hidden)]
    /// Your hook gets the report after actor cleanup, before completion is published.
    pub fn on_shutdown<Hook, Output, Mode>(
        &self,
        hook: Hook,
    ) -> Result<(), (AktorGroupError, String)>
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
            super::driver::contain(
                || drop(hook),
                |dropped| {
                    self.killswitch().fail(AktorFailureInput::new(
                        crate::AktorFailureSource::ShutdownCallback,
                        dropped,
                    ));
                },
            );

            er_bail!(error);
        }

        *self.control.shutdown_hook.borrow_mut() = Some(Box::new(move |report| {
            Box::pin(async move { hook(report).into_shutdown().await })
        }));
        Ok(())
    }

    /// Run dependent application work on this group's local driver.
    pub fn spawn_task<F, O, E>(
        &self,
        future: F,
        policy: crate::AktorJob,
    ) -> Result<(), (crate::AktorGroupError, String)>
    where
        F: Future<Output = O> + 'static,
        O: crate::AktorJobOutput<E>,
    {
        let refused = if self.killswitch().is_stopping() {
            Some(crate::AktorGroupError::Stopping)
        } else if !self.control.listening.get() {
            Some(crate::AktorGroupError::NotStarted)
        } else {
            None
        };

        if let Some(reason) = refused {
            super::driver::contain(
                || drop(future),
                |error| {
                    self.killswitch().fail(AktorFailureInput::new(
                        crate::AktorFailureSource::ApplicationTask,
                        error,
                    ));
                },
            );
            er_bail!(reason);
        }

        self.control
            .applications
            .set(self.control.applications.get().saturating_add(1));
        self.control.callers.borrow_mut().push(Application {
            after: self.control.owners_count.get() as usize,
            policy,
            future: Box::pin(async move { future.await.into_job().map(drop) }),
        });
        self.control.changed.notify();
        Ok(())
    }

    /// Start shutdown now and wait for the group's report.
    pub fn stop(&self) -> impl Future<Output = AktorShutdownReport> + 'static {
        self.killswitch().stop();
        let completion = self.completion();
        let control = self.control.clone();

        async move {
            completion
                .wait()
                .await
                .unwrap_or_else(|_| control.report.borrow().snapshot())
        }
    }

    pub fn stop_released(
        &self,
    ) -> impl Future<Output = Result<crate::AktorRetired, (AktorRetirementError, String)>> + 'static
    {
        self.killswitch().stop();
        let completion = self.completion();

        async move { completion.wait_retired().await }
    }

    pub(crate) fn spawn_driver(
        &mut self,
        spawn: Rc<DriverSpawner>,
    ) -> Result<(), (crate::AktorStartReason, String)> {
        use crate::AktorStartReason;

        let driver = self
            .listen(async |_| ())
            .er_with(|err| err.start_reason())?;
        *self.control.spawner.borrow_mut() = Some(Rc::downgrade(&spawn));
        let retained = Rc::new(RefCell::new(Some(driver)));
        let submitted = retained.clone();

        let result = spawn(Box::pin(poll_fn(move |cx| {
            let driver = submitted.borrow_mut().take();
            let Some(mut driver) = driver else {
                return Poll::Ready(());
            };

            if driver.as_mut().poll(cx).is_ready() {
                Poll::Ready(())
            } else {
                *submitted.borrow_mut() = Some(driver);
                Poll::Pending
            }
        })));

        if let Err(report) = result {
            self.rejected = Some(retained);
            let error = er_new!(AktorStartReason::InvalidSetup.er_save("spawner", report));
            self.killswitch().fail_startup(AktorFailureInput::new(
                crate::AktorFailureSource::GroupDriver,
                error.er_report(),
            ));
            return Err(error.into());
        }

        // The driver can borrow the spawner without keeping its own executor alive.
        self.spawner = Some(spawn);
        Ok(())
    }

    #[doc(hidden)]
    /// A handle whose actor never launched, calls on it find it closed.
    pub(crate) fn closed_handle<S: 'static>(&self, name: &str) -> Handle<S, (), Clock> {
        let (handle, owner) = closed_channel::<S, Clock>();

        *handle.inner.group.borrow_mut() = Some((
            name.into(),
            crate::AktorExecution::Local,
            self.killswitch().into(),
        ));
        drop(owner);
        handle
    }

    pub(crate) fn spawn_with_custom<S: 'static, E: 'static, Runner, SetupOutput, CleanupOutput>(
        &mut self,
        args: AktorArgs<
            impl AsyncFnOnce() -> SetupOutput + 'static,
            impl AsyncFnOnce(S) -> CleanupOutput + 'static,
        >,
        hooks: hooks::CallHooks<S>,
        kind: crate::AktorExecution,
        runner: Runner,
    ) -> Result<Handle<S, (), Clock>, AktorStateError>
    where
        SetupOutput: crate::local::impls::runner::LocalSetupOutput<S, E>,
        CleanupOutput: crate::local::impls::runner::LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, E>) -> Result<(), (RunnerError, String)>
            + 'static,
    {
        if self.killswitch().is_stopping() {
            return Err(AktorStateError::Closed);
        }

        if !self.control.listening.get() {
            return Err(AktorStateError::NotStarted);
        }

        let (handle, mut owner) = channel_with_clock::<S, E, Clock>(args.capacity);

        owner.hooks = hooks;
        *handle.inner.group.borrow_mut() =
            Some((args.name.as_str().into(), kind, self.killswitch().into()));

        let shutdown = handle.clone();
        let cancel = handle.clone();

        let id = self.register_attempt(&args.name, kind);
        let completion = owner.inner.completion.clone();
        self.owners.borrow_mut().push(Entry {
            id,
            status: Box::new(move || OwnerStatus {
                cleanup_completed: Some(completion.cleanup_completed.get()),
                ran_past_grace: completion.cut_call.get() || completion.ran_out.get(),
                init: completion.init.get(),
            }),
            name: args.name,
            kind,
            shutdown: Box::new(move || {
                shutdown.shutdown();
            }),
            cancel: Some(Box::new(move || {
                cancel.cancel_calls();
            })),
            owner: Box::pin(async move {
                let _ = owner
                    .run_prepared(
                        async move || (args.setup)().await,
                        async move |state| (args.cleanup)(state).await,
                        runner,
                    )
                    .await;
                None
            }),
            finished: false,
        });

        self.control.changed.notify();
        Ok(handle)
    }

    /// Move this future onto your local executor to drive the actors.
    /// Dropping it cancels the actors and publishes a failed report.
    pub fn listen<'a, Cleanup, Output, Mode>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<LocalFuture<'a, AktorShutdownReport>, (AktorGroupError, String)>
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
            spawner: None,
            rejected: self.rejected.clone(),
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
    pub fn claim_listener(&self) -> Result<(), (AktorGroupError, String)> {
        if self.control.stopping.get() {
            er_bail!(AktorGroupError::Stopped);
        }

        if self.control.listening.replace(true) {
            er_bail!(AktorGroupError::ListenerTaken)
        } else {
            Ok(())
        }
    }
}
impl<Clock: AktorGroupClock> Drop for AktorGroup<Clock> {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.killswitch().stop();
        }
    }
}

impl<Clock: AktorGroupClock> AktorKillSwitch<Clock> {
    pub(crate) fn registered_id(&self) -> Option<crate::ActorId> {
        let id = self.actor_id()?;
        self.control
            .report
            .borrow()
            .actors
            .iter()
            .any(|actor| actor.id == id)
            .then_some(id)
    }

    pub(crate) fn actor_id(&self) -> Option<crate::ActorId> {
        self.startup_failure.last().map(|(id, _)| *id)
    }

    pub(crate) fn validate_attempt(&self, name: &str, kind: crate::AktorExecution) {
        if let Some(id) = self.actor_id() {
            self.control.report.borrow_mut().attempt(
                id,
                name,
                kind,
                crate::AktorInitEvidence::NeverEntered,
            );
        }
    }

    pub(crate) fn init_evidence(&self, init: crate::AktorInitEvidence) {
        if let Some(id) = self.actor_id()
            && let Some(attempt) = self.control.report.borrow_mut().actor_mut(id)
        {
            attempt.init = attempt.init.observed(init);
        }
    }
    #[doc(hidden)]
    pub fn record_diagnostic(&self, error: String) {
        self.control
            .report
            .borrow_mut()
            .record(AktorFailureInput::new(
                crate::AktorFailureSource::ApplicationTask,
                error,
            ));
    }

    pub fn stop(&self) {
        self.stop_with(StopCause::Requested);
    }

    fn stop_with(&self, cause: StopCause) {
        if !self.control.stopping.replace(true) {
            self.control.report.borrow_mut().stop_cause = Some(cause);
            #[cfg(feature = "embassy_cross_core")]
            self.control.shared_stopping.store(true, Ordering::Release);
            // Actors stop one at a time with two graces each, the last grace is for the
            // application and the shutdown closure.
            let actors = self
                .control
                .owners_count
                .get()
                .saturating_add(self.control.applications.get());
            let budget = self
                .control
                .grace
                .saturating_mul(actors.saturating_mul(2).saturating_add(1));

            self.control.deadline.set(Some(Clock::deadline(budget)));

            if !self.control.listening.get() {
                let hook = self.control.shutdown_hook.borrow_mut().take();

                super::driver::contain(
                    || drop(hook),
                    |error| {
                        self.control
                            .report
                            .borrow_mut()
                            .record(AktorFailureInput::new(
                                crate::AktorFailureSource::ShutdownCallback,
                                error,
                            ));
                    },
                );

                self.control.publish();
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

    #[doc(hidden)]
    pub fn wait_grace(&self) -> LocalFuture<'static, ()> {
        Clock::wait(Clock::deadline(self.control.grace))
    }

    /// Run an actor's `end` for at most one grace, counted from when it starts.
    #[doc(hidden)]
    pub async fn end_within<F: Future>(&self, end: F) -> Option<F::Output> {
        crate::local::signal::within(end, self.wait_grace()).await
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
    pub async fn run<F: IntoFuture>(&self, work: F) -> Option<F::Output> {
        let work = work.into_future();

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
    /// Stopped or failed while a `start` was still running.
    pub(crate) fn is_startup_aborted(&self) -> bool {
        self.is_stopping()
    }

    #[doc(hidden)]
    pub fn has_startup_failure(&self) -> bool {
        self.startup_id().is_some_and(|id| {
            self.control.report.borrow().stop_cause == Some(StopCause::StartupFailure(id))
        })
    }

    pub(crate) fn delivery(&self, id: FailureId) -> Option<crate::Delivery> {
        self.control
            .report
            .borrow()
            .failure(id)
            .map(|failure| failure.delivery.clone())
    }

    pub(crate) fn driver_failure(&self) -> Option<FailureId> {
        self.control.driver_loss.get()
    }

    pub(crate) fn driver_loss<E>(&self) -> Option<crate::setup::StartFailure<E>> {
        let id = self.control.driver_loss.get()?;
        let report = self.control.report.borrow();
        let failure = report.failure(id)?;
        let mut error = crate::LaunchError::new(crate::AktorStartReason::Stopping);
        error.failure_id = Some(id);
        Some((error, failure.error.clone()))
    }

    pub(crate) fn startup_id(&self) -> Option<FailureId> {
        self.startup_failure
            .last()
            .and_then(|(_, receipt)| receipt.get())
            .or(self.control.driver_loss.get())
    }

    #[doc(hidden)]
    pub fn fail_startup(&self, reason: AktorFailureInput) -> FailureId {
        self.record_failure(reason, true)
    }

    #[doc(hidden)]
    pub fn fail(&self, reason: AktorFailureInput) -> FailureId {
        self.record_failure(reason, false)
    }

    #[doc(hidden)]
    pub fn startup_error(
        &self,
        actor: String,
        kind: crate::AktorExecution,
        message: String,
    ) -> FailureId {
        self.fail_startup(AktorFailureInput::actor(
            actor,
            kind,
            crate::AktorPhase::Start,
            message,
        ))
    }

    pub(super) fn owner_failure(&self, actor: crate::ActorId, failure: AktorFailureInput) {
        let id = self
            .control
            .report
            .borrow_mut()
            .record_for(Some(actor), failure)
            .0;
        self.stop_with(StopCause::Failure(id));
    }

    fn record_failure(&self, failure: AktorFailureInput, startup: bool) -> FailureId {
        let id = self
            .control
            .report
            .borrow_mut()
            .record_for(self.actor_id(), failure)
            .0;
        if startup && self.control.driver_loss.get().is_none() {
            for (_, receipt) in &self.startup_failure {
                if receipt.get().is_none() {
                    receipt.set(Some(id));
                }
            }
        }

        self.stop_with(if startup {
            StopCause::StartupFailure(id)
        } else {
            StopCause::Failure(id)
        });
        id
    }
}

impl<Clock: AktorGroupClock> AktorGroupCompletion<Clock> {
    /// True once shutdown has published its report, even if an actor is still running.
    pub fn has_report(&self) -> bool {
        self.control.published.get().is_some()
    }

    /// Read the retained report if shutdown has finished.
    pub fn try_report(&self) -> Option<AktorShutdownReport> {
        self.has_report()
            .then(|| self.control.report.borrow().snapshot())
    }

    pub fn retired(&self) -> crate::AktorRetiredStatus {
        self.try_report().as_ref().map_or(
            crate::AktorRetiredStatus::Pending,
            crate::AktorRetiredStatus::from_report,
        )
    }

    pub async fn wait_retired(
        &self,
    ) -> Result<crate::AktorRetired, (AktorRetirementError, String)> {
        let _ = self.wait().await;
        self.retired()
            .into_result()
            .expect("local driver published")
    }

    pub async fn wait(&self) -> Result<AktorShutdownReport, crate::AktorPublicationLost> {
        let changed = self.control.changed.listen();

        poll_fn(|cx| {
            changed.register(cx);

            if let Some(rejected) = &self.rejected {
                let driver = rejected.borrow_mut().take();
                if let Some(mut driver) = driver
                    && driver.as_mut().poll(cx).is_pending()
                {
                    *rejected.borrow_mut() = Some(driver);
                }
            }

            super::driver::poll_final_hook(&self.control, cx);
            match self.control.published.get() {
                Some(report) => Poll::Ready(Ok(report.clone())),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl<Clock: AktorGroupClock> core::future::IntoFuture for AktorGroupCompletion<Clock> {
    type Output = Result<AktorShutdownReport, crate::AktorPublicationLost>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, Clock: AktorGroupClock> core::future::IntoFuture for &'a AktorGroupCompletion<Clock> {
    type Output = Result<AktorShutdownReport, crate::AktorPublicationLost>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

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
            rejected: self.rejected.clone(),
            control: self.control.clone(),
        }
    }
}
