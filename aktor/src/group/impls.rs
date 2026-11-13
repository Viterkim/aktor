use super::shutdown::bounded;
use super::*;
#[cfg(not(target_family = "wasm"))]
use crate::AktorShutdownPolicy;
use crate::{AktorGroupError, AktorGroupRetirement, AktorRetirementError, AktorShutdownOutput};
use core::{future::poll_fn, pin::pin, task::Poll};
#[cfg(not(target_family = "wasm"))]
use er::*;

#[cfg(not(target_family = "wasm"))]
#[derive(Er)]
pub struct GroupThreadErr;

enum StopAction {
    WaitForListener,
    #[cfg(not(target_family = "wasm"))]
    DisposeHooks,
    PublishReport,
}

impl State {
    fn claim_listener(&mut self) -> Result<(), AktorGroupError> {
        if self.finished {
            return Err(AktorGroupError::Stopped);
        }

        if self.listening {
            return Err(AktorGroupError::ListenerTaken);
        }

        self.listening = true;
        Ok(())
    }

    fn check_registration(&self) -> Result<(), AktorGroupError> {
        if self.finished {
            return Err(AktorGroupError::Stopped);
        }
        if self.deadline.is_some() {
            return Err(AktorGroupError::Stopping);
        }
        if !self.listening {
            return Err(AktorGroupError::NotStarted);
        }

        Ok(())
    }

    fn stop_unstarted(&mut self) -> StopAction {
        self.finished = true;

        #[cfg(not(target_family = "wasm"))]
        if self.listener_hook.is_some() || self.shutdown_hook.is_some() {
            self.callback_running = true;
            return StopAction::DisposeHooks;
        }

        StopAction::PublishReport
    }
}

impl AktorGroup {
    #[cfg(not(target_family = "wasm"))]
    /// Run once after actor cleanup, before publishing the group's final report.
    #[doc(hidden)]
    pub fn on_shutdown<Cleanup, Output, Mode>(
        &self,
        cleanup: Cleanup,
    ) -> Result<(), AktorGroupError>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + Send + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: Send + 'static,
    {
        self.register_shutdown(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        }))
    }

    #[cfg(target_family = "wasm")]
    /// Run once after actor cleanup, before publishing the group's final report.
    #[doc(hidden)]
    pub fn on_shutdown<Cleanup, Output, Mode>(
        &self,
        cleanup: Cleanup,
    ) -> Result<(), AktorGroupError>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'static,
    {
        self.register_shutdown(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        }))
    }

    fn register_shutdown(&self, hook: ShutdownHook) -> Result<(), AktorGroupError> {
        let state = self.control.lock();
        #[cfg(not(target_family = "wasm"))]
        let mut state = state;
        #[cfg(not(target_family = "wasm"))]
        let registered = state.shutdown_hook.is_some();
        #[cfg(target_family = "wasm")]
        let registered = self.shutdown_hook.borrow().is_some();
        let error = if registered {
            Some(AktorGroupError::ShutdownTaken)
        } else if state.finished {
            Some(AktorGroupError::Stopped)
        } else if state.deadline.is_some() {
            Some(AktorGroupError::Stopping)
        } else {
            None
        };

        if let Some(error) = error {
            drop(state);
            super::shutdown::contain_drop(
                hook,
                &self.killswitch(),
                "rejected shutdown closure drop",
            );
            return Err(error);
        }

        #[cfg(not(target_family = "wasm"))]
        {
            state.shutdown_hook = Some(hook);
        }
        #[cfg(target_family = "wasm")]
        {
            *self.shutdown_hook.borrow_mut() = Some(hook);
        }
        Ok(())
    }

    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn register_owner(
        &mut self,
        name: String,
        kind: AktorExecution,
        start: Action,
        cancel: Action,
        give_up: Option<GiveUp>,
        confirm_later: Option<ConfirmLater>,
        outcome: CloseFuture,
    ) -> Result<(), AktorGroupError> {
        #[cfg(not(target_family = "wasm"))]
        let mut entries = self
            .actors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        #[cfg(target_family = "wasm")]
        let mut entries = self.actors.borrow_mut();
        let mut state = self.control.lock();

        state.check_registration()?;

        state.kinds.push((name.clone(), kind));
        entries.push(Entry {
            kind,
            name,
            start,
            cancel,
            give_up,
            confirm_later,
            outcome,
        });

        Ok(())
    }

    #[doc(hidden)]
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// How long each actor gets to stop, one at a time.
    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    #[doc(hidden)]
    pub fn with_grace(grace: Duration) -> Self {
        Self::new_group(
            grace,
            #[cfg(not(target_family = "wasm"))]
            AktorShutdownPolicy::KeepAlive,
        )
    }

    /// Choose the shutdown policy for a native group you start yourself.
    #[doc(hidden)]
    #[cfg(not(target_family = "wasm"))]
    pub fn with_policy(grace: Duration, policy: AktorShutdownPolicy) -> Self {
        Self::new_group(grace, policy)
    }

    fn new_group(
        grace: Duration,
        #[cfg(not(target_family = "wasm"))] shutdown_policy: AktorShutdownPolicy,
    ) -> Self {
        let control = Arc::new(Control {
            stopping: AtomicBool::new(false),
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            runtime: Mutex::new(None),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            standard: std::sync::atomic::AtomicBool::new(false),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            intervals: Mutex::new(None),
            state: Mutex::new(State::default()),
            changed: watch::channel(None).0,
            completed: watch::channel(None).0,
            grace,
            #[cfg(not(target_family = "wasm"))]
            shutdown_policy,
            application_changed: watch::channel(()).0,
            #[cfg(not(target_family = "wasm"))]
            wake: std::sync::Condvar::new(),
            #[cfg(not(target_family = "wasm"))]
            threads: tokio::sync::Notify::new(),
        });

        Self {
            stop_on_drop: false,
            control,
            startup_failure: None,
            #[cfg(not(target_family = "wasm"))]
            actors: Arc::new(Mutex::new(Vec::new())),
            #[cfg(target_family = "wasm")]
            actors: Rc::new(RefCell::new(Vec::new())),
            #[cfg(target_family = "wasm")]
            shutdown_hook: Rc::new(RefCell::new(None)),
            #[cfg(target_family = "wasm")]
            late: Rc::new(RefCell::new(None)),
        }
    }

    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn start_threaded(&mut self) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        self.start()
    }

    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    #[doc(hidden)]
    pub fn spawner(&self, standard: bool) -> std::io::Result<crate::executor::Spawner> {
        #[cfg(feature = "tokio")]
        if !standard
            && let Some(runtime) = &*self
                .control
                .runtime
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        {
            return Ok(crate::executor::Spawner::Tokio(runtime.clone()));
        }

        crate::executor::Spawner::current(standard)
    }

    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn start_standard(&mut self) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        let completed = self.completion();
        let closing = self.listen(async |_| Ok::<_, AktorError>(()), None)?;

        self.control
            .standard
            .store(true, std::sync::atomic::Ordering::Release);

        let late = self.killswitch();

        if let Err(error) = crate::executor::Spawner::Std.spawn_named("aktor group", async move {
            closing.await;
            late.finish_late().await;
        }) {
            return Err(self.killswitch().start_failed(error));
        }

        self.stop_on_drop = true;
        Ok(completed)
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), AktorGroupError> {
        self.control.lock().check_registration()
    }

    pub fn killswitch(&self) -> AktorKillSwitch {
        AktorKillSwitch {
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
        }
    }

    pub fn completion(&self) -> AktorGroupCompletion {
        AktorGroupCompletion {
            result: self.control.completed.subscribe(),
        }
    }

    #[doc(hidden)]
    pub fn track_startup(&mut self) {
        self.startup_failure = Some(Arc::new(AtomicBool::new(false)));
    }

    #[doc(hidden)]
    pub fn new_registration(&self) -> Self {
        Self {
            stop_on_drop: false,
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
            actors: self.actors.clone(),
            #[cfg(target_family = "wasm")]
            shutdown_hook: self.shutdown_hook.clone(),
            #[cfg(target_family = "wasm")]
            late: self.late.clone(),
        }
    }

    #[cfg(any(
        not(target_family = "wasm"),
        all(
            any(feature = "wasm_browser_workers", feature = "browser_local"),
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    /// Start shutdown handling with defaults.
    #[doc(hidden)]
    pub fn start(&mut self) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        #[cfg(target_family = "wasm")]
        {
            self.start_with(async |_| Ok::<_, String>(()))
        }
        #[cfg(all(
            any(feature = "tokio", feature = "wasm_browser_workers"),
            not(target_family = "wasm")
        ))]
        {
            self.start_native(None)
        }
        #[cfg(all(
            not(any(feature = "tokio", feature = "wasm_browser_workers")),
            not(target_family = "wasm")
        ))]
        {
            Err(AktorGroupError::NoDefaultRuntime.into())
        }
    }

    /// Start shutdown and wait for the report. Some actors may still be running when it returns.
    /// Use `completion().wait_retired()` to wait for those actors to stop.
    pub fn shutdown(&self) -> AktorGroupCompletion {
        self.killswitch().stop();
        self.completion()
    }

    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        not(target_family = "wasm")
    ))]
    /// Keep shutdown running after the calling runtime exits. Await the completion later.
    #[doc(hidden)]
    pub fn start_with<Cleanup, Output, Mode>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<AktorGroupCompletion, (AktorGroupError, String)>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + Send + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: Send + 'static,
    {
        self.start_native(Some(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        })))
    }

    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        not(target_family = "wasm")
    ))]
    fn start_native(
        &mut self,
        hook: Option<ShutdownHook>,
    ) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        tokio::runtime::Handle::try_current().map_err(|_| AktorGroupError::NoTokio)?;

        let completed = self.completion();
        let control = self.control.clone();
        let closing = self.listen(
            move |report| async move {
                let hook = control.lock().listener_hook.take();

                match hook {
                    Some(hook) => hook(report).await,
                    None => Ok(()),
                }
            },
            hook,
        )?;
        let late = self.killswitch();
        let (started, ready) = std::sync::mpsc::sync_channel(1);

        let launch = || {
            #[cfg(all(test, feature = "tokio"))]
            if crate::executor::FAIL_SPAWN.with(|setting| setting.get()) == Some("aktor group") {
                return Err(std::io::Error::other("injected scheduling failure"));
            }

            std::thread::Builder::new()
                .name("aktor group".into())
                .spawn(move || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => {
                            if started.send(Ok(runtime.handle().clone())).is_ok() {
                                runtime.block_on(async move {
                                    closing.await;
                                    late.finish_late().await;
                                });
                            }
                        }
                        Err(error) => {
                            let _sent = started.send(Err(error));
                        }
                    }
                })
        };
        let runtime = launch().and_then(|_| {
            ready.recv().unwrap_or_else(|_| {
                Err(std::io::Error::other(
                    "actor group thread stopped during startup",
                ))
            })
        });

        match runtime {
            Ok(runtime) => {
                #[cfg(feature = "tokio")]
                {
                    *self
                        .control
                        .runtime
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = Some(runtime);
                }
                #[cfg(not(feature = "tokio"))]
                let _ = runtime;

                self.stop_on_drop = true;
                Ok(completed)
            }
            Err(error) => Err(self.killswitch().start_failed(error)),
        }
    }

    #[cfg(all(
        any(feature = "wasm_browser_workers", feature = "browser_local"),
        target_family = "wasm",
        target_os = "unknown"
    ))]
    /// Start shutdown handling on the browser executor.
    #[doc(hidden)]
    pub fn start_with<Cleanup, Output, Mode>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<AktorGroupCompletion, (AktorGroupError, String)>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'static,
    {
        let completed = self.completion();
        let closing = self.listen(move |report| cleanup(report).into_shutdown())?;

        self.stop_on_drop = true;

        let late = self.late.clone();

        wasm_bindgen_futures::spawn_local(async move {
            closing.await;

            let collecting = late.borrow_mut().take();

            if let Some(collecting) = collecting {
                collecting.await;
            }
        });
        Ok(completed)
    }

    #[cfg(any(
        not(target_family = "wasm"),
        all(
            any(feature = "wasm_browser_workers", feature = "browser_local"),
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    fn listen<Cleanup, CleanupFuture>(
        &self,
        cleanup: Cleanup,
        #[cfg(not(target_family = "wasm"))] hook: Option<ShutdownHook>,
    ) -> Result<
        impl Future<Output = AktorShutdownReport> + use<Cleanup, CleanupFuture>,
        AktorGroupError,
    >
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorError>>,
    {
        {
            let mut state = self.control.lock();

            state.claim_listener()?;
            #[cfg(not(target_family = "wasm"))]
            {
                // A failed thread launch drops the listener future on the starting thread.
                state.listener_hook = hook;
            }
        }

        let group = self.new_registration();
        let kill = self.killswitch();

        let closing = async move {
            kill.wait_stopping().await;
            group.finish(cleanup).await
        };
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        let closing = super::intervals::drive(self.control.clone(), closing);

        Ok(closing)
    }

    #[doc(hidden)]
    pub fn claim_listener(&self) -> Result<(), AktorGroupError> {
        self.control.lock().claim_listener()
    }
}
impl Drop for AktorGroup {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.killswitch().stop();
        }
    }
}
#[doc(hidden)]
impl Default for AktorGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl AktorKillSwitch {
    pub fn stop(&self) {
        let (deadline, action) = {
            let mut state = self.control.lock();

            if state.deadline.is_some() || state.finished {
                return;
            }

            // Actors stop one at a time, a grace for their calls and one for `end`. The
            // last grace is for the application and the shutdown closure.
            let grace = self.control.grace;
            let actors = u32::try_from(state.kinds.len()).unwrap_or(u32::MAX);
            let budget = grace
                .saturating_mul(actors.saturating_mul(2).saturating_add(1))
                .saturating_add(reserve(grace).saturating_mul(actors));
            let deadline = shutdown_deadline(Instant::now(), budget);

            state.report.startup |= state.starting;
            state.deadline = Some(deadline);
            self.control.stopping.store(true, Ordering::Release);

            let action = if state.listening {
                StopAction::WaitForListener
            } else {
                state.stop_unstarted()
            };

            (deadline, action)
        };

        self.finish_stop(deadline, action, true);
    }

    #[cfg(not(target_family = "wasm"))]
    fn start_failed(&self, error: std::io::Error) -> (AktorGroupError, String) {
        let details = error.er(|| GroupThreadErr).er_report();
        let (deadline, action, first_stop, report) = {
            let mut state = self.control.lock();

            state.listening = false;
            state.report.startup |= state.starting;
            state.report.application.push(AktorError::new(details, ()));
            self.control.stopping.store(true, Ordering::Release);

            let first_stop = state.deadline.is_none();
            let deadline = *state
                .deadline
                .get_or_insert_with(|| shutdown_deadline(Instant::now(), self.control.grace));
            let action = state.stop_unstarted();

            (deadline, action, first_stop, state.report.clone())
        };

        self.finish_stop(deadline, action, first_stop);

        AktorGroupError::StartFailed {
            report: Box::new(report),
        }
        .into()
    }

    fn finish_stop(&self, deadline: Instant, action: StopAction, first_stop: bool) {
        if first_stop {
            #[cfg(not(target_family = "wasm"))]
            match action {
                StopAction::WaitForListener | StopAction::DisposeHooks => {
                    super::native::watchdog(self);
                }
                StopAction::PublishReport => {}
            }

            self.control.changed.send_replace(Some(deadline));
        }

        match action {
            StopAction::WaitForListener => {}
            #[cfg(not(target_family = "wasm"))]
            StopAction::DisposeHooks => {
                self.control.wake.notify_all();
                self.dispose_shutdown_hooks();
            }
            StopAction::PublishReport => self.publish_stopped(),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    fn dispose_shutdown_hooks(&self) {
        let kill = self.clone();
        let disposer = std::thread::Builder::new()
            .name("aktor shutdown closure drop".into())
            .spawn(move || kill.drop_shutdown_hooks());

        if let Err(error) = disposer {
            let abandoned = {
                let mut state = self.control.lock();
                state.report.application.push(AktorError::new(
                    format!(
                        "Could not start a thread to drop the cancelled shutdown closure: {error}"
                    ),
                    (),
                ));

                match state.watchdog_disposal {
                    WatchdogDisposal::Gone => {
                        state.abandon_shutdown_hooks();
                        true
                    }
                    WatchdogDisposal::Available | WatchdogDisposal::Requested => {
                        if self.control.shutdown_policy == AktorShutdownPolicy::KeepAlive {
                            state.watchdog_disposal = WatchdogDisposal::Requested;
                        }
                        false
                    }
                }
            };

            if abandoned {
                self.publish_stopped();
            } else {
                self.control.wake.notify_all();
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(super) fn drop_shutdown_hooks(&self) {
        let (listener, registered) = {
            let mut state = self.control.lock();
            (state.listener_hook.take(), state.shutdown_hook.take())
        };

        super::shutdown::contain_drop(listener, self, "cancelled shutdown closure drop");
        super::shutdown::contain_drop(registered, self, "cancelled shutdown closure drop");

        {
            let mut state = self.control.lock();
            state.callback_running = false;
            if state
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                state.report.callback_timed_out();
            }
        }

        self.publish_stopped();
    }

    pub(super) fn publish_stopped(&self) {
        let report = self.control.lock().report.clone();
        self.control.completed.send_replace(Some(Completed {
            report,
            retirement_final: true,
        }));

        #[cfg(not(target_family = "wasm"))]
        self.control.wake.notify_all();
    }

    /// The group's completion, the same as `group.completion()`.
    pub fn completion(&self) -> AktorGroupCompletion {
        AktorGroupCompletion {
            result: self.control.completed.subscribe(),
        }
    }

    /// Let your UI or other tasks know we're closing.
    pub async fn wait_stopping(&self) {
        stopped(&mut self.control.changed.subscribe()).await;
    }

    /// Run work until it finishes or the group starts closing. If the group starts closing
    /// first, `run` drops the work and returns `None`.
    ///
    /// ```rust,ignore
    /// let Some(response) = kill.run(handle(&handles, request)).await else {
    ///     return unavailable();
    /// };
    /// ```
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        let mut stopping = pin!(self.wait_stopping());
        let mut work = pin!(work);

        poll_fn(|cx| {
            if stopping.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }

            work.as_mut().poll(cx).map(Some)
        })
        .await
    }

    pub fn is_stopping(&self) -> bool {
        self.control.stopping.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn is_finished(&self) -> bool {
        self.control.lock().finished
    }

    #[doc(hidden)]
    pub fn set_starting(&self, starting: bool) -> bool {
        let mut state = self.control.lock();

        if state.deadline.is_some()
            || state.finished
            || (starting && state.report.failure.is_some())
        {
            return false;
        }

        state.starting = starting;
        true
    }

    #[doc(hidden)]
    pub fn has_startup_failure(&self) -> bool {
        self.startup_failure
            .as_ref()
            .is_some_and(|receipt| receipt.load(Ordering::Acquire))
    }

    #[doc(hidden)]
    pub fn fail_startup(&self, failure: AktorFailureReport) -> bool {
        self.record_failure(failure, true, |_| {})
    }

    #[doc(hidden)]
    pub fn fail(&self, failure: AktorFailureReport) -> bool {
        self.record_failure(failure, false, |_| {})
    }

    #[doc(hidden)]
    pub fn startup_error(
        &self,
        actor: String,
        kind: crate::AktorExecution,
        error: crate::AktorError,
    ) -> crate::AktorFailure {
        self.startup_error_with(actor, kind, error, |_, _| {})
    }

    pub(crate) fn startup_error_with(
        &self,
        actor: String,
        kind: crate::AktorExecution,
        error: crate::AktorError,
        before_stop: impl FnOnce(bool, &str),
    ) -> crate::AktorFailure {
        let diagnostics = error.diagnostics;
        let primary = self.record_failure(
            AktorFailureReport {
                actor,
                kind: Some(kind),
                phase: "setup".into(),
                message: diagnostics.clone(),
            },
            true,
            |primary| before_stop(primary, &diagnostics),
        );

        crate::AktorFailure {
            diagnostics,
            primary,
        }
    }

    fn record_failure(
        &self,
        mut failure: AktorFailureReport,
        startup: bool,
        before_stop: impl FnOnce(bool),
    ) -> bool {
        let first = {
            let mut state = self.control.lock();

            if state.finished {
                drop(state);
                before_stop(false);
                return false;
            }

            if failure.kind.is_none() {
                failure.kind = state.kind(&failure.actor);
            }

            if state.report.failure.is_none() {
                if startup && let Some(receipt) = &self.startup_failure {
                    receipt.store(true, Ordering::Release);
                }

                state.report.startup |= startup;
                state.report.failure = Some(failure);
                true
            } else {
                false
            }
        };

        before_stop(first);
        self.stop();
        first
    }

    #[doc(hidden)]
    pub fn deadline(&self) -> Instant {
        self.control.lock().deadline.unwrap_or_else(Instant::now)
    }

    /// Every actor is done, what's left only gets one more grace.
    #[doc(hidden)]
    pub fn shorten_deadline(&self) -> Instant {
        let shorter = shutdown_deadline(Instant::now(), self.control.grace);
        let deadline = {
            let mut state = self.control.lock();
            let deadline = state
                .deadline
                .map_or(shorter, |current| current.min(shorter));

            state.deadline = Some(deadline);
            deadline
        };

        #[cfg(not(target_family = "wasm"))]
        self.control.wake.notify_all();

        deadline
    }

    #[doc(hidden)]
    pub fn force_at(&self) -> Instant {
        let reserve = reserve(self.control.grace);
        let deadline = self.deadline();
        deadline.checked_sub(reserve).unwrap_or(deadline)
    }

    #[cfg(any(feature = "tokio", feature = "std_thread"))]
    #[doc(hidden)]
    pub async fn wait_for_force(&self) {
        self.wait_stopping().await;
        self.bounded(self.force_at(), core::future::pending::<()>())
            .await;
    }

    #[cfg(any(feature = "tokio", feature = "std_thread"))]
    #[doc(hidden)]
    pub async fn wait_for_force_on(&self, standard: bool) {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            self.wait_stopping().await;
            super::shutdown::bounded_standard(self.force_at(), core::future::pending::<()>()).await;
            return;
        }

        let _ = standard;

        self.wait_for_force().await;
    }

    /// Run an actor's `end` for at most one grace, on the clock its owner runs on.
    #[doc(hidden)]
    pub async fn end_within<F: Future>(&self, end: F, standard: bool) -> Option<F::Output> {
        let until = shutdown_deadline(Instant::now(), self.control.grace);

        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return super::shutdown::bounded_standard(until, end).await;
        }

        let _ = standard;
        bounded(until, end).await
    }

    /// Collect outcomes of actors shutdown gave up on, run after the group's closing future.
    #[cfg(not(target_family = "wasm"))]
    #[doc(hidden)]
    pub async fn finish_late(&self) {
        let late = self.control.lock().late.take();

        if let Some(late) = late {
            late.await;
        }
    }

    #[doc(hidden)]
    pub async fn bounded<F: Future>(&self, deadline: Instant, future: F) -> Option<F::Output> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if self
            .control
            .standard
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return super::shutdown::bounded_standard(deadline, future).await;
        }

        bounded(deadline, future).await
    }
}

impl State {
    pub fn kind(&self, name: &str) -> Option<AktorExecution> {
        let mut kinds = self
            .kinds
            .iter()
            .filter(|(actor, _)| actor == name)
            .map(|(_, kind)| *kind);

        let first = kinds.next()?;

        kinds.all(|kind| kind == first).then_some(first)
    }
}

impl Control {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        #[cfg(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        ))]
        return self.state.guard();

        #[cfg(not(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        )))]
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

impl AktorGroupCompletion {
    /// Read the latest retirement status without waiting or cloning the shutdown report.
    pub fn retirement_status(&self) -> AktorGroupRetirement {
        let value = self.result.borrow();
        #[cfg(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        ))]
        let closed = value.is_closed();
        #[cfg(not(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        )))]
        let closed = self.result.has_changed().is_err();

        match &*value {
            Some(completed) => completed.report.retirement_status(),
            None if closed => AktorGroupRetirement::ReportLost,
            None => AktorGroupRetirement::Pending,
        }
    }

    /// Read the retained report if shutdown has finished.
    pub fn try_report(&self) -> Option<AktorShutdownReport> {
        self.result
            .borrow()
            .as_ref()
            .map(|completed| completed.report.clone())
    }

    /// Wait until every actor has stopped, including ones that shutdown left running.
    /// `Ok` gives you the shutdown report. An actor can stop with a failed or
    /// interrupted `end`, so check cleanup before reusing its resources.
    /// `Err((error, report))` means Aktor lost confirmation or no actor reported.
    /// Keep resources reserved in that case.
    ///
    /// An actor that shutdown leaves running may never finish. Keep this off anything
    /// that has to move on.
    pub async fn wait_retired(
        &self,
    ) -> Result<AktorShutdownReport, (AktorRetirementError, String)> {
        let mut result = self.result.clone();

        let report = loop {
            if let Some(completed) = result.borrow_and_update().as_ref() {
                let report = &completed.report;
                if report.retired() {
                    return Ok(report.clone());
                }

                if report.actors.is_empty() || completed.retirement_final {
                    break Some(report.clone());
                }
            }

            if result.changed().await.is_err() {
                break result
                    .borrow()
                    .as_ref()
                    .map(|completed| completed.report.clone());
            }
        };

        let text = report.as_ref().map(ToString::to_string).unwrap_or_default();
        let error = AktorRetirementError {
            report: report.map(Box::new),
        };
        let text = if text.is_empty() {
            error.to_string()
        } else {
            text
        };

        Err((error, text))
    }

    /// Wait for the shutdown report. If the group disappears before publishing one,
    /// `group_error` is [`ReportLost`](crate::AktorGroupShutdownError::ReportLost).
    pub async fn wait(&self) -> AktorShutdownReport {
        let mut result = self.result.clone();

        loop {
            if let Some(completed) = result.borrow_and_update().as_ref() {
                return completed.report.clone();
            }

            if result.changed().await.is_err() {
                return AktorShutdownReport {
                    group_error: Some(crate::AktorGroupShutdownError::ReportLost),
                    ..AktorShutdownReport::default()
                };
            }
        }
    }
}
impl core::future::IntoFuture for AktorGroupCompletion {
    type Output = AktorShutdownReport;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<Box<dyn Future<Output = AktorShutdownReport> + Send>>;
    #[cfg(target_family = "wasm")]
    type IntoFuture = Pin<Box<dyn Future<Output = AktorShutdownReport>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a> core::future::IntoFuture for &'a AktorGroupCompletion {
    type Output = AktorShutdownReport;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<Box<dyn Future<Output = AktorShutdownReport> + Send + 'a>>;
    #[cfg(target_family = "wasm")]
    type IntoFuture = Pin<Box<dyn Future<Output = AktorShutdownReport> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

pub async fn stopped(stopping: &mut watch::Receiver<Option<Instant>>) {
    loop {
        if stopping.borrow_and_update().is_some() {
            return;
        }

        if stopping.changed().await.is_err() {
            return;
        }
    }
}

/// How long a cancelled actor gets to notice before its `stuck` decides.
pub fn reserve(grace: Duration) -> Duration {
    (grace / 10).min(Duration::from_millis(100))
}

pub fn shutdown_deadline(now: Instant, grace: Duration) -> Instant {
    if let Some(deadline) = now.checked_add(grace) {
        return deadline;
    }

    let mut deadline = now;
    let mut low = 0;
    let mut high = grace.as_nanos();

    while low < high {
        let nanos = low + (high - low).div_ceil(2);
        let duration = Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        );

        if let Some(candidate) = now.checked_add(duration) {
            deadline = candidate;
            low = nanos;
        } else {
            high = nanos - 1;
        }
    }

    deadline
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retirement() {
        use crate::AktorRetireStatus::{Confirmed, Terminated, Unconfirmed};

        let completion = AktorGroup::new().completion();

        let (_, report) = completion.wait_retired().await.unwrap_err();
        assert_eq!(report, "actor retirement could not be confirmed");

        for (retirement, confirmed) in [
            (None, false),
            (Some(Unconfirmed), false),
            (Some(Confirmed), true),
            (Some(Terminated), true),
        ] {
            let report = AktorShutdownReport {
                actors: retirement
                    .map(|retirement| AktorOutcome {
                        actor: "cleanup".into(),
                        kind: AktorExecution::TokioThread,
                        retirement,
                        diagnostics: vec![AktorError::new("cleanup failed", ())],
                        ran_past_grace: false,
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            };
            let (sender, result) = watch::channel(Some(Completed {
                report,
                retirement_final: false,
            }));
            drop(sender);

            let completion = AktorGroupCompletion { result };

            assert_eq!(completion.wait_retired().await.is_ok(), confirmed);
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn lost_retirement() {
        fn outcome() -> CloseFuture {
            Box::pin(async {
                AktorOutcome {
                    actor: "owner".into(),
                    kind: AktorExecution::TokioThread,
                    retirement: crate::AktorRetireStatus::Unconfirmed,
                    diagnostics: Vec::new(),
                    ran_past_grace: false,
                }
            })
        }

        for confirm in [None, Some(false), Some(true)] {
            let mut group = AktorGroup::new();
            group.start_threaded().unwrap();

            let release = if confirm != Some(true) {
                let (release, waiting) = tokio::sync::oneshot::channel();
                group
                    .register_owner(
                        "waiting".into(),
                        AktorExecution::TokioThread,
                        Box::new(|| {}),
                        Box::new(|| {}),
                        None,
                        Some(Box::pin(async move {
                            waiting
                                .await
                                .unwrap_or(false)
                                .then(|| (crate::AktorRetireStatus::Confirmed, Vec::new()))
                        })),
                        outcome(),
                    )
                    .unwrap();
                Some(release)
            } else {
                None
            };

            let confirming = confirm.map(|confirmed| {
                Box::pin(async move {
                    confirmed.then(|| (crate::AktorRetireStatus::Confirmed, Vec::new()))
                }) as ConfirmLater
            });
            group
                .register_owner(
                    "owner".into(),
                    AktorExecution::TokioThread,
                    Box::new(|| {}),
                    Box::new(|| {}),
                    None,
                    confirming,
                    outcome(),
                )
                .unwrap();

            let completion = group.shutdown();
            let result = tokio::time::timeout(Duration::from_secs(2), completion.wait_retired())
                .await
                .expect("retirement never finished");
            assert_eq!(result.is_ok(), confirm == Some(true));

            if let Some(release) = release {
                let _ = release.send(true);
            }
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn late_termination() {
        let mut group = AktorGroup::new();
        group.start_threaded().unwrap();
        let (retired, waiting) = tokio::sync::oneshot::channel();

        group
            .register_owner(
                "worker".into(),
                AktorExecution::BrowserWebWorker,
                Box::new(|| {}),
                Box::new(|| {}),
                None,
                Some(Box::pin(async move {
                    waiting
                        .await
                        .ok()
                        .map(|diagnostics| (crate::AktorRetireStatus::Terminated, diagnostics))
                })),
                Box::pin(async {
                    AktorOutcome {
                        actor: "worker".into(),
                        kind: AktorExecution::BrowserWebWorker,
                        retirement: crate::AktorRetireStatus::Unconfirmed,
                        diagnostics: Vec::new(),
                        ran_past_grace: false,
                    }
                }),
            )
            .unwrap();

        let closing = group.shutdown();
        closing.wait().await;
        retired.send(Vec::new()).unwrap();

        let report = closing.wait_retired().await.unwrap();
        assert_eq!(
            report.actors[0].retirement,
            crate::AktorRetireStatus::Terminated
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn terminated_stays_retired() {
        let mut group = AktorGroup::with_grace(Duration::from_millis(10));
        group.start_threaded().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();

        group
            .register_owner(
                "worker".into(),
                AktorExecution::TokioThread,
                Box::new(|| {}),
                Box::new(|| {}),
                Some(Box::new(|| true)),
                None,
                Box::pin(async move {
                    let _ = receive.await;
                    AktorOutcome {
                        actor: "worker".into(),
                        kind: AktorExecution::TokioThread,
                        retirement: crate::AktorRetireStatus::Unconfirmed,
                        diagnostics: vec![AktorError::new("late outcome", ())],
                        ran_past_grace: false,
                    }
                }),
            )
            .unwrap();

        let completion = group.shutdown();
        completion.wait_retired().await.unwrap();
        let mut updates = completion.result.clone();
        let _ = updates.borrow_and_update();
        send.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                updates.changed().await.unwrap();
                let update = updates.borrow_and_update();
                let report = &update.as_ref().unwrap().report;
                if !report.actors[0].diagnostics.is_empty() {
                    assert_eq!(
                        report.actors[0].retirement,
                        crate::AktorRetireStatus::Terminated
                    );
                    assert!(report.actors[0].ran_past_grace);
                    break;
                }
            }
        })
        .await
        .unwrap();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn thread_failure() {
        type StartGroup =
            fn(&mut AktorGroup) -> Result<AktorGroupCompletion, (AktorGroupError, String)>;

        let starters: &[StartGroup] = &[
            AktorGroup::start_threaded,
            #[cfg(feature = "std_thread")]
            AktorGroup::start_standard,
        ];

        for start in starters {
            let mut group = AktorGroup::new();
            let completed = group.completion();

            crate::executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor group")));
            let result = start(&mut group);
            crate::executor::FAIL_SPAWN.with(|setting| setting.set(None));

            let Err((AktorGroupError::StartFailed { report }, printed)) = result else {
                panic!("thread failure didn't fail the start");
            };

            assert!(report.application[0].diagnostics.contains("injected"));
            assert!(printed.contains("injected"));
            assert!(completed.wait().await.failed());
        }
    }

    #[test]
    fn large_grace() {
        let now = Instant::now();

        for grace in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(5),
        ] {
            assert_eq!(
                shutdown_deadline(now, grace),
                now.checked_add(grace).unwrap()
            );
        }

        let last = shutdown_deadline(now, Duration::MAX);

        assert_eq!(shutdown_deadline(now, last.duration_since(now)), last);
        assert!(last.checked_add(Duration::from_nanos(1)).is_none());

        let group = AktorGroup::with_grace(Duration::MAX);

        group.killswitch().stop();
        assert!(group.killswitch().deadline() > Instant::now() + Duration::from_secs(5));
    }
}
