use super::shutdown::bounded;
use super::*;
#[cfg(not(target_family = "wasm"))]
use crate::AktorGroupStuck;
use crate::{AktorGroupError, AktorRetirementError, AktorShutdownOutput};
use core::task::{Context, Poll};
use er::*;

#[cfg(not(target_family = "wasm"))]
#[derive(Er)]
#[er(format = "could not start a thread to drop the cancelled shutdown closure")]
struct SpawnDisposerErr;
#[cfg(not(target_family = "wasm"))]
fn spawn_disposer(kill: AktorKillSwitch) -> ErResult<(), SpawnDisposerErr> {
    std::thread::Builder::new()
        .name("aktor shutdown closure drop".into())
        .spawn(move || kill.drop_shutdown_hooks())
        .map(drop)
        .er(())
}

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
    fn new_actor(&self) -> crate::ActorId {
        crate::ActorId::next()
    }

    fn claim_listener(&mut self) -> ErResult<(), AktorGroupError> {
        if self.finished {
            er_bail!(AktorGroupError::Stopped);
        }

        if self.listening {
            er_bail!(AktorGroupError::ListenerTaken);
        }

        self.listening = true;
        Ok(())
    }

    pub(super) fn check_registration(&self) -> ErResult<(), AktorGroupError> {
        if self.finished {
            er_bail!(AktorGroupError::Stopped);
        }
        if self.deadline.is_some() {
            er_bail!(AktorGroupError::Stopping);
        }
        if !self.listening {
            er_bail!(AktorGroupError::NotStarted);
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
    ) -> Result<(), (AktorGroupError, String)>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + Send + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: Send + 'static,
    {
        Ok(self.register_shutdown(super::shared::hook(cleanup))?)
    }

    #[cfg(target_family = "wasm")]
    /// Run once after actor cleanup, before publishing the group's final report.
    #[doc(hidden)]
    pub fn on_shutdown<Cleanup, Output, Mode>(
        &self,
        cleanup: Cleanup,
    ) -> Result<(), (AktorGroupError, String)>
    where
        Cleanup: FnOnce(AktorShutdownReport) -> Output + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'static,
    {
        Ok(self.register_shutdown(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        }))?)
    }

    fn register_shutdown(&self, hook: ShutdownHook) -> ErResult<(), AktorGroupError> {
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
                crate::AktorFailureSource::ShutdownCallback,
            );
            er_bail!(error);
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
    ) -> Result<(), (AktorGroupError, String)> {
        #[cfg(not(target_family = "wasm"))]
        let mut entries = self
            .actors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        #[cfg(target_family = "wasm")]
        let mut entries = self.actors.borrow_mut();
        let mut state = self.control.lock();

        state.check_registration()?;

        let id = self
            .killswitch()
            .actor_id()
            .unwrap_or_else(|| state.new_actor());
        state
            .report
            .attempt(id, &name, kind, crate::AktorInitEvidence::Unknown)
            .owner = Some(crate::AktorOwnerOutcome {
            retirement: crate::AktorRetirement::StillRunning,
            ran_past_grace: false,
        });
        state.registered += 1;

        entries.push(Entry {
            id,
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
            AktorGroupStuck::KeepRunning,
        )
    }

    pub(crate) fn new_group(
        grace: Duration,
        #[cfg(not(target_family = "wasm"))] shutdown_policy: AktorGroupStuck,
    ) -> Self {
        let control = Arc::new(Control {
            stopping: AtomicBool::new(false),
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            runtime: Mutex::new(None),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            standard: std::sync::atomic::AtomicBool::new(false),
            state: Mutex::new(State {
                kept: Some(Vec::new()),
                ..State::default()
            }),
            changed: watch::channel(None).0,
            completed: watch::channel(None).0,
            grace,
            #[cfg(not(target_family = "wasm"))]
            shutdown_policy,
            application_changed: watch::channel(()).0,
            #[cfg(not(target_family = "wasm"))]
            threads: tokio::sync::Notify::new(),
        });

        Self {
            stop_on_drop: false,
            control,
            startup_failure: Vec::new(),
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
        self.start_native(None)
    }

    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    #[doc(hidden)]
    pub fn spawner(
        &self,
        standard: bool,
    ) -> Result<crate::executor::Spawner, (AktorGroupError, String)> {
        #[cfg(feature = "tokio")]
        if !standard {
            // Checked and set under one lock, so two racing spawns can't start two runtimes.
            #[cfg_attr(not(feature = "std_thread"), allow(unused_mut))]
            let mut runtime = self
                .control
                .runtime
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            if let Some(runtime) = &*runtime {
                return Ok(crate::executor::Spawner::Tokio(runtime.clone()));
            }

            // A Tokio actor joining a standard group would borrow the caller's runtime, and its
            // supervisor would die with it. The group gets a runtime of its own instead.
            #[cfg(feature = "std_thread")]
            if self
                .control
                .standard
                .load(std::sync::atomic::Ordering::Acquire)
            {
                let started = super::shared::runtime().er(|| AktorGroupError::NoDefaultRuntime)?;

                *runtime = Some(started.clone());
                return Ok(crate::executor::Spawner::Tokio(started));
            }
        }

        Ok(crate::executor::Spawner::current(standard).er(|| AktorGroupError::NoTokio)?)
    }

    /// The group runs on standard threads, `start_standard` started it.
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn is_standard(&self) -> bool {
        self.control
            .standard
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn start_standard(&mut self) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        let completed = self.completion();
        let closing = self.listen(async |_| Ok::<_, (AktorCleanupError, String)>(()), None)?;

        self.control
            .standard
            .store(true, std::sync::atomic::Ordering::Release);

        let late = self.killswitch();

        if let Err(error) = crate::executor::Spawner::Std
            .spawn_named("aktor group", async move {
                let _ = closing.await;
                late.finish_late().await;
            })
            .er(|| GroupThreadErr)
        {
            return Err(self.killswitch().start_failed(error));
        }

        self.stop_on_drop = true;
        Ok(completed)
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), (AktorGroupError, String)> {
        Ok(self.control.lock().check_registration()?)
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

    /// Hold a handle until the group is done, so the actor lives as long as its group. A handle
    /// that isn't `Send` waits in a [`keeper`] next to its actor instead.
    #[cfg(any(
        all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ),
        feature = "bevy"
    ))]
    pub(crate) fn keep(&self, handle: impl Send + 'static) {
        let handle: Box<dyn core::any::Any + Send> = Box::new(handle);
        let mut state = self.control.lock();

        if let Some(kept) = &mut state.kept {
            kept.push(handle);
        } else {
            drop(state);
            drop(handle);
        }
    }

    #[doc(hidden)]
    #[cfg(any(target_family = "wasm", feature = "tokio", feature = "std_thread"))]
    pub(crate) fn startup_receipt(&self) -> crate::AktorStartup {
        self.control
            .lock()
            .report
            .startup(self.startup_failure.first().map(|entry| entry.0))
    }

    #[doc(hidden)]
    pub fn track_startup(&mut self) {
        self.startup_failure.clear();
        self.track_member();
    }

    #[cfg(any(target_family = "wasm", feature = "tokio", feature = "std_thread"))]
    pub(crate) fn fallback_member(&self, index: Option<usize>) -> Self {
        let mut group = self.new_registration();
        let parent = self.startup_failure.last().map(|entry| entry.0);
        match parent
            .zip(index)
            .and_then(|(id, index)| self.control.lock().report.child(id, index))
        {
            Some(id) => group
                .startup_failure
                .push((id, Arc::new(std::sync::OnceLock::new()))),
            None => group.startup_failure.clear(),
        }
        group
    }

    pub(crate) fn track_member(&mut self) {
        let mut state = self.control.lock();
        let id = state.new_actor();
        let root = self.startup_failure.first().map_or(id, |entry| entry.0);
        state.report.scopes.push(crate::types::LaunchScope {
            root,
            parent: self.startup_failure.last().map(|entry| entry.0),
            id,
        });
        self.startup_failure
            .push((id, Arc::new(std::sync::OnceLock::new())));
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
            self.start_with(async |_| ())
        }
        #[cfg(all(
            any(feature = "tokio", feature = "wasm_browser_workers"),
            not(target_family = "wasm")
        ))]
        {
            tokio::runtime::Handle::try_current().er(|| AktorGroupError::NoTokio)?;
            self.start_native(None)
        }
        #[cfg(all(
            not(any(feature = "tokio", feature = "wasm_browser_workers")),
            not(target_family = "wasm")
        ))]
        {
            er_bail!(AktorGroupError::NoDefaultRuntime)
        }
    }

    /// Start shutdown now. The group keeps collecting cleanup results after either wait is dropped.
    pub fn stop(&self) -> impl Future<Output = AktorShutdownReport> + Send + 'static {
        self.control.lock().hook_at_grace = true;
        self.killswitch().stop();
        let completion = self.completion();
        let control = self.control.clone();

        async move {
            completion
                .wait()
                .await
                .unwrap_or_else(|_| control.lock().report.snapshot())
        }
    }

    pub fn stop_released(
        &self,
    ) -> impl Future<Output = Result<crate::AktorRetired, (AktorRetirementError, String)>> + Send + 'static
    {
        self.killswitch().stop_internal(StopCause::Requested, true);
        let completion = self.completion();

        async move { completion.wait_retired().await }
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
        tokio::runtime::Handle::try_current().er(|| AktorGroupError::NoTokio)?;
        self.start_native(Some(super::shared::hook(cleanup)))
    }

    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        not(target_family = "wasm")
    ))]
    fn start_native(
        &mut self,
        hook: Option<ShutdownHook>,
    ) -> Result<AktorGroupCompletion, (AktorGroupError, String)> {
        let completed = self.completion();
        let kill = self.killswitch();
        let closing = self.listen(
            move |report| async move {
                let hook = kill.control.lock().listener_hook.take();

                match hook {
                    Some(hook) => hook(report, kill).await,
                    None => Ok(()),
                }
            },
            hook,
        )?;
        let late = self.killswitch();

        // Shutdown keeps going on the shared aktor thread after the caller's runtime is gone.
        let runtime: ErResult<_, GroupThreadErr> = (|| {
            #[cfg(all(test, feature = "tokio"))]
            if crate::executor::FAIL_SPAWN.with(|setting| setting.get()) == Some("aktor group") {
                er_bail!(GroupThreadErr, "injected scheduling failure");
            }

            let runtime = super::shared::runtime().er(())?;

            runtime.spawn(async move {
                let _ = closing.await;
                late.finish_late().await;
            });
            Ok(runtime)
        })();

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
            let _ = closing.await;

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
    ) -> ErResult<
        impl Future<Output = AktorShutdownReport> + use<Cleanup, CleanupFuture>,
        AktorGroupError,
    >
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
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
        Ok(closing)
    }

    #[doc(hidden)]
    pub fn claim_listener(&self) -> Result<(), (AktorGroupError, String)> {
        Ok(self.control.lock().claim_listener()?)
    }
}
impl Drop for AktorGroup {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.killswitch().stop();
        }
    }
}
/// Holds a handle that can't sit in the `Send` group until the group's report is out. Spawn it
/// on the actor's own executor.
#[cfg(any(
    all(
        feature = "local",
        any(
            all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
            all(
                feature = "browser_local",
                target_family = "wasm",
                target_os = "unknown"
            )
        )
    ),
    all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ),
    all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_os = "unknown",
        target_feature = "atomics"
    )
))]
pub(crate) fn keeper<Handle: 'static>(
    handle: Handle,
    completion: AktorGroupCompletion,
) -> crate::message::LocalFuture<'static, ()> {
    Box::pin(async move {
        let _kept = handle;
        let _ = completion.wait().await;
    })
}

impl AktorKillSwitch {
    pub(crate) fn registered_id(&self) -> Option<crate::ActorId> {
        let id = self.actor_id()?;
        self.control
            .lock()
            .report
            .actors
            .iter()
            .any(|actor| actor.id == id)
            .then_some(id)
    }

    pub(crate) fn actor_id(&self) -> Option<crate::ActorId> {
        self.startup_failure.last().map(|(id, _)| *id)
    }

    #[cfg(any(
        all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ),
        all(
            any(feature = "browser_local", feature = "wasm_browser_workers"),
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn validate_attempt(&self, name: &str, kind: crate::AktorExecution) {
        if let Some(id) = self.actor_id() {
            self.control.lock().report.attempt(
                id,
                name,
                kind,
                crate::AktorInitEvidence::NeverEntered,
            );
        }
    }

    pub(crate) fn init_evidence(&self, init: crate::AktorInitEvidence) {
        if let Some(id) = self.actor_id()
            && let Some(attempt) = self.control.lock().report.actor_mut(id)
        {
            attempt.init = attempt.init.observed(init);
        }
    }
    pub fn stop(&self) {
        self.stop_internal(StopCause::Requested, false);
    }

    /// Stop the group cleanly on behalf of an actor retiring itself, without marking startup as
    /// externally aborted.
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "bevy"
    ))]
    pub(crate) fn retire_self(&self) {
        if let Some((actor, _)) = self.startup_failure.last() {
            self.stop_internal(StopCause::Retired(*actor), false);
        } else {
            self.stop();
        }
    }

    /// Stopped from outside or failed while a `start` was still running. An actor retiring itself
    /// from its own `start` doesn't count.
    #[cfg(any(
        all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ),
        all(
            any(feature = "browser_local", feature = "wasm_browser_workers"),
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn is_startup_aborted(&self) -> bool {
        let state = self.control.lock();
        matches!(
            state.report.stop_cause,
            Some(StopCause::Requested | StopCause::Failure(_) | StopCause::StartupFailure(_))
        )
    }

    fn stop_internal(&self, cause: StopCause, hook_at_grace: bool) {
        let (deadline, action) = {
            let mut state = self.control.lock();

            let Some(stop) = self.begin_stop(&mut state, cause) else {
                return;
            };
            state.hook_at_grace |= hook_at_grace;
            stop
        };

        self.finish_stop(deadline, action, true);
    }

    fn begin_stop(&self, state: &mut State, cause: StopCause) -> Option<(Instant, StopAction)> {
        if state.deadline.is_some() || state.finished {
            return None;
        }

        state.report.stop_cause = Some(cause);

        // Actors stop one at a time, a grace for their calls and one for `end`. The
        // last grace is for the application and the shutdown closure.
        let grace = self.control.grace;
        let actors =
            u32::try_from(state.registered.saturating_add(state.applications)).unwrap_or(u32::MAX);
        let budget = grace
            .saturating_mul(actors.saturating_mul(2).saturating_add(1))
            .saturating_add(reserve(grace).saturating_mul(actors));
        let deadline = shutdown_deadline(Instant::now(), budget);

        state.deadline = Some(deadline);
        self.control.stopping.store(true, Ordering::Release);

        let action = if state.listening {
            StopAction::WaitForListener
        } else {
            state.stop_unstarted()
        };

        Some((deadline, action))
    }

    #[cfg(not(target_family = "wasm"))]
    fn start_failed(&self, error: ErTree<GroupThreadErr>) -> (AktorGroupError, String) {
        let details = error.er_report();
        let (deadline, action, first_stop, report) = {
            let mut state = self.control.lock();

            state.listening = false;
            let id = state.report.record(AktorFailureInput::new(
                crate::AktorFailureSource::GroupDriver,
                details,
            ));
            state
                .report
                .stop_cause
                .get_or_insert(StopCause::StartupFailure(id));
            self.mark_startup(id);
            self.control.stopping.store(true, Ordering::Release);

            let first_stop = state.deadline.is_none();
            let deadline = *state
                .deadline
                .get_or_insert_with(|| shutdown_deadline(Instant::now(), self.control.grace));
            let action = state.stop_unstarted();

            (deadline, action, first_stop, state.report.snapshot())
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
                super::native::poke();
                self.dispose_shutdown_hooks();
            }
            StopAction::PublishReport => self.publish_stopped(),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    fn dispose_shutdown_hooks(&self) {
        if let Err(error) = spawn_disposer(self.clone()) {
            let abandoned = {
                let mut state = self.control.lock();
                state.report.record(AktorFailureInput::new(
                    crate::AktorFailureSource::GroupDriver,
                    error.er_report(),
                ));

                match state.watchdog_disposal {
                    WatchdogDisposal::Gone => {
                        state.abandon_shutdown_hooks();
                        true
                    }
                    WatchdogDisposal::Available | WatchdogDisposal::Requested => {
                        if self.control.shutdown_policy == AktorGroupStuck::KeepRunning {
                            state.watchdog_disposal = WatchdogDisposal::Requested;
                        }
                        false
                    }
                }
            };

            if abandoned {
                self.publish_stopped();
            } else {
                super::native::poke();
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub(super) fn drop_shutdown_hooks(&self) {
        let (listener, registered) = {
            let mut state = self.control.lock();
            (state.listener_hook.take(), state.shutdown_hook.take())
        };

        super::shutdown::contain_drop(listener, self, crate::AktorFailureSource::ShutdownCallback);
        super::shutdown::contain_drop(
            registered,
            self,
            crate::AktorFailureSource::ShutdownCallback,
        );

        {
            let mut state = self.control.lock();
            state.callback_running = false;
            if state
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                state.report.late(crate::AktorLateWork::ShutdownCallback);
            }
        }

        self.publish_stopped();
    }

    pub(super) fn publish_stopped(&self) {
        self.publish(Collection::Complete, true);

        #[cfg(not(target_family = "wasm"))]
        super::native::poke();
    }

    // Read the recorder while updating the watch so simultaneous publishers can't send older snapshots.
    pub(super) fn publish(&self, collection: Collection, work_finished: bool) {
        self.control.completed.send_if_modified(|snapshot| {
            let report = Arc::new(self.control.lock().report.snapshot());
            let grace = snapshot
                .as_ref()
                .map_or_else(|| report.clone(), |previous| previous.grace.clone());
            *snapshot = Some(Completed {
                grace,
                report,
                collection,
                work_finished,
            });
            true
        });
    }

    #[cfg(any(
        feature = "local",
        feature = "transport",
        all(target_family = "wasm", feature = "wasm_browser_workers")
    ))]
    pub(crate) fn delivery(&self, id: FailureId) -> Option<crate::Delivery> {
        self.control
            .lock()
            .report
            .failure(id)
            .map(|failure| failure.delivery.clone())
    }

    fn refresh_failures(&self) {
        self.control.completed.send_if_modified(|snapshot| {
            let Some(snapshot) = snapshot else {
                return false;
            };
            let state = self.control.lock();
            let latest = Arc::make_mut(&mut snapshot.report);
            latest.failures.clone_from(&state.report.failures);
            latest.actors.clone_from(&state.report.actors);
            true
        });
    }

    /// The group's completion, the same as `group.completion()`.
    pub fn completion(&self) -> AktorGroupCompletion {
        AktorGroupCompletion {
            result: self.control.completed.subscribe(),
        }
    }

    /// Let your UI or other tasks know we're closing.
    pub fn wait_stopping(&self) -> AktorGroupStopping {
        let mut changed = self.control.changed.subscribe();

        AktorGroupStopping(Box::pin(async move { stopped(&mut changed).await }))
    }

    /// Run work until it finishes or the group starts closing. If the group starts closing
    /// first, `run` drops the work and returns `None`.
    ///
    /// ```rust,ignore
    /// let Some(response) = kill.run(handle(&handles, request)).await else {
    ///     return unavailable();
    /// };
    /// ```
    pub fn run<F: IntoFuture>(&self, work: F) -> AktorRun<F::IntoFuture> {
        let work = work.into_future();

        let stopping = if self.is_stopping() {
            RunStopping::Stopped
        } else {
            RunStopping::Waiting(self.wait_stopping())
        };

        AktorRun { stopping, work }
    }

    pub fn is_stopping(&self) -> bool {
        self.control.stopping.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn is_finished(&self) -> bool {
        self.control.lock().finished
    }

    #[doc(hidden)]
    pub fn has_startup_failure(&self) -> bool {
        self.startup_id().is_some_and(|id| {
            self.control.lock().report.stop_cause == Some(StopCause::StartupFailure(id))
        })
    }

    pub(crate) fn startup_id(&self) -> Option<FailureId> {
        let (_, receipt) = self.startup_failure.last()?;
        receipt.get().copied()
    }

    fn mark_startup(&self, id: FailureId) {
        for (_, receipt) in &self.startup_failure {
            let _ = receipt.set(id);
        }
    }

    #[doc(hidden)]
    pub fn fail_startup(&self, failure: AktorFailureInput) -> FailureId {
        self.record_failure(failure, true, |_| {}).0
    }

    #[doc(hidden)]
    pub fn fail(&self, failure: AktorFailureInput) -> FailureId {
        self.record_failure(failure, false, |_| {}).0
    }

    #[cfg(all(
        feature = "local",
        any(
            all(not(target_family = "wasm"), any(feature = "tokio", feature = "bevy")),
            all(
                feature = "browser_local",
                target_family = "wasm",
                target_os = "unknown"
            )
        )
    ))]
    pub(crate) fn startup_error(
        &self,
        actor: String,
        kind: AktorExecution,
        message: String,
    ) -> FailureId {
        self.startup_error_with(actor, kind, message, |_| {})
    }

    #[cfg(any(
        all(
            feature = "local",
            any(
                all(not(target_family = "wasm"), any(feature = "tokio", feature = "bevy")),
                all(feature = "browser_local", target_family = "wasm")
            )
        ),
        all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        )
    ))]
    pub(crate) fn startup_error_with(
        &self,
        actor: String,
        kind: AktorExecution,
        message: String,
        before_stop: impl FnOnce(FailureId),
    ) -> FailureId {
        self.record_failure(
            AktorFailureInput::actor(actor, kind, crate::AktorPhase::Start, message),
            true,
            before_stop,
        )
        .0
    }

    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    pub(crate) fn stop_startup(&self, id: FailureId) {
        let stop = {
            let mut state = self.control.lock();
            self.mark_startup(id);
            self.begin_stop(&mut state, StopCause::StartupFailure(id))
        };
        if let Some((deadline, action)) = stop {
            self.finish_stop(deadline, action, true);
        }
    }

    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    pub(crate) fn observe_failure(&self, failure: AktorFailureInput) -> FailureId {
        let id = self
            .control
            .lock()
            .report
            .record_for(self.actor_id(), failure)
            .0;
        self.refresh_failures();
        id
    }

    pub(crate) fn record_failure(
        &self,
        failure: AktorFailureInput,
        startup: bool,
        before_stop: impl FnOnce(FailureId),
    ) -> crate::Delivery {
        self.record_loss(failure, startup, None, before_stop)
    }

    pub(crate) fn record_loss(
        &self,
        failure: AktorFailureInput,
        startup: bool,
        cause: Option<&std::sync::OnceLock<crate::Delivery>>,
        before_stop: impl FnOnce(FailureId),
    ) -> crate::Delivery {
        let (receipt, stop) = {
            let mut state = self.control.lock();
            let receipt = state.report.record_for(self.actor_id(), failure);
            if let Some(cause) = cause {
                let _ = cause.set(receipt.clone());
            }
            let id = receipt.0;
            if startup {
                self.mark_startup(id);
            }
            let cause = if startup {
                StopCause::StartupFailure(id)
            } else {
                StopCause::Failure(id)
            };
            (receipt, self.begin_stop(&mut state, cause))
        };

        self.refresh_failures();
        before_stop(receipt.0);
        if let Some((deadline, action)) = stop {
            self.finish_stop(deadline, action, true);
        }
        receipt
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
        super::native::poke();

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
    /// True once shutdown has published its first report, even if an actor is still running.
    pub fn has_report(&self) -> bool {
        self.result.borrow().is_some()
    }

    /// Clone the latest shutdown report, if one has been published.
    pub fn try_report(&self) -> Option<AktorShutdownReport> {
        self.result
            .borrow()
            .as_ref()
            .map(|completed| (*completed.report).clone())
    }

    pub fn retired(&self) -> crate::AktorRetiredStatus {
        use crate::AktorRetiredStatus;
        let closed = self.result.has_changed().is_err();
        let result = self.result.borrow();

        if let Some(completed) = result.as_ref() {
            if completed.work_finished && completed.collection == Collection::Complete {
                return AktorRetiredStatus::from_report(&completed.report);
            }

            if completed.work_finished && completed.collection == Collection::Lost || closed {
                return AktorRetiredStatus::Unconfirmed(Some((*completed.report).clone()));
            }
        } else if closed {
            return AktorRetiredStatus::Unconfirmed(None);
        }

        AktorRetiredStatus::Pending
    }

    pub async fn wait_retired(
        &self,
    ) -> Result<crate::AktorRetired, (AktorRetirementError, String)> {
        let mut result = self.result.clone();

        loop {
            result.borrow_and_update();
            if let Some(outcome) = self.retired().into_result() {
                return outcome;
            }
            let _ = result.changed().await;
        }
    }

    /// Wait for publication. A closed supervisor can disappear before publishing anything.
    pub async fn wait(&self) -> Result<AktorShutdownReport, crate::AktorPublicationLost> {
        let mut result = self.result.clone();

        loop {
            if let Some(completed) = result.borrow_and_update().as_ref() {
                return Ok((*completed.grace).clone());
            }
            if result.changed().await.is_err() {
                return result
                    .borrow()
                    .as_ref()
                    .map(|completed| (*completed.grace).clone())
                    .ok_or(crate::AktorPublicationLost);
            }
        }
    }
}
impl core::future::IntoFuture for AktorGroupCompletion {
    type Output = Result<AktorShutdownReport, crate::AktorPublicationLost>;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<
        Box<dyn Future<Output = Result<AktorShutdownReport, crate::AktorPublicationLost>> + Send>,
    >;
    #[cfg(target_family = "wasm")]
    type IntoFuture =
        Pin<Box<dyn Future<Output = Result<AktorShutdownReport, crate::AktorPublicationLost>>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a> core::future::IntoFuture for &'a AktorGroupCompletion {
    type Output = Result<AktorShutdownReport, crate::AktorPublicationLost>;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<
        Box<
            dyn Future<Output = Result<AktorShutdownReport, crate::AktorPublicationLost>>
                + Send
                + 'a,
        >,
    >;
    #[cfg(target_family = "wasm")]
    type IntoFuture = Pin<
        Box<dyn Future<Output = Result<AktorShutdownReport, crate::AktorPublicationLost>> + 'a>,
    >;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<F> AktorRun<F> {
    pub(crate) fn ungrouped(work: F) -> Self {
        Self {
            stopping: RunStopping::None,
            work,
        }
    }
}
impl<F: Future> Future for AktorRun<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.stopping {
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm"))
            ))]
            RunStopping::None => {}
            RunStopping::Stopped => return Poll::Ready(None),
            RunStopping::Waiting(stopping) => {
                if Pin::new(stopping).poll(cx).is_ready() {
                    *this.stopping = RunStopping::Stopped;
                    return Poll::Ready(None);
                }
            }
        }

        this.work.poll(cx).map(Some)
    }
}

impl Future for AktorGroupStopping {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        self.0.as_mut().poll(cx)
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
    #[cfg(feature = "tokio")]
    use crate::AktorRetirement;

    #[tokio::test]
    async fn retirement() {
        use crate::AktorRetirement::{Released, StillRunning, Terminated};

        let completion = AktorGroup::new().completion();

        let (error, _) = completion.wait_retired().await.unwrap_err();
        assert!(error.report.is_none());

        for (retirement, confirmed) in [
            (None, false),
            (Some(StillRunning), false),
            (Some(Released), true),
            (Some(Terminated), true),
        ] {
            let report = AktorShutdownReport {
                actors: retirement
                    .map(|retirement| AktorOutcome {
                        id: crate::ActorId(0),
                        name: "cleanup".into(),
                        kind: AktorExecution::TokioThread,
                        owner: Some(crate::AktorOwnerOutcome {
                            retirement,
                            ran_past_grace: false,
                        }),
                        init: crate::AktorInitEvidence::Unknown,
                    })
                    .into_iter()
                    .collect(),
                stop_cause: StopCause::Requested,
                failures: Vec::new(),
                late_work: Vec::new(),
            };
            let report = Arc::new(report);
            let (sender, result) = watch::channel(Some(Completed {
                grace: report.clone(),
                report,
                collection: Collection::Complete,
                work_finished: true,
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
                OwnerOutcome {
                    retirement: AktorRetirement::StillRunning,
                    init: crate::AktorInitEvidence::Unknown,
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
                                .then_some(AktorRetirement::Released)
                        })),
                        outcome(),
                    )
                    .unwrap();
                Some(release)
            } else {
                None
            };

            let confirming = confirm.map(|confirmed| {
                Box::pin(async move { confirmed.then_some(AktorRetirement::Released) })
                    as ConfirmLater
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

            let _stopped = group.stop().await;
            let result = tokio::time::timeout(Duration::from_secs(2), group.stop_released())
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
    async fn lost_outcome() {
        let mut group = AktorGroup::with_grace(Duration::from_millis(10));
        group.start_threaded().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();

        group
            .register_owner(
                "owner".into(),
                AktorExecution::TokioThread,
                Box::new(|| panic!("shutdown failed")),
                Box::new(|| {}),
                Some(Box::new(|| true)),
                None,
                Box::pin(async move {
                    let _ = receive.await;
                    panic!("shutdown failed");
                }),
            )
            .unwrap();

        let _stopped = group.stop().await;
        send.send(()).unwrap();
        let (error, _) = tokio::time::timeout(Duration::from_secs(2), group.stop_released())
            .await
            .unwrap()
            .unwrap_err();
        let report = error.report.unwrap();
        assert_eq!(
            report
                .failures
                .iter()
                .filter(|row| row.source == crate::AktorFailureSource::GroupDriver)
                .count(),
            2
        );
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
                    waiting.await.ok().map(|()| AktorRetirement::Terminated)
                })),
                Box::pin(async {
                    OwnerOutcome {
                        retirement: AktorRetirement::StillRunning,
                        init: crate::AktorInitEvidence::Unknown,
                        ran_past_grace: false,
                    }
                }),
            )
            .unwrap();

        let _stopped = group.stop().await;
        retired.send(()).unwrap();

        let report = group.stop_released().await.unwrap().into_report();
        assert_eq!(
            report.actors[0].retirement(),
            Some(AktorRetirement::Terminated)
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn terminated_stays_retired() {
        let mut group = AktorGroup::with_grace(Duration::from_millis(10));
        group.start_threaded().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();

        let (terminated, observed) = tokio::sync::oneshot::channel();
        group
            .register_owner(
                "worker".into(),
                AktorExecution::TokioThread,
                Box::new(|| {}),
                Box::new(|| {}),
                Some(Box::new(move || {
                    let _ = terminated.send(());
                    true
                })),
                None,
                Box::pin(async move {
                    let _ = receive.await;
                    OwnerOutcome {
                        retirement: AktorRetirement::StillRunning,
                        init: crate::AktorInitEvidence::Ready,
                        ran_past_grace: false,
                    }
                }),
            )
            .unwrap();

        let _snapshot = group.stop();
        observed.await.unwrap();
        send.send(()).unwrap();

        let retired = group.stop_released().await.unwrap().into_report();
        assert_eq!(retired.retirement(), Some(AktorRetirement::Terminated));
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

            assert!(report.failures[0].error.contains("injected"));
            assert!(printed.contains("injected"));
            assert!(completed.wait().await.unwrap().failed());
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
