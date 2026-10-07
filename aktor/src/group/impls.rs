use super::shutdown::bounded;
use super::*;
use crate::AktorShutdownOutput;

impl AktorGroup {
    #[cfg(not(target_family = "wasm"))]
    /// Run once after actor cleanup, before publishing the group's final report.
    pub fn on_shutdown<Cleanup, Output, Mode>(&self, cleanup: Cleanup) -> Result<(), AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> Output + Send + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: Send + 'static,
    {
        self.register_shutdown(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        }))
    }

    #[cfg(target_family = "wasm")]
    /// Run once after actor cleanup, before publishing the group's final report.
    pub fn on_shutdown<Cleanup, Output, Mode>(&self, cleanup: Cleanup) -> Result<(), AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> Output + 'static,
        Output: AktorShutdownOutput<Mode>,
        Output::Future: 'static,
    {
        self.register_shutdown(Box::new(move |report| {
            Box::pin(async move { cleanup(report).into_shutdown().await })
        }))
    }

    fn register_shutdown(&self, hook: ShutdownHook) -> Result<(), AktorError> {
        let state = self.control.lock();
        #[cfg(not(target_family = "wasm"))]
        let mut state = state;
        #[cfg(not(target_family = "wasm"))]
        let registered = state.shutdown_hook.is_some();
        #[cfg(target_family = "wasm")]
        let registered = self.shutdown_hook.borrow().is_some();
        let error = if registered {
            Some("this group already has a shutdown closure")
        } else if state.deadline.is_some() || state.finished {
            Some("actor group is closing")
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
            return Err(AktorError::new(error));
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
    pub fn register_owner(
        &mut self,
        name: String,
        kind: AktorExecution,
        start: Action,
        cancel: Action,
        outcome: CloseFuture,
    ) -> Result<(), AktorError> {
        #[cfg(not(target_family = "wasm"))]
        let mut entries = self
            .actors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        #[cfg(target_family = "wasm")]
        let mut entries = self.actors.borrow_mut();
        let mut state = self.control.lock();

        if !state.listening {
            return Err(AktorError::new(
                "start the actor group before spawning actors",
            ));
        }

        if state.deadline.is_some() {
            return Err(AktorError::new("actor group is closing"));
        }

        state.kinds.push((name.clone(), kind));
        entries.push(Entry {
            kind,
            name,
            start,
            cancel,
            outcome,
        });

        Ok(())
    }

    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// How long shutdown gets, including your last closure.
    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    pub fn with_grace(grace: Duration) -> Self {
        let control = Arc::new(Control {
            stopping: AtomicBool::new(false),
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            runtime: Mutex::new(None),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            standard: std::sync::atomic::AtomicBool::new(false),
            state: Mutex::new(State::default()),
            changed: watch::channel(None).0,
            completed: watch::channel(None).0,
            grace,
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
        }
    }

    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn start_threaded(&mut self) -> Result<GroupCompletion, AktorError> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| AktorError::new("starting an actor group needs a Tokio runtime"))?;

        let completed = self.completion();
        let closing = self.listen(async |_| Ok::<_, AktorCleanupError>(()))?;
        let (started, ready) = std::sync::mpsc::sync_channel(1);

        let launch = || {
            #[cfg(test)]
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
                                runtime.block_on(closing);
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
                *self
                    .control
                    .runtime
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(runtime);
                self.stop_on_drop = true;
                Ok(completed)
            }
            Err(error) => {
                {
                    let mut state = self.control.lock();
                    state.listening = false;
                    state.finished = true;
                    state
                        .report
                        .application
                        .push(AktorError::new(error.to_string()));
                };

                self.killswitch().dispose_shutdown_hook();
                let report = self.control.lock().report.clone();
                self.control.completed.send_replace(Some(report));
                self.control.wake.notify_all();
                Err(AktorError::new(error.to_string()))
            }
        }
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
    pub fn start_standard(&mut self) -> Result<GroupCompletion, AktorError> {
        let completed = self.completion();
        let closing = self.listen(async |_| Ok::<_, AktorCleanupError>(()))?;

        self.control
            .standard
            .store(true, std::sync::atomic::Ordering::Release);

        if let Err(error) = crate::executor::Spawner::Std.spawn_named("aktor group", async move {
            closing.await;
        }) {
            {
                let mut state = self.control.lock();

                state.listening = false;
                state.finished = true;
                state
                    .report
                    .application
                    .push(AktorError::new(error.to_string()));
            };

            self.killswitch().dispose_shutdown_hook();
            let report = self.control.lock().report.clone();
            self.control.completed.send_replace(Some(report));
            self.control.wake.notify_all();
            return Err(AktorError::new(error.to_string()));
        }

        self.stop_on_drop = true;
        Ok(completed)
    }

    #[doc(hidden)]
    pub fn check_started(&self) -> Result<(), AktorError> {
        if !self.control.lock().listening {
            return Err(AktorError::new("actor group has no shutdown listener"));
        }

        Ok(())
    }

    pub fn killswitch(&self) -> KillSwitch {
        KillSwitch {
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
        }
    }

    pub fn completion(&self) -> GroupCompletion {
        GroupCompletion {
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
    pub fn start(&mut self) -> Result<GroupCompletion, AktorError> {
        #[cfg(any(
            any(feature = "tokio", feature = "wasm_browser_workers"),
            target_family = "wasm"
        ))]
        {
            self.start_with(async |_| Ok::<_, AktorCleanupError>(()))
        }
        #[cfg(all(
            not(any(feature = "tokio", feature = "wasm_browser_workers")),
            not(target_family = "wasm")
        ))]
        {
            Err(AktorError::new(
                "use AktorKind::StdThread or enable the tokio feature",
            ))
        }
    }

    /// Start closing now, and observe the same report later.
    pub fn shutdown(&self) -> GroupCompletion {
        self.killswitch().stop();
        self.completion()
    }

    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        not(target_family = "wasm")
    ))]
    /// Start shutdown handling alongside your app. The completion can be awaited later.
    pub fn start_with<Cleanup, CleanupFuture, E>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<GroupCompletion, AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>> + Send + 'static,
        E: 'static,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| AktorError::new("starting an actor group needs a Tokio runtime"))?;
        let completed = self.completion();
        let closing = self.listen(cleanup)?;

        self.stop_on_drop = true;

        runtime.spawn(closing);
        Ok(completed)
    }
    #[cfg(all(
        any(feature = "wasm_browser_workers", feature = "browser_local"),
        target_family = "wasm",
        target_os = "unknown"
    ))]
    /// Start shutdown handling on the browser executor.
    pub fn start_with<Cleanup, CleanupFuture, E>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<GroupCompletion, AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture + 'static,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>> + 'static,
        E: 'static,
    {
        let completed = self.completion();
        let closing = self.listen(cleanup)?;

        self.stop_on_drop = true;

        wasm_bindgen_futures::spawn_local(async move {
            closing.await;
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
    fn listen<Cleanup, CleanupFuture, E>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<impl Future<Output = ShutdownReport> + use<Cleanup, CleanupFuture, E>, AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        self.claim_listener()?;

        let group = Self {
            stop_on_drop: false,
            control: self.control.clone(),
            startup_failure: self.startup_failure.clone(),
            actors: self.actors.clone(),
            #[cfg(target_family = "wasm")]
            shutdown_hook: self.shutdown_hook.clone(),
        };

        let kill = self.killswitch();

        Ok(async move {
            kill.wait_stopping().await;
            group.finish(cleanup).await
        })
    }

    pub(super) fn claim_listener(&self) -> Result<(), AktorError> {
        let mut state = self.control.lock();

        if state.finished {
            return Err(AktorError::new("this group has already stopped"));
        }

        if state.listening {
            return Err(AktorError::new(
                "this group already has a shutdown listener",
            ));
        }

        state.listening = true;
        Ok(())
    }
}
impl Drop for AktorGroup {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.killswitch().stop();
        }
    }
}
impl Default for AktorGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl KillSwitch {
    pub fn stop(&self) {
        let (deadline, report) = {
            let mut state = self.control.lock();

            if state.deadline.is_some() || state.finished {
                return;
            }

            let deadline = shutdown_deadline(Instant::now(), self.control.grace);

            state.report.startup |= state.starting;
            state.deadline = Some(deadline);
            self.control.stopping.store(true, Ordering::Release);

            let report = if state.listening {
                None
            } else {
                state.finished = true;
                Some(state.report.clone())
            };

            (deadline, report)
        };

        #[cfg(not(target_family = "wasm"))]
        if report.is_none() {
            super::native::watchdog(self);
        }

        self.control.changed.send_replace(Some(deadline));

        if report.is_some() {
            #[cfg(not(target_family = "wasm"))]
            self.dispose_shutdown_hook();
            let report = self.control.lock().report.clone();
            self.control.completed.send_replace(Some(report));
        }
    }

    #[cfg(not(target_family = "wasm"))]
    fn dispose_shutdown_hook(&self) {
        let hook = self.control.lock().shutdown_hook.take();
        super::shutdown::contain_drop(hook, self, "cancelled shutdown closure drop");
    }

    /// Let your UI or other tasks know we're closing.
    pub async fn wait_stopping(&self) {
        stopped(&mut self.control.changed.subscribe()).await;
    }

    pub fn is_stopping(&self) -> bool {
        self.control.stopping.load(Ordering::Acquire)
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
    pub fn fail_startup(&self, failure: ActorFailure) -> bool {
        self.record_failure(failure, true)
    }

    #[doc(hidden)]
    pub fn fail(&self, failure: ActorFailure) -> bool {
        self.record_failure(failure, false)
    }

    fn record_failure(&self, mut failure: ActorFailure, startup: bool) -> bool {
        let first = {
            let mut state = self.control.lock();

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

        self.stop();
        first
    }

    pub fn deadline(&self) -> Instant {
        self.control.lock().deadline.unwrap_or_else(Instant::now)
    }

    pub fn force_at(&self) -> Instant {
        let reserve = (self.control.grace / 10).min(Duration::from_millis(100));
        let deadline = self.deadline();
        deadline.checked_sub(reserve).unwrap_or(deadline)
    }

    #[cfg(any(feature = "tokio", feature = "std_thread"))]
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
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

impl GroupCompletion {
    /// Read the retained report if shutdown has finished.
    pub fn try_report(&self) -> Option<ShutdownReport> {
        self.result.borrow().clone()
    }

    pub fn new_observer(&self) -> Self {
        Self {
            result: self.result.clone(),
        }
    }

    pub async fn wait(&self) -> ShutdownReport {
        let mut result = self.result.clone();

        loop {
            if let Some(report) = result.borrow_and_update().clone() {
                return report;
            }

            if result.changed().await.is_err() {
                return ShutdownReport {
                    timed_out: true,
                    ..ShutdownReport::default()
                };
            }
        }
    }
}
impl core::future::IntoFuture for GroupCompletion {
    type Output = ShutdownReport;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<Box<dyn Future<Output = ShutdownReport> + Send>>;
    #[cfg(target_family = "wasm")]
    type IntoFuture = Pin<Box<dyn Future<Output = ShutdownReport>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a> core::future::IntoFuture for &'a GroupCompletion {
    type Output = ShutdownReport;
    #[cfg(not(target_family = "wasm"))]
    type IntoFuture = Pin<Box<dyn Future<Output = ShutdownReport> + Send + 'a>>;
    #[cfg(target_family = "wasm")]
    type IntoFuture = Pin<Box<dyn Future<Output = ShutdownReport> + 'a>>;

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
    #[tokio::test]
    async fn thread_failure() {
        type StartGroup = fn(&mut AktorGroup) -> Result<GroupCompletion, AktorError>;

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

            assert!(result.err().unwrap().to_string().contains("injected"));
            assert!(completed.wait().await.failed());
            assert!(group.check_started().is_err());
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
