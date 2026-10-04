#[cfg(feature = "tokio")]
use super::shutdown::bounded;
use super::*;

impl AktorGroup {
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// How long shutdown gets, including your last closure.
    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    pub fn with_grace(grace: Duration) -> Self {
        let control = Arc::new(Control {
            state: Mutex::new(State::default()),
            changed: watch::channel(None).0,
            completed: watch::channel(None).0,
            grace,
            #[cfg(not(target_family = "wasm"))]
            wake: std::sync::Condvar::new(),
            #[cfg(not(target_family = "wasm"))]
            threads: tokio::sync::Notify::new(),
        });

        Self {
            stop_on_drop: false,
            control,
            #[cfg(not(target_family = "wasm"))]
            actors: Arc::new(Mutex::new(Vec::new())),
            #[cfg(target_family = "wasm")]
            actors: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn killswitch(&self) -> KillSwitch {
        KillSwitch {
            control: self.control.clone(),
        }
    }

    pub fn completion(&self) -> GroupCompletion {
        GroupCompletion {
            result: self.control.completed.subscribe(),
        }
    }

    #[cfg(any(
        not(target_family = "wasm"),
        all(
            feature = "wasm_browser_workers",
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    /// Start shutdown handling with defaults.
    pub fn start(&mut self) -> Result<GroupCompletion, AktorError> {
        self.start_with(async |_| Ok::<_, AktorCleanupError>(()))
    }

    /// Start closing now, and observe the same report later.
    pub fn shutdown(&self) -> GroupCompletion {
        self.killswitch().stop();
        self.completion()
    }

    #[cfg(not(target_family = "wasm"))]
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
        feature = "wasm_browser_workers",
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
            feature = "wasm_browser_workers",
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
            actors: self.actors.clone(),
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
            state.deadline = Some(deadline);
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
        if let Some(report) = report {
            self.control.completed.send_replace(Some(report));
        }
    }

    /// Let your UI or other tasks know we're closing.
    pub async fn wait_stopping(&self) {
        stopped(&mut self.control.changed.subscribe()).await;
    }

    pub fn is_stopping(&self) -> bool {
        self.control.lock().deadline.is_some()
    }

    #[doc(hidden)]
    pub fn fail(&self, failure: ActorFailure) {
        {
            let mut state = self.control.lock();
            if state.report.failure.is_none() {
                state.report.failure = Some(failure);
            }
        }

        self.stop();
    }

    pub fn deadline(&self) -> Instant {
        self.control.lock().deadline.unwrap_or_else(Instant::now)
    }

    pub fn force_at(&self) -> Instant {
        let reserve = (self.control.grace / 10).min(Duration::from_millis(100));
        let deadline = self.deadline();
        deadline.checked_sub(reserve).unwrap_or(deadline)
    }

    #[cfg(feature = "tokio")]
    pub async fn wait_for_force(&self) {
        self.wait_stopping().await;
        bounded(self.force_at(), core::future::pending::<()>()).await;
    }
}
impl Control {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}
impl GroupCompletion {
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

fn shutdown_deadline(now: Instant, grace: Duration) -> Instant {
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
