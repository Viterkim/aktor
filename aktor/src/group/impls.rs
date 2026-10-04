use super::*;
use futures_util::{
    FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use std::panic::{AssertUnwindSafe, catch_unwind};

impl AktorGroup {
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// How long shutdown gets, including your last closure.
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

    fn claim_listener(&self) -> Result<(), AktorError> {
        let mut state = self.control.lock();
        if state.listening {
            return Err(AktorError::new(
                "this group already has a shutdown listener",
            ));
        }

        state.listening = true;
        Ok(())
    }

    /// Your last closure gets the reports after actor cleanup.
    pub async fn run<O, A, Application, Cleanup, CleanupFuture, E>(
        mut self,
        application: Application,
        cleanup: Cleanup,
    ) -> Result<Option<O>, Box<ShutdownReport>>
    where
        Application: AsyncFnOnce(&mut AktorGroup) -> Result<O, AktorError<A>>,
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        if let Err(error) = self.claim_listener() {
            return Err(Box::new(ShutdownReport {
                application: vec![error],
                ..ShutdownReport::default()
            }));
        }

        let kill = self.killswitch();
        let mut stopping = self.control.changed.subscribe();
        let mut output = None;

        {
            let mut application = Box::pin(AssertUnwindSafe(application(&mut self)).catch_unwind());
            tokio::select! {
                biased;
                _ = stopped(&mut stopping) => {}
                value = &mut application => match value {
                    Ok(Ok(value)) => output = Some(value),
                    Ok(Err(error)) => kill.control.lock().report.application.push(error.report()),
                    Err(payload) => kill.fail(ActorFailure {
                        actor: "application".into(), phase: "run".into(), message: panic_message(&payload),
                    }),
                },
            }

            contain_drop(application, &kill, "application drop");
        }

        let report = self.finish(cleanup).await;
        if report.failed() {
            Err(Box::new(report))
        } else {
            Ok(output)
        }
    }

    async fn finish<Cleanup, CleanupFuture, E>(self, cleanup: Cleanup) -> ShutdownReport
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        let kill = self.killswitch();
        kill.stop();
        let deadline = kill.deadline();
        let reserve = (self.control.grace / 10).min(Duration::from_millis(100));
        let force_at = kill.force_at();
        let mut pending = FuturesUnordered::new();
        let mut cancellations = Vec::new();
        let mut names = Vec::new();

        #[cfg(not(target_family = "wasm"))]
        let entries = std::mem::take(
            &mut *self
                .actors
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        #[cfg(target_family = "wasm")]
        let entries = std::mem::take(&mut *self.actors.borrow_mut());
        let mut done = vec![false; entries.len()];

        for (index, entry) in entries.into_iter().enumerate().rev() {
            (entry.start)();
            names.push((index, entry.name));
            cancellations.push((index, entry.cancel));
            pending.push(async move { (index, entry.outcome.await) });
        }

        let drained = async {
            while let Some((index, outcome)) = pending.next().await {
                done[index] = true;
                kill.control.lock().report.actors.push(outcome);
            }
        };

        if bounded(force_at, drained).await.is_none() {
            kill.control.lock().report.timed_out = true;
            for (index, cancel) in cancellations {
                if !done[index] {
                    cancel();
                }
            }

            let forced = async {
                while let Some((index, outcome)) = pending.next().await {
                    done[index] = true;
                    kill.control.lock().report.actors.push(outcome);
                }
            };

            let settle_at = deadline.checked_sub(reserve / 2).unwrap_or(deadline);
            if bounded(settle_at, forced).await.is_none() {
                let mut state = kill.control.lock();
                state.report.timed_out = true;
                state.force_exit = true;
                for (index, name) in names {
                    if !done[index] {
                        state.report.actors.push(ActorOutcome {
                            actor: name,
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });
                    }
                }
            }
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let finished = async {
                loop {
                    if kill.control.lock().running.is_empty() {
                        return;
                    }
                    kill.control.threads.notified().await;
                }
            };
            if bounded(force_at, finished).await.is_none() {
                let mut state = kill.control.lock();
                state.report.timed_out = true;
                state.force_exit = true;
                let running = state.running.clone();
                for name in running {
                    if !state.report.actors.iter().any(|actor| actor.actor == *name) {
                        state.report.actors.push(ActorOutcome {
                            actor: (*name).clone(),
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });
                    }
                }
            }
        }

        let before_hook = kill.control.lock().report.clone();
        let mut hook =
            Box::pin(AssertUnwindSafe(async { cleanup(before_hook).await }).catch_unwind());
        let hook_deadline = deadline.checked_sub(reserve / 4).unwrap_or(deadline);
        match bounded(hook_deadline, &mut hook).await {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(error))) => kill.control.lock().report.application.push(error.report()),
            Some(Err(payload)) => {
                let message = panic_message(&payload);
                kill.fail(ActorFailure {
                    actor: "application".into(),
                    phase: "cleanup".into(),
                    message,
                });
            }
            None => kill.control.lock().report.timed_out = true,
        }

        contain_drop(hook, &kill, "cleanup drop");

        let report = {
            let mut state = kill.control.lock();
            state.finished = true;
            state.report.clone()
        };

        kill.control.completed.send_replace(Some(report.clone()));
        #[cfg(not(target_family = "wasm"))]
        kill.control.wake.notify_all();

        report
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
        let first = {
            let mut state = self.control.lock();

            if self.control.changed.borrow().is_some() || state.finished {
                false
            } else {
                let deadline = Instant::now()
                    .checked_add(self.control.grace)
                    .unwrap_or_else(Instant::now);
                self.control.changed.send_replace(Some(deadline));
                // Keep the stop bit and failure independent.
                state.report.timed_out = false;
                true
            }
        };

        #[cfg(not(target_family = "wasm"))]
        if first {
            super::native::watchdog(self);
        }
        #[cfg(target_family = "wasm")]
        let _ = first;
    }

    /// Let your UI or other tasks know we're closing.
    pub async fn wait_stopping(&self) {
        stopped(&mut self.control.changed.subscribe()).await;
    }

    pub fn is_stopping(&self) -> bool {
        self.control.changed.borrow().is_some()
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

    fn deadline(&self) -> Instant {
        self.control.changed.borrow().unwrap_or_else(Instant::now)
    }

    fn force_at(&self) -> Instant {
        let reserve = (self.control.grace / 10).min(Duration::from_millis(100));
        let deadline = self.deadline();
        deadline.checked_sub(reserve).unwrap_or(deadline)
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn wait_for_force(&self) {
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

async fn stopped(stopping: &mut watch::Receiver<Option<Instant>>) {
    loop {
        if stopping.borrow_and_update().is_some() {
            return;
        }

        if stopping.changed().await.is_err() {
            return;
        }
    }
}

pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic payload had no message".into()
    }
}

pub async fn bounded<F: Future>(deadline: Instant, future: F) -> Option<F::Output> {
    #[cfg(not(target_family = "wasm"))]
    let timer = tokio::time::sleep(deadline.saturating_duration_since(Instant::now()));
    #[cfg(target_family = "wasm")]
    let timer = gloo_timers::future::TimeoutFuture::new(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u32::MAX)) as u32,
    );

    tokio::pin!(timer, future);
    tokio::select! { biased; output = &mut future => Some(output), _ = &mut timer => None }
}

fn contain_drop(value: impl Sized, kill: &KillSwitch, phase: &str) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        kill.control
            .lock()
            .report
            .application
            .push(AktorError::new(format!(
                "{phase}: {}",
                panic_message(&payload)
            )));
        kill.fail(ActorFailure {
            actor: "application".into(),
            phase: phase.into(),
            message: panic_message(&payload),
        });
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            std::mem::forget(payload);
        }
    }
}
