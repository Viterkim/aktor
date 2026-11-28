use super::*;
use core::{future::poll_fn, pin::pin, task::Poll};
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use er::*;
use futures_util::{
    FutureExt,
    stream::{FuturesUnordered, Stream, StreamExt},
};
use std::panic::{AssertUnwindSafe, catch_unwind};

impl AktorGroup {
    fn settle_application(&self) -> impl Future<Output = ()> + use<> {
        let kill = self.killswitch();
        async move {
            let mut changed = kill.control.application_changed.subscribe();
            while kill.control.lock().applications != 0 {
                if changed.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    pub async fn finish<Cleanup, CleanupFuture>(self, cleanup: Cleanup) -> AktorShutdownReport
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
    {
        let kill = self.killswitch();

        let backup = self.new_registration();
        let run = AssertUnwindSafe(self.collect(cleanup)).catch_unwind().await;
        match run {
            Ok(report) => report,
            Err(payload) => {
                kill.driver_panicked(payload);
                backup
                    .final_hook(shutdown_deadline(Instant::now(), kill.control.grace))
                    .await;
                kill.control.lock().finished = true;
                kill.publish(Collection::Lost, true);
                #[cfg(not(target_family = "wasm"))]
                super::native::poke();
                kill.control.lock().report.snapshot()
            }
        }
    }

    async fn final_hook(&self, deadline: Instant) {
        let kill = self.killswitch();
        #[cfg(not(target_family = "wasm"))]
        let hook = kill.control.lock().shutdown_hook.take();
        #[cfg(target_family = "wasm")]
        let hook = self.shutdown_hook.borrow_mut().take();
        if let Some(hook) = hook {
            let report = kill.control.lock().report.snapshot();
            #[cfg(not(target_family = "wasm"))]
            let hook = |report| hook(report, kill.clone());
            finish_hook(hook, report, &kill, deadline, cfg!(target_family = "wasm")).await;
        }
    }

    async fn collect<Cleanup, CleanupFuture>(self, cleanup: Cleanup) -> AktorShutdownReport
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
    {
        let kill = self.killswitch();
        kill.stop();

        let grace = self.control.grace;
        let reserve = super::impls::reserve(grace);
        let force_at = kill.force_at();
        let mut application = Box::pin(self.settle_application());
        let mut application_done = false;

        #[cfg(not(target_family = "wasm"))]
        let entries = std::mem::take(
            &mut *self
                .actors
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );

        #[cfg(target_family = "wasm")]
        let entries = std::mem::take(&mut *self.actors.borrow_mut());

        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        let mut tasks = core::mem::take(&mut kill.control.lock().callers);

        let count = entries.len();
        let mut done = Done {
            ids: entries.iter().map(|entry| entry.id).collect(),
            finished: vec![false; count],
            given_up: vec![false; count],
            confirm_later: Vec::with_capacity(count),
            confirming: Vec::new(),
            lost_confirmation: false,
        };
        let mut starts = Vec::with_capacity(count);
        let mut cancels = Vec::with_capacity(count);
        let mut give_ups = Vec::with_capacity(count);
        let mut pending = FuturesUnordered::new();

        for (index, entry) in entries.into_iter().enumerate() {
            starts.push(Some(entry.start));
            cancels.push(Some(entry.cancel));
            give_ups.push(entry.give_up);
            done.confirm_later.push(entry.confirm_later);
            let observer = kill.clone();
            pending.push(async move {
                let outcome = AssertUnwindSafe(entry.outcome)
                    .catch_unwind()
                    .await
                    .map_or_else(
                        |payload| {
                            observer.driver_panicked(payload);
                            None
                        },
                        Some,
                    );
                (index, outcome)
            });
        }

        // Later actors can depend on earlier ones, so they stop first, like drop order.
        // Each one gets a grace for its calls and another for its `end`, a stuck actor
        // doesn't eat the next one's time.
        let stopped = {
            let stopping = async {
                for position in (0..=count).rev() {
                    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
                    while let Some(task) = tasks.last_mut().filter(|task| task.after == position) {
                        task.stop(&kill, grace).await;
                        tasks.pop();
                    }
                    let Some(index) = position.checked_sub(1) else {
                        break;
                    };

                    if done.finished[index] {
                        continue;
                    }

                    if let Some(start) = starts[index].take() {
                        contain(start, &kill, crate::AktorFailureSource::GroupDriver);
                    }

                    let until = shutdown_deadline(Instant::now(), grace);

                    if done.settle(&kill, &mut pending, index, until).await {
                        continue;
                    }

                    if let Some(owner) = kill
                        .control
                        .lock()
                        .report
                        .actor_mut(done.ids[index])
                        .and_then(|actor| actor.owner.as_mut())
                    {
                        owner.ran_past_grace = true;
                    }

                    if let Some(cancel) = cancels[index].take() {
                        contain(cancel, &kill, crate::AktorFailureSource::GroupDriver);
                    }

                    // `end` times itself, this is its grace plus a moment to report.
                    let until = shutdown_deadline(Instant::now(), grace.saturating_add(reserve));

                    if done.settle(&kill, &mut pending, index, until).await {
                        continue;
                    }

                    done.give_up(&kill, index, give_ups[index].take());
                }
            };
            let mut stopping = pin!(stopping);
            let mut stopped = false;

            let both = poll_fn(|cx| {
                if !application_done && application.as_mut().poll(cx).is_ready() {
                    application_done = true;
                }

                if !stopped && stopping.as_mut().poll(cx).is_ready() {
                    stopped = true;
                }

                if stopped {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            });

            kill.bounded(force_at, both).await.is_some()
        };

        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        for task in &mut tasks {
            task.cut(&kill);
        }

        if !stopped {
            for index in (0..count).rev() {
                if done.finished[index] {
                    continue;
                }

                if let Some(start) = starts[index].take() {
                    contain(start, &kill, crate::AktorFailureSource::GroupDriver);
                }

                if let Some(owner) = kill
                    .control
                    .lock()
                    .report
                    .actor_mut(done.ids[index])
                    .and_then(|actor| actor.owner.as_mut())
                {
                    owner.ran_past_grace = true;
                }

                if let Some(cancel) = cancels[index].take() {
                    contain(cancel, &kill, crate::AktorFailureSource::GroupDriver);
                }

                done.give_up(&kill, index, give_ups[index].take());
            }
        }

        // Every actor had its turn, what's left gets one more grace instead of the whole budget.
        let deadline = kill.shorten_deadline();
        let force_at = deadline.checked_sub(reserve).unwrap_or(deadline);
        let settle_at = deadline.checked_sub(reserve / 2).unwrap_or(deadline);

        // Stragglers get a moment here, most of the grace belongs to the shutdown closure.
        let soon = shutdown_deadline(Instant::now(), reserve);

        if !application_done {
            application_done = kill
                .bounded(soon.min(force_at), &mut application)
                .await
                .is_some();
        }

        #[cfg(not(target_family = "wasm"))]
        let threads_done = done
            .wait_threads(&kill, &mut pending, soon.min(settle_at))
            .await;

        #[cfg(target_family = "wasm")]
        let threads_done = true;

        if threads_done {
            done.land(&kill, &mut pending, soon.min(settle_at)).await;
        }

        let before_hook = kill.control.lock().report.snapshot();
        let hook_deadline = deadline.checked_sub(reserve / 4).unwrap_or(deadline);

        #[cfg(not(target_family = "wasm"))]
        let inline = kill.control.lock().listener_hook.is_none();
        #[cfg(target_family = "wasm")]
        let inline = true;
        finish_hook(cleanup, before_hook, &kill, hook_deadline, inline).await;

        let deferred_hook = {
            let state = kill.control.lock();
            !state.hook_at_grace
                && (done.given_up.contains(&true)
                    || !done.confirming.is_empty()
                    || !application_done
                    || state.callback_workers != 0)
        };
        if !deferred_hook {
            self.final_hook(hook_deadline).await;
        }

        // A stuck `Kill` thread is waited on after the closure, so the published report still
        // gets its real outcome when it finishes in time. If it doesn't, it stays tracked and
        // the group policy decides whether the watchdog takes the process down.
        #[cfg(not(target_family = "wasm"))]
        if !threads_done && done.wait_threads(&kill, &mut pending, settle_at).await {
            done.land(
                &kill,
                &mut pending,
                shutdown_deadline(Instant::now(), reserve).min(settle_at),
            )
            .await;
        }

        let kept = {
            let mut state = kill.control.lock();

            #[cfg(not(target_family = "wasm"))]
            {
                state.actors_and_hooks_done = true;
            }

            state.kept.take()
        };

        contain_drop(kept, &kill, crate::AktorFailureSource::GroupDriver);

        #[cfg(not(target_family = "wasm"))]
        super::native::poke();

        if !application_done {
            application_done = kill.bounded(deadline, &mut application).await.is_some();
        }

        // An actor shutdown gave up on may have finished while the closure ran.
        while let Some(Some((index, outcome))) = pending.next().now_or_never() {
            done.late(&kill, index, outcome);
        }

        let mut workers_changed = kill.control.application_changed.subscribe();
        let (report, mut workers) = {
            let mut state = kill.control.lock();
            state.finished = true;
            if !application_done {
                state.report.late(crate::AktorLateWork::Application);
            }
            (state.report.snapshot(), state.callback_workers)
        };

        let has_late = done.given_up.contains(&true) || !done.confirming.is_empty();
        kill.publish(
            done.collection(!has_late),
            !deferred_hook && application_done && workers == 0,
        );
        #[cfg(not(target_family = "wasm"))]
        super::native::poke();

        // A left actor can still finish later, and a worker that came back Unconfirmed can still
        // confirm. Whoever ran shutdown keeps collecting those and publishes the report again
        // each time, so `wait_retired` sees it.
        if deferred_hook || has_late || !application_done || workers != 0 {
            let group = self.new_registration();
            let late = kill.clone();
            let collecting = async move {
                let mut done = done;
                let mut pending = pending;
                let mut confirming = FuturesUnordered::new();
                let mut updated = false;

                loop {
                    for (id, confirmed) in done.confirming.drain(..) {
                        let observer = late.clone();
                        confirming.push(async move {
                            let retirement = AssertUnwindSafe(confirmed)
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|payload| {
                                    observer.driver_panicked(payload);
                                    None
                                });
                            (id, retirement)
                        });
                    }

                    let waiting = done.given_up.contains(&true);
                    let actors_done = !waiting && confirming.is_empty();

                    if actors_done && application_done && workers == 0 {
                        if deferred_hook {
                            group
                                .final_hook(shutdown_deadline(Instant::now(), late.control.grace))
                                .await;
                            workers = late.control.lock().callback_workers;
                            if workers != 0 {
                                continue;
                            }
                        }
                        late.publish(done.collection(true), true);
                        return;
                    }

                    if updated {
                        late.publish(
                            done.collection(false),
                            !deferred_hook && application_done && workers == 0,
                        );
                    }

                    let worker_finished = async {
                        loop {
                            let active = late.control.lock().callback_workers;
                            if active < workers {
                                return active;
                            }

                            if workers_changed.changed().await.is_err() {
                                return 0;
                            }
                        }
                    };

                    tokio::select! {
                        biased;
                        active = worker_finished, if workers != 0 => workers = active,
                        late_outcome = pending.next(), if waiting => match late_outcome {
                            Some((index, outcome)) => done.late(&late, index, outcome),
                            None => done.given_up.fill(false),
                        },
                        confirmed = confirming.next(), if !confirming.is_empty() => {
                            if let Some((id, confirmed)) = confirmed {
                                if let Some(retirement) = confirmed {
                                    let mut state = late.control.lock();
                                    let actor = state.report.actor_mut(id).expect("registered actor");
                                    if let Some(owner) = &mut actor.owner { owner.retirement = retirement; }
                                } else {
                                    done.lost_confirmation = true;
                                }
                            }
                        },
                        _ = &mut application, if !application_done => application_done = true,
                    }

                    updated = true;
                }
            };

            let fallback = self.new_registration();
            let collecting = async move {
                if let Err(payload) = AssertUnwindSafe(collecting).catch_unwind().await {
                    let kill = fallback.killswitch();
                    kill.driver_panicked(payload);
                    fallback
                        .final_hook(shutdown_deadline(Instant::now(), kill.control.grace))
                        .await;
                    kill.control.lock().finished = true;
                    kill.publish(Collection::Lost, true);
                    #[cfg(not(target_family = "wasm"))]
                    super::native::poke();
                }
            };
            #[cfg(not(target_family = "wasm"))]
            {
                kill.control.lock().late = Some(Box::pin(collecting));
            }
            #[cfg(target_family = "wasm")]
            {
                *self.late.borrow_mut() = Some(Box::pin(collecting));
            }
        }

        report
    }
}

impl AktorKillSwitch {
    fn driver_panicked(&self, payload: Box<dyn core::any::Any + Send>) {
        self.fail(AktorFailureInput::new(
            crate::AktorFailureSource::GroupDriver,
            crate::panic::panic_tree(&payload).er_report(),
        ));
        contain_drop(payload, self, crate::AktorFailureSource::GroupDriver);
    }
}

/// Owners can finish in any order, including after their grace expires.
struct Done {
    ids: Vec<crate::ActorId>,
    finished: Vec<bool>,
    given_up: Vec<bool>,
    confirm_later: Vec<Option<ConfirmLater>>,
    /// Owners we can still get retirement evidence for.
    confirming: Vec<(crate::ActorId, ConfirmLater)>,
    lost_confirmation: bool,
}
impl Done {
    fn collection(&self, complete: bool) -> Collection {
        if self.lost_confirmation {
            Collection::Lost
        } else if complete {
            Collection::Complete
        } else {
            Collection::Pending
        }
    }

    /// Collect outcomes until this actor's arrives, other actors can finish on their own meanwhile.
    async fn settle(
        &mut self,
        kill: &AktorKillSwitch,
        pending: &mut (impl Stream<Item = (usize, Option<OwnerOutcome>)> + Unpin),
        index: usize,
        until: Instant,
    ) -> bool {
        let collect = async {
            while !self.finished[index] {
                let Some((finished, outcome)) = pending.next().await else {
                    return;
                };

                self.late(kill, finished, outcome);
            }
        };

        kill.bounded(until, collect).await;
        self.finished[index]
    }

    /// Wait for tracked threads to end, collecting outcomes meanwhile. Threads shutdown may
    /// leave don't count.
    #[cfg(not(target_family = "wasm"))]
    async fn wait_threads(
        &mut self,
        kill: &AktorKillSwitch,
        pending: &mut (impl Stream<Item = (usize, Option<OwnerOutcome>)> + Unpin),
        until: Instant,
    ) -> bool {
        let mut collecting = true;
        let finished = async {
            loop {
                if kill.control.lock().only_left() {
                    return;
                }

                tokio::select! {
                    _ = kill.control.threads.notified() => {},
                    late = pending.next(), if collecting => match late {
                        Some((index, outcome)) => self.late(kill, index, outcome),
                        None => collecting = false,
                    },
                }
            }
        };

        kill.bounded(until, finished).await.is_some()
    }

    /// A given up actor whose thread just ended has its real outcome on the way, give it a
    /// moment so the report doesn't call a clean close Unconfirmed.
    async fn land(
        &mut self,
        kill: &AktorKillSwitch,
        pending: &mut (impl Stream<Item = (usize, Option<OwnerOutcome>)> + Unpin),
        until: Instant,
    ) {
        if !self.given_up.contains(&true) {
            return;
        }

        let landing = async {
            while self.given_up.contains(&true) {
                let Some((index, outcome)) = pending.next().await else {
                    return;
                };

                self.late(kill, index, outcome);
            }
        };

        kill.bounded(until, landing).await;
    }

    /// An outcome arrived, maybe for an actor shutdown already gave up on.
    fn late(&mut self, kill: &AktorKillSwitch, index: usize, outcome: Option<OwnerOutcome>) {
        let mut state = kill.control.lock();

        let id = self.ids[index];
        if self.finished[index] && !self.given_up[index] {
            return;
        }

        self.given_up[index] = false;
        self.finished[index] = true;
        let Some(mut outcome) = outcome else {
            self.lost_confirmation = true;
            return;
        };

        let actor = state.report.actor_mut(id).expect("registered actor");
        if actor.retirement() == Some(crate::AktorRetirement::Terminated) {
            outcome.retirement = crate::AktorRetirement::Terminated;
        }
        let unconfirmed = outcome.retirement == crate::AktorRetirement::StillRunning;
        actor.owner_outcome(outcome);

        if unconfirmed {
            if let Some(confirmed) = self.confirm_later[index].take() {
                self.confirming.push((id, confirmed));
            } else {
                self.lost_confirmation = true;
            }
        }
    }

    fn give_up(&mut self, kill: &AktorKillSwitch, index: usize, give_up: Option<GiveUp>) {
        {
            let mut state = kill.control.lock();

            let actor = state
                .report
                .actor_mut(self.ids[index])
                .expect("registered actor");
            if let Some(owner) = &mut actor.owner {
                owner.ran_past_grace = true;
            }
            self.given_up[index] = true;
        }

        self.finished[index] = true;

        if let Some(give_up) = give_up {
            let mut gone = false;

            contain(
                || gone = give_up(),
                kill,
                crate::AktorFailureSource::GroupDriver,
            );

            if gone
                && let Some(actor) = kill.control.lock().report.actor_mut(self.ids[index])
                && let Some(owner) = &mut actor.owner
            {
                owner.retirement = crate::AktorRetirement::Terminated;
            }
        }
    }
}

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub(crate) async fn bounded_standard<F: Future>(deadline: Instant, future: F) -> Option<F::Output> {
    let timer = crate::executor::sleep_until(deadline);
    tokio::pin!(timer, future);
    tokio::select! { biased; output = &mut future => Some(output), _ = &mut timer => None }
}

pub(crate) async fn bounded<F: Future>(deadline: Instant, future: F) -> Option<F::Output> {
    #[cfg(all(
        not(any(feature = "tokio", feature = "wasm_browser_workers")),
        not(feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    let _ = deadline;
    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        not(target_family = "wasm")
    ))]
    let timer = tokio::time::sleep(deadline.saturating_duration_since(Instant::now()));
    #[cfg(all(
        feature = "std_thread",
        not(any(feature = "tokio", feature = "wasm_browser_workers")),
        not(target_family = "wasm")
    ))]
    let timer = crate::executor::sleep_until(deadline);
    #[cfg(all(
        not(feature = "std_thread"),
        not(any(feature = "tokio", feature = "wasm_browser_workers")),
        not(target_family = "wasm")
    ))]
    let timer = core::future::pending::<()>();
    #[cfg(target_family = "wasm")]
    let timer = browser_deadline(
        || deadline.saturating_duration_since(Instant::now()),
        gloo_timers::future::TimeoutFuture::new,
    );

    tokio::pin!(timer, future);
    tokio::select! { biased; output = &mut future => Some(output), _ = &mut timer => None }
}

#[cfg(any(target_family = "wasm", all(test, feature = "tokio")))]
async fn browser_deadline<F: Future<Output = ()>>(
    mut remaining: impl FnMut() -> Duration,
    mut schedule: impl FnMut(u32) -> F,
) {
    loop {
        let duration = remaining();

        if duration.is_zero() {
            return;
        }

        schedule(crate::timeout::browser_millis(duration)).await;
    }
}

async fn finish_hook<Cleanup, CleanupFuture>(
    cleanup: Cleanup,
    report: AktorShutdownReport,
    kill: &AktorKillSwitch,
    deadline: Instant,
    inline: bool,
) where
    Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
{
    #[cfg(not(target_family = "wasm"))]
    {
        kill.control.lock().callback_running = true;
        super::native::poke();
    }
    let mut hook = Box::pin(AssertUnwindSafe(async { cleanup(report).await }).catch_unwind());

    match kill.bounded(deadline, &mut hook).await {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err((_, report)))) => {
            kill.fail(AktorFailureInput::new(
                crate::AktorFailureSource::ShutdownCallback,
                report,
            ));
        }
        Some(Err(payload)) => {
            kill.fail(AktorFailureInput::new(
                crate::AktorFailureSource::ShutdownCallback,
                crate::panic::panic_tree(&payload).er_report(),
            ));

            contain_drop(payload, kill, crate::AktorFailureSource::ShutdownCallback);
        }
        None if inline => kill.control.lock().report.callback_timed_out(),
        None => kill
            .control
            .lock()
            .report
            .late(crate::AktorLateWork::ShutdownCallback),
    }

    contain_drop(hook, kill, crate::AktorFailureSource::ShutdownCallback);

    #[cfg(not(target_family = "wasm"))]
    {
        let mut state = kill.control.lock();
        state.callback_running = false;
        if Instant::now() >= deadline {
            state.report.late(crate::AktorLateWork::ShutdownCallback);
        }
    }
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub(super) fn fail_application<E: core::error::Error + 'static>(
    kill: &AktorKillSwitch,
    error: ErBranch<E>,
) {
    kill.fail(AktorFailureInput::new(
        crate::AktorFailureSource::ApplicationTask,
        error.er_report(),
    ));
}

pub(crate) fn contain_drop(
    value: impl Sized,
    kill: &AktorKillSwitch,
    source: crate::AktorFailureSource<()>,
) {
    contain(|| drop(value), kill, source);
}

pub(crate) fn contain(
    action: impl FnOnce(),
    kill: &AktorKillSwitch,
    source: crate::AktorFailureSource<()>,
) {
    contain_with(action, kill, || source.clone());
}

pub(crate) fn contain_with(
    action: impl FnOnce(),
    kill: &AktorKillSwitch,
    source: impl Fn() -> crate::AktorFailureSource<()>,
) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(action)) {
        crate::panic::each(payload, |payload, _| {
            kill.fail(AktorFailureInput::new(
                source(),
                crate::panic::panic_tree(&payload).er_report(),
            ));
            crate::panic::dispose_with(payload, |nested| {
                kill.fail(AktorFailureInput::new(
                    source(),
                    crate::panic::panic_tree(nested).er_report(),
                ));
            });
        });
    }
}

#[cfg(all(test, feature = "tokio", not(target_family = "wasm")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn browser_deadlines() {
        use std::cell::Cell;

        let max = i32::MAX as u64;

        for duration in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_millis(max),
            Duration::from_millis(max + 1),
            Duration::from_millis(2 * max + 17),
        ] {
            let remaining = Cell::new(duration);
            let mut elapsed = Duration::ZERO;

            browser_deadline(
                || remaining.get(),
                |chunk| {
                    assert!(i32::try_from(chunk).is_ok());

                    let wait = Duration::from_millis(u64::from(chunk));

                    remaining.set(remaining.get().saturating_sub(wait));
                    elapsed += wait;
                    core::future::ready(())
                },
            )
            .await;
            assert!(remaining.get().is_zero());
            assert!(
                elapsed >= duration && elapsed.saturating_sub(duration) < Duration::from_millis(1)
            );
        }
    }

    #[tokio::test]
    async fn stop_keeps_one_deadline() {
        let mut group = AktorGroup::new();
        let complete = group.start().unwrap();
        let kill = group.killswitch();
        let racers: Vec<_> = (0..8)
            .map(|_| {
                let kill = kill.clone();

                std::thread::spawn(move || {
                    kill.stop();
                    kill.deadline()
                })
            })
            .collect();

        let deadlines: Vec<_> = racers
            .into_iter()
            .map(|racer| racer.join().unwrap())
            .collect();

        assert!(deadlines.iter().all(|value| *value == deadlines[0]));
        assert_eq!(kill.deadline(), deadlines[0]);
        assert!(!complete.wait().await.unwrap().failed());
    }
}
