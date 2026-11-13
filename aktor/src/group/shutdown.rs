use super::*;
use core::{future::poll_fn, pin::pin, task::Poll};
use futures_util::{
    FutureExt,
    stream::{FuturesUnordered, Stream, StreamExt},
};
use std::panic::{AssertUnwindSafe, catch_unwind};

impl AktorGroup {
    fn settle_application(&self) -> impl Future<Output = ()> + use<> {
        let kill = self.killswitch();
        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        let callers = std::mem::take(&mut kill.control.lock().callers);
        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        for caller in &callers {
            caller.abort();
        }

        async move {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            let mut callers = callers;
            let mut changed = kill.control.application_changed.subscribe();
            loop {
                #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
                for caller in callers {
                    if let Err(error) = caller.await
                        && error.is_panic()
                    {
                        let payload = error.into_panic();
                        fail_application(&kill, "task", panic_message(&payload));
                        contain_drop(payload, &kill, "task panic drop");
                    }
                }

                #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
                {
                    let mut state = kill.control.lock();
                    callers = std::mem::take(&mut state.callers);
                    if state.applications == 0 && callers.is_empty() {
                        break;
                    }
                }
                #[cfg(not(all(feature = "tokio", not(target_family = "wasm"))))]
                if kill.control.lock().applications == 0 {
                    break;
                }
                #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
                {
                    for caller in &callers {
                        caller.abort();
                    }
                    if !callers.is_empty() {
                        continue;
                    }
                }
                if changed.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    pub async fn finish<Cleanup, CleanupFuture>(self, cleanup: Cleanup) -> AktorShutdownReport
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorError>>,
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

        let count = entries.len();
        let mut done = Done {
            finished: vec![false; count],
            given_up: vec![None; count],
            confirm_later: Vec::with_capacity(count),
            confirming: Vec::new(),
            lost_confirmation: false,
        };
        let mut starts = Vec::with_capacity(count);
        let mut cancels = Vec::with_capacity(count);
        let mut give_ups = Vec::with_capacity(count);
        let mut actors = Vec::with_capacity(count);
        let mut pending = FuturesUnordered::new();

        for (index, entry) in entries.into_iter().enumerate() {
            starts.push(Some(entry.start));
            cancels.push(Some(entry.cancel));
            give_ups.push(entry.give_up);
            done.confirm_later.push(entry.confirm_later);
            actors.push((entry.name, entry.kind));
            pending.push(async move {
                let mut outcome = entry.outcome.await;
                outcome.kind = entry.kind;
                (index, outcome)
            });
        }

        // Later actors can depend on earlier ones, so they stop first, like drop order.
        // Each one gets a grace for its calls and another for its `end`, a stuck actor
        // doesn't eat the next one's time.
        let stopped = {
            let stopping = async {
                for index in (0..count).rev() {
                    if done.finished[index] {
                        continue;
                    }

                    if let Some(start) = starts[index].take() {
                        start();
                    }

                    let until = shutdown_deadline(Instant::now(), grace);

                    if done.settle(&kill, &mut pending, index, until).await {
                        continue;
                    }

                    if let Some(cancel) = cancels[index].take() {
                        cancel();
                    }

                    // `end` times itself, this is its grace plus a moment to report.
                    let until = shutdown_deadline(Instant::now(), grace.saturating_add(reserve));

                    if done.settle(&kill, &mut pending, index, until).await {
                        continue;
                    }

                    done.give_up(&kill, index, &actors[index], give_ups[index].take());
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

        if !stopped {
            kill.control.lock().report.ran_past_grace = true;

            for (index, actor) in actors.iter().enumerate().rev() {
                if done.finished[index] {
                    continue;
                }

                if let Some(start) = starts[index].take() {
                    start();
                }

                if let Some(cancel) = cancels[index].take() {
                    cancel();
                }

                done.give_up(&kill, index, actor, give_ups[index].take());
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

        let before_hook = kill.control.lock().report.clone();
        let hook_deadline = deadline.checked_sub(reserve / 4).unwrap_or(deadline);

        finish_hook(cleanup, before_hook, &kill, hook_deadline).await;

        #[cfg(not(target_family = "wasm"))]
        let registered = kill.control.lock().shutdown_hook.take();
        #[cfg(target_family = "wasm")]
        let registered = self.shutdown_hook.borrow_mut().take();
        if let Some(hook) = registered {
            let before_hook = kill.control.lock().report.clone();
            finish_hook(hook, before_hook, &kill, hook_deadline).await;
        }

        // A stuck `Kill` thread is waited on after the closure, so the published report still
        // gets its real outcome when it finishes in time. If it doesn't, it stays tracked and
        // the group policy decides whether the watchdog takes the process down.
        #[cfg(not(target_family = "wasm"))]
        if !threads_done {
            if done.wait_threads(&kill, &mut pending, settle_at).await {
                done.land(
                    &kill,
                    &mut pending,
                    shutdown_deadline(Instant::now(), reserve).min(settle_at),
                )
                .await;
            } else {
                let mut state = kill.control.lock();

                state.report.ran_past_grace = true;

                let running = state.running.clone();

                for (name, _) in running {
                    // Only actors, a thread like an interval runner isn't one of those.
                    if !state.report.actors.iter().any(|actor| actor.actor == *name)
                        && let Some(kind) = state.kind(&name)
                    {
                        state.report.actors.push(AktorOutcome {
                            retirement: crate::AktorRetireStatus::Unconfirmed,
                            kind,
                            actor: (*name).clone(),
                            diagnostics: Vec::new(),
                            ran_past_grace: true,
                        });
                    }
                }
            }
        }

        #[cfg(not(target_family = "wasm"))]
        {
            kill.control.lock().actors_and_hooks_done = true;
            kill.control.wake.notify_all();
        }

        if !application_done {
            application_done = kill.bounded(deadline, &mut application).await.is_some();
        }

        // An actor shutdown gave up on may have finished while the closure ran.
        while let Some(Some((index, outcome))) = pending.next().now_or_never() {
            done.late(&kill, index, outcome);
        }

        let mut application_changed = kill.control.application_changed.subscribe();
        let report = {
            let mut state = kill.control.lock();
            state.finished = true;
            state.report.ran_past_grace |= !application_done;
            state.report.clone()
        };

        let has_late = done.given_up.iter().any(Option::is_some) || !done.confirming.is_empty();
        kill.control.completed.send_replace(Some(Completed {
            report: report.clone(),
            retirement_final: done.lost_confirmation || !has_late,
        }));
        #[cfg(not(target_family = "wasm"))]
        kill.control.wake.notify_all();

        // A left actor can still finish later, and a worker that came back Unconfirmed can still
        // confirm. Whoever ran shutdown keeps collecting those and publishes the report again
        // each time, so `wait_retired` sees it.
        if has_late || !application_done {
            let late = kill.clone();
            let collecting = async move {
                let mut done = done;
                let mut pending = pending;
                let mut confirming = FuturesUnordered::new();
                let mut updated = false;

                loop {
                    for (position, confirmed) in done.confirming.drain(..) {
                        confirming.push(async move { (position, confirmed.await) });
                    }

                    let waiting = done.given_up.iter().any(Option::is_some);
                    let actors_done = !waiting && confirming.is_empty();

                    if updated {
                        let report = late.control.lock().report.clone();
                        late.control.completed.send_replace(Some(Completed {
                            report,
                            retirement_final: done.lost_confirmation || actors_done,
                        }));
                    }

                    if actors_done && application_done {
                        return;
                    }

                    tokio::select! {
                        biased;
                        late_outcome = pending.next(), if waiting => match late_outcome {
                            Some((index, outcome)) => done.late(&late, index, outcome),
                            None => done.given_up.fill(None),
                        },
                        confirmed = confirming.next(), if !confirming.is_empty() => {
                            if let Some((position, confirmed)) = confirmed {
                                if let Some((retirement, diagnostics)) = confirmed {
                                    let mut state = late.control.lock();
                                    let actor = &mut state.report.actors[position];
                                    actor.retirement = retirement;
                                    actor.diagnostics = diagnostics;
                                } else {
                                    done.lost_confirmation = true;
                                }
                            }
                        },
                        _ = &mut application, if !application_done => application_done = true,
                        _ = application_changed.changed(), if !application_done => {},
                    }

                    updated = true;
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

/// Which actors are finished, and where the ones shutdown gave up on sit in the report.
struct Done {
    finished: Vec<bool>,
    given_up: Vec<Option<usize>>,
    confirm_later: Vec<Option<ConfirmLater>>,
    /// Report positions of Unconfirmed actors that can still confirm.
    confirming: Vec<(usize, ConfirmLater)>,
    lost_confirmation: bool,
}
impl Done {
    /// Collect outcomes until this actor's arrives, other actors can finish on their own meanwhile.
    async fn settle(
        &mut self,
        kill: &AktorKillSwitch,
        pending: &mut (impl Stream<Item = (usize, AktorOutcome)> + Unpin),
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
        pending: &mut (impl Stream<Item = (usize, AktorOutcome)> + Unpin),
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
        pending: &mut (impl Stream<Item = (usize, AktorOutcome)> + Unpin),
        until: Instant,
    ) {
        if self.given_up.iter().all(Option::is_none) {
            return;
        }

        let landing = async {
            while self.given_up.iter().any(Option::is_some) {
                let Some((index, outcome)) = pending.next().await else {
                    return;
                };

                self.late(kill, index, outcome);
            }
        };

        kill.bounded(until, landing).await;
    }

    /// An outcome arrived, maybe for an actor shutdown already gave up on.
    fn late(&mut self, kill: &AktorKillSwitch, index: usize, mut outcome: AktorOutcome) {
        let mut state = kill.control.lock();

        // Something got cut, the group ran past its grace too.
        state.report.ran_past_grace |= outcome.ran_past_grace;

        let position = match self.given_up[index].take() {
            Some(position) => {
                if state.report.actors[position].retirement == crate::AktorRetireStatus::Terminated
                {
                    outcome.retirement = crate::AktorRetireStatus::Terminated;
                }
                outcome.ran_past_grace |= state.report.actors[position].ran_past_grace;
                state.report.actors[position] = outcome;
                Some(position)
            }
            None if !self.finished[index] => {
                state.report.actors.push(outcome);
                Some(state.report.actors.len() - 1)
            }
            None => None,
        };

        if let Some(position) = position
            && state.report.actors[position].retirement == crate::AktorRetireStatus::Unconfirmed
        {
            if let Some(confirmed) = self.confirm_later[index].take() {
                self.confirming.push((position, confirmed));
            } else {
                self.lost_confirmation = true;
            }
        }

        self.finished[index] = true;
    }

    fn give_up(
        &mut self,
        kill: &AktorKillSwitch,
        index: usize,
        (name, kind): &(String, AktorExecution),
        give_up: Option<GiveUp>,
    ) {
        {
            let mut state = kill.control.lock();

            state.report.ran_past_grace = true;
            self.given_up[index] = Some(state.report.actors.len());
            state.report.actors.push(AktorOutcome {
                retirement: crate::AktorRetireStatus::Unconfirmed,
                kind: *kind,
                actor: name.clone(),
                diagnostics: Vec::new(),
                ran_past_grace: true,
            });
        }

        self.finished[index] = true;

        if let Some(give_up) = give_up {
            let mut gone = false;

            contain(|| gone = give_up(), kill, "give up");

            if gone && let Some(position) = self.given_up[index] {
                kill.control.lock().report.actors[position].retirement =
                    crate::AktorRetireStatus::Terminated;
            }
        }
    }
}

pub use crate::panic::panic_message;

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub async fn bounded_standard<F: Future>(deadline: Instant, future: F) -> Option<F::Output> {
    let timer = crate::executor::sleep_until(deadline);
    tokio::pin!(timer, future);
    tokio::select! { biased; output = &mut future => Some(output), _ = &mut timer => None }
}

pub async fn bounded<F: Future>(deadline: Instant, future: F) -> Option<F::Output> {
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

pub async fn finish_hook<Cleanup, CleanupFuture>(
    cleanup: Cleanup,
    report: AktorShutdownReport,
    kill: &AktorKillSwitch,
    deadline: Instant,
) where
    Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), AktorError>>,
{
    #[cfg(not(target_family = "wasm"))]
    let started = {
        kill.control.lock().callback_running = true;
        kill.control.wake.notify_all();
        Instant::now()
    };
    let mut hook = Box::pin(AssertUnwindSafe(async { cleanup(report).await }).catch_unwind());

    match kill.bounded(deadline, &mut hook).await {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(error))) => {
            kill.control.lock().report.application.push(error);
        }
        Some(Err(payload)) => {
            fail_application(kill, "cleanup", panic_message(&payload));

            contain_drop(payload, kill, "cleanup panic drop");
        }
        None => kill.control.lock().report.callback_timed_out(),
    }

    contain_drop(hook, kill, "cleanup drop");

    #[cfg(not(target_family = "wasm"))]
    {
        let mut state = kill.control.lock();
        state.callback_running = false;
        if started < deadline && Instant::now() >= deadline {
            state.report.callback_timed_out();
        }
    }
}

pub fn fail_application(kill: &AktorKillSwitch, phase: &str, message: String) {
    let primary = kill.fail(AktorFailureReport {
        kind: None,
        actor: "application".into(),
        phase: phase.into(),
        message: message.clone(),
    });

    if !primary {
        kill.control
            .lock()
            .report
            .application
            .push(AktorError::new(format!("{phase}: {message}"), ()));
    }
}

pub fn contain_drop(value: impl Sized, kill: &AktorKillSwitch, phase: &str) {
    contain(|| drop(value), kill, phase);
}

pub fn contain(action: impl FnOnce(), kill: &AktorKillSwitch, phase: &str) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(action)) {
        fail_application(kill, phase, panic_message(&payload));

        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            std::mem::forget(payload);
        }
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
        assert!(!complete.wait().await.failed());
    }
}
