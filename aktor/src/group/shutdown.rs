use super::*;
use futures_util::{
    FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use std::panic::{AssertUnwindSafe, catch_unwind};

impl AktorGroup {
    pub(super) async fn finish<Cleanup, CleanupFuture, E>(self, cleanup: Cleanup) -> ShutdownReport
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
            names.push((index, entry.name, entry.kind));
            cancellations.push((index, entry.cancel));
            pending.push(async move {
                let mut outcome = entry.outcome.await;
                outcome.kind = Some(entry.kind);
                (index, outcome)
            });
        }

        let drained = async {
            while let Some((index, outcome)) = pending.next().await {
                done[index] = true;
                kill.control.lock().report.actors.push(outcome);
            }
        };

        if kill.bounded(force_at, drained).await.is_none() {
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

            if kill.bounded(settle_at, forced).await.is_none() {
                let mut state = kill.control.lock();

                state.report.timed_out = true;
                state.force_exit = true;

                for (index, name, kind) in names {
                    if !done[index] {
                        state.report.actors.push(ActorOutcome {
                            kind: Some(kind),
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

            let settle_at = deadline.checked_sub(reserve / 2).unwrap_or(deadline);

            if kill.bounded(settle_at, finished).await.is_none() {
                let mut state = kill.control.lock();

                state.report.timed_out = true;
                state.force_exit = true;

                let running = state.running.clone();

                for name in running {
                    if !state.report.actors.iter().any(|actor| actor.actor == *name) {
                        let kind = state.kind(&name);

                        state.report.actors.push(ActorOutcome {
                            kind,
                            actor: (*name).clone(),
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });
                    }
                }
            }
        }

        let before_hook = kill.control.lock().report.clone();
        let hook_deadline = deadline.checked_sub(reserve / 4).unwrap_or(deadline);

        finish_hook(cleanup, before_hook, &kill, hook_deadline).await;

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

pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic payload had no message".into()
    }
}

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

pub async fn finish_hook<Cleanup, CleanupFuture, E>(
    cleanup: Cleanup,
    report: ShutdownReport,
    kill: &KillSwitch,
    deadline: Instant,
) where
    Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
{
    let mut hook = Box::pin(AssertUnwindSafe(async { cleanup(report).await }).catch_unwind());

    match kill.bounded(deadline, &mut hook).await {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(error))) => {
            kill.control.lock().report.application.push(error.report());
            contain_drop(error, kill, "cleanup error drop");
        }
        Some(Err(payload)) => {
            kill.fail(ActorFailure {
                kind: None,
                actor: "application".into(),
                phase: "cleanup".into(),
                message: panic_message(&payload),
            });

            contain_drop(payload, kill, "cleanup panic drop");
        }
        None => kill.control.lock().report.timed_out = true,
    }

    contain_drop(hook, kill, "cleanup drop");
}

pub fn contain_drop(value: impl Sized, kill: &KillSwitch, phase: &str) {
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
            kind: None,
            actor: "application".into(),
            phase: phase.into(),
            message: panic_message(&payload),
        });

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
