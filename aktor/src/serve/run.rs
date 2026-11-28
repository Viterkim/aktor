use super::*;
use crate::{
    AktorInitEvidence, AktorPhase, AktorRetirement,
    owner_tasks::{Spawned, Work, scoped},
    panic::panic_tree,
    queue::{
        Admission,
        mailbox::{self, Received},
    },
};
use core::{future::poll_fn, pin::pin};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

enum Ran {
    Call(Result<(), Box<dyn Any + Send>>),
    Spawned(Box<dyn Any + Send>),
    Forced,
}

/// Everything the actor holds. When a runtime drops the actor mid-way, each piece drops on its
/// own with its panic contained, one bad drop can't take the process down with the next.
struct Parts<S, M: Mail<S>, W: Work, End> {
    actor: String,
    kind: crate::AktorExecution,
    kill: AktorKillSwitch,
    receiver: mailbox::Receiver<M>,
    hooks: CallHooks<S>,
    intervals: Vec<Interval<M>>,
    spawned: Option<Spawned<W>>,
    state: Option<M::State>,
    end: Option<End>,
}
impl<S, M: Mail<S>, W: Work, End> Drop for Parts<S, M, W, End> {
    fn drop(&mut self) {
        use crate::group::shutdown::contain_with;
        let source = || crate::AktorFailureSource::Actor {
            id: (),
            name: self.actor.clone(),
            kind: self.kind,
            phase: AktorPhase::Drop,
        };

        contain_with(|| self.receiver.close(), &self.kill, source);
        for message in self.receiver.take_queued() {
            contain_with(|| drop(message), &self.kill, source);
        }
        for interval in self.intervals.drain(..) {
            contain_with(|| drop(interval), &self.kill, source);
        }

        contain_with(|| drop(self.spawned.take()), &self.kill, source);
        contain_with(|| drop(self.hooks.before_each.take()), &self.kill, source);
        contain_with(|| drop(self.hooks.after_each.take()), &self.kill, source);
        contain_with(|| drop(self.end.take()), &self.kill, source);
        contain_with(|| drop(self.state.take()), &self.kill, source);
    }
}

#[derive(Er)]
struct EndTimeout;
pub(crate) async fn serve<S, M, W, E, Start, End, EndFuture, Ready>(
    serve: Serve<S, M, Start, End, Ready>,
) where
    M: Mail<S> + Send + 'static,
    W: Work,
    E: 'static,
    Start: Future<Output = Result<M::State, (E, String)>>,
    End: FnOnce(M::State) -> EndFuture,
    EndFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
    Ready: FnOnce(Started<E>),
{
    let Serve {
        name,
        kind,
        kill,
        mailbox,
        handle,
        force,
        start,
        end,
        hooks,
        intervals,
        stuck,
        clock,
        yields,
        ready,
        cleaned,
        mut report,
    } = serve;
    let Mailbox {
        receiver,
        jobs,
        admission,
        finished,
    } = mailbox;

    let retire = {
        let queue = receiver.retirer();
        let admission = admission.clone();
        let kill = kill.clone();

        Arc::new(move || {
            kill.retire_self();
            admission.close(&queue);
        })
    };
    let owner = jobs.owner.get().expect("registered actor").clone();
    let spawned = Spawned::<W>::new(admission.clone(), jobs, owner.clone(), retire);
    let context = spawned.context();

    let mut parts = Parts {
        actor: name.clone(),
        kind,
        kill: kill.clone(),
        receiver,
        hooks,
        intervals,
        spawned: Some(spawned),
        state: None,
        end: Some(end),
    };

    scoped(context, async move {
        let mut ready = Some(ready);

        let mut start_phase = AktorPhase::Start;
        report.outcome.init = AktorInitEvidence::Entered;
        kill.init_evidence(AktorInitEvidence::Entered);
        let started = {
            let mut start = Box::pin(AssertUnwindSafe(start).catch_unwind());
            let mut cut = force.clone();
            let forced = async {
                tokio::select! {
                    _ = cut.wait_for(|forced| *forced) => {},
                    () = clock.wait_for_force(&kill) => {},
                }
            };
            let started = match parts.spawned.as_mut() {
                Some(spawned) => tokio::select! {
                    biased;
                    started = &mut start => Some(started),
                    payload = spawned.serve() => {
                        start_phase = AktorPhase::Background;
                        Some(Err(payload))
                    },
                    () = forced, if stuck == AktorStuck::Kill => None,
                },
                None => None,
            };

            report.dispose(start);
            started
        };

        let mut start_failed = |report: &mut Report,
                                phase,
                                mut failure: ErTree<crate::LaunchError<E>>,
                                previous: Option<crate::Delivery>| {
            let message = failure.er_report();
            let receipt = if let Some(receipt) = previous {
                tell(&mut ready, Started::Init(failure));
                receipt
            } else {
                report.fail_startup(phase, message.clone(), |id| {
                    failure.top.failure_id = Some(id);
                    tell(&mut ready, Started::Init(failure));
                })
            };
            cleaned.send_replace(Some(Err((
                CleanupError::Unconfirmed(Some(receipt.0)),
                receipt,
                message,
            ))));
        };

        match started {
            Some(Ok(Ok(state))) => {
                owner
                    .starting
                    .store(false, core::sync::atomic::Ordering::Release);
                report.outcome.init = AktorInitEvidence::Ready;
                kill.init_evidence(AktorInitEvidence::Ready);
                parts.state = Some(state);
            }
            Some(Ok(Err(error))) => {
                report.outcome.retirement = AktorRetirement::Released;

                let (data, text) = error;
                let failure = er_new!(
                    crate::LaunchError {
                        init: Some(data),
                        ..crate::LaunchError::new(crate::AktorStartReason::Init)
                    }
                    .er_save("report", text)
                );
                start_failed(&mut report, AktorPhase::Start, failure, None);
            }
            Some(Err(payload)) => {
                crate::panic::each(payload, |payload, secondary| {
                    if secondary {
                        report.panicked(AktorPhase::Drop, payload);
                        return;
                    }
                    if payload.is::<crate::owner_tasks::JobPanic>() {
                        let receipt = report.panicked(AktorPhase::Background, payload);
                        let error = crate::LaunchError {
                            failure_id: Some(receipt.0),
                            ..crate::LaunchError::new(crate::AktorStartReason::Init)
                        };
                        let failure = er_new!(error.er_save("job", receipt.report()));
                        start_failed(&mut report, AktorPhase::Background, failure, Some(receipt));
                        return;
                    }

                    let failure =
                        ErBranch::from(crate::LaunchError::new(crate::AktorStartReason::Init))
                            .er_add(panic_tree(&payload));
                    start_failed(
                        &mut report,
                        start_phase.clone(),
                        crate::report::unlocated(failure),
                        None,
                    );
                    report.dispose(payload);
                });
            }
            None => {
                report.outcome.ran_past_grace = true;
                admission.cut_jobs(&owner);
                tell(&mut ready, Started::Cut);
            }
        }

        let Some(state) = parts.state.as_mut() else {
            discard(&mut report, &admission, &mut parts.receiver, false);
            let_go(&mut report, parts);
            gone(&mut report, &admission, cleaned, finished);
            report.finish();
            return;
        };

        let started = if kill.is_startup_aborted() {
            Started::Cut
        } else {
            Started::Ready
        };
        report.capture(AktorPhase::Start, || tell(&mut ready, started));

        let force_now = force;
        let mut draining = false;
        let mut forced = false;
        let mut budget = 32_u8;

        // One catch around the whole loop instead of one per call. A cut call drops in the
        // middle of its work, a panic there lands here and `end` still runs.
        let serving = AssertUnwindSafe(async {
            let Some(spawned) = parts.spawned.as_mut() else {
                return;
            };

            for interval in parts.intervals.drain(..) {
                spawned.start(W::boxed(super::intervals::tick(
                    handle.clone(),
                    interval,
                    clock,
                )));
            }

            loop {
                // Plain reads, no waker. A stream of ready calls still sees a force or a stop on
                // the next turn, an empty mailbox gets nudged for them.
                if stuck == AktorStuck::Kill && *force_now.borrow() {
                    forced = true;
                    break;
                }

                if !draining && admission.is_closed() {
                    close(&mut report, &admission, &mut parts.receiver);
                    draining = true;
                }

                let mut message = tokio::select! {
                    biased;
                    payload = spawned.serve() => {
                        report.panicked(AktorPhase::Background, payload);
                        break;
                    },
                    next = parts.receiver.next() => match next {
                        Received::Message(message) => message,
                        Received::Closed => break,
                        Received::Nudged => continue,
                    },
                };

                let operation = message.operation();

                let mut call =
                    pin!(AssertUnwindSafe(message.run(state, &mut parts.hooks)).catch_unwind());

                // The call goes first, so work it spawns gets its first turn in the same poll. A
                // stop during a long call closes the mailbox right away, callers waiting for room
                // hear it now.
                let ran = loop {
                    tokio::select! {
                        biased;
                        ran = &mut call => break Ran::Call(ran),
                        payload = spawned.serve() => break Ran::Spawned(payload),
                        () = poll_fn(|cx| parts.receiver.poll_nudge(cx)) => {
                            if stuck == AktorStuck::Kill && *force_now.borrow() {
                                break Ran::Forced;
                            }

                            if !draining && admission.is_closed() {
                                close(&mut report, &admission, &mut parts.receiver);
                                draining = true;
                            }
                        },
                    }
                };

                match ran {
                    Ran::Call(Ok(())) => {}
                    Ran::Call(Err(payload)) => {
                        report.panicked(operation.phase(), payload);
                        break;
                    }
                    Ran::Spawned(payload) => {
                        report.panicked(AktorPhase::Background, payload);
                        break;
                    }
                    Ran::Forced => {
                        report.cut();
                        report.outcome.ran_past_grace = true;
                        forced = true;
                        break;
                    }
                }

                if yields {
                    budget -= 1;

                    if budget == 0 {
                        budget = 32;
                        clock::yield_owner().await;
                    }
                }
            }
        })
        .catch_unwind()
        .await;

        if let Err(payload) = serving {
            report.panicked(AktorPhase::Drop, payload);
        }

        // Cancelling between two calls throws the queued ones away, those were admitted too.
        // `retire` drops its queue on purpose, those callers settle as Discarded.
        discard(&mut report, &admission, &mut parts.receiver, forced);

        if let Some(spawned) = parts.spawned.as_mut() {
            spawned.stop();
            let mut force = force_now;
            loop {
                tokio::select! {
                    biased;
                    _ = force.wait_for(|cut| *cut), if stuck == AktorStuck::Kill => {
                        break;
                    },
                    drained = spawned.drain() => match drained {
                        Some(payload) => { report.panicked(AktorPhase::Background, payload); }
                        None => break,
                    },
                }
            }
        }
        report.dispose(parts.spawned.take());

        let (Some(state), Some(end)) = (parts.state.take(), parts.end.take()) else {
            crate::message::consumed();
        };
        let mut ending = Box::pin(AssertUnwindSafe(async move { end(state).await }).catch_unwind());
        let ended = match stuck {
            AktorStuck::Kill => clock.end_within(&kill, &mut ending).await,
            AktorStuck::Leave => Some((&mut ending).await),
        };

        report.dispose(ending);

        match ended {
            Some(Ok(Ok(()))) => {
                report.outcome.retirement = AktorRetirement::Released;
                report.cleanup = Some(Ok(()));
                cleaned.send_replace(Some(Ok(())));
            }
            Some(Ok(Err((_, failure)))) => {
                report.outcome.retirement = AktorRetirement::Released;

                let delivery = report.failed(AktorPhase::End, failure.clone());
                report.returned.push(delivery.clone());
                report.cleanup = Some(Err(failure.clone()));
                cleaned.send_replace(Some(Err((
                    CleanupError::Failed(delivery.0),
                    delivery,
                    failure,
                ))));
            }
            Some(Err(payload)) => {
                let message = panic_tree(&payload).er_report();
                let delivery = report.panicked(AktorPhase::End, payload);
                cleaned.send_replace(Some(Err((
                    CleanupError::Unconfirmed(Some(delivery.0)),
                    delivery,
                    message,
                ))));
            }
            None => {
                report.outcome.ran_past_grace = true;
                let message = er_new!(EndTimeout).er_report();
                let delivery = report.failed(AktorPhase::End, message.clone());
                cleaned.send_replace(Some(Err((
                    CleanupError::Unconfirmed(Some(delivery.0)),
                    delivery,
                    message,
                ))));
            }
        }

        let_go(&mut report, parts);

        gone(&mut report, &admission, cleaned, finished);
        report.finish();
    })
    .await;
}

fn let_go<S, M: Mail<S>, W: Work, End>(report: &mut Report, mut parts: Parts<S, M, W, End>) {
    report.dispose(parts.spawned.take());
    report.dispose(parts.hooks.before_each.take());
    report.dispose(parts.hooks.after_each.take());

    report.dispose(parts.end.take());
    report.dispose(parts.state.take());
    report.dispose(parts);
}

fn gone(
    report: &mut Report,
    admission: &Admission,
    cleaned: watch::Sender<Option<crate::serve::EndReceipt>>,
    finished: watch::Sender<bool>,
) {
    if let Some(receipt) = admission.lost.get().or_else(|| report.returned.first()) {
        let _ = admission.terminal.set(receipt.clone());
    }
    let failed = report.went_wrong() || admission.terminal.get().is_some();

    report.capture(AktorPhase::Drop, || {
        if failed {
            finished.send_replace(true);
        }

        drop((cleaned, finished));
    });
}

fn tell<E, Ready: FnOnce(Started<E>)>(ready: &mut Option<Ready>, started: Started<E>) {
    if let Some(ready) = ready.take() {
        ready(started);
    }
}

fn close<M>(report: &mut Report, admission: &Admission, receiver: &mut mailbox::Receiver<M>) {
    report.capture(AktorPhase::Drop, || admission.shutdown());
    report.capture(AktorPhase::Drop, || receiver.close());
}

fn discard<M>(
    report: &mut Report,
    admission: &Admission,
    receiver: &mut mailbox::Receiver<M>,
    forced: bool,
) {
    close(report, admission, receiver);

    let queued = receiver.take_queued();
    if forced && !queued.is_empty() {
        report.cut();
        report.outcome.ran_past_grace = true;
    }

    for message in queued {
        report.dispose(message);
    }
}
