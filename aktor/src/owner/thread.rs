use super::*;
use crate::{
    AktorCleanupError, AktorExecution, AktorFailureInput, AktorGroup, AktorKillSwitch,
    AktorRetirement, AktorStartReason, AktorStuck, OwnerOutcome,
    executor::Driver,
    message::Message,
    operation::hooks::CallHooks,
    owner_tasks::LocalWork,
    serve::{self, Clock, Interval, Registered, Serve, Served, Started},
};
use core::future::Future;
use tokio::sync::mpsc;

type ThreadStarted<E> = ErResult<Started<E>, DriverErr>;

#[derive(Er)]
pub(crate) struct DriverErr;

pub(crate) struct Launched<S, E> {
    pub actor: Aktor<S>,
    pub started: oneshot::Receiver<ThreadStarted<E>>,
    /// How the thread ended, for a start that failed.
    pub joined: oneshot::Receiver<Result<(), (crate::FailureId, String)>>,
}

#[derive(Er)]
pub(crate) struct LaunchErr {
    pub reason: AktorStartReason,
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn launch<S, E, Start, StartFuture, End, EndFuture>(
    group: &mut AktorGroup,
    name: String,
    capacity: core::num::NonZeroUsize,
    standard: bool,
    stuck: AktorStuck,
    hooks: CallHooks<S>,
    intervals: Vec<Interval<Message<S>>>,
    start: Start,
    end: End,
) -> ErResult<Launched<S, E>, LaunchErr>
where
    S: 'static,
    E: Send + 'static,
    Start: FnOnce() -> StartFuture + Send + 'static,
    StartFuture: Future<Output = Result<S, (E, String)>>,
    End: FnOnce(S) -> EndFuture + Send + 'static,
    EndFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
{
    let kind = match standard {
        true => AktorExecution::StdThread,
        false => AktorExecution::TokioThread,
    };
    let kill = group.killswitch();

    group
        .check_started()
        .er_with(|error| error.start_reason())?;

    let spawner = match group.spawner(standard).er(AktorStartReason::InvalidSetup) {
        Ok(spawner) => spawner,
        Err(error) => {
            kill.fail_startup(AktorFailureInput::actor(
                name,
                kind,
                crate::AktorPhase::Start,
                error.er_report(),
            ));
            return Err(error);
        }
    };

    let (handle, mailbox) = crate::queue::channel(capacity).er(AktorStartReason::InvalidSetup)?;

    handle.admission.manage(kill.clone());
    handle.admission.set_standard(standard);

    let Registered { force, served, .. } =
        serve::register(group, name.clone(), kind, &handle, stuck).er_with(|error| error.reason)?;
    let (owner_served, mut thread_served) = mpsc::unbounded_channel();
    let (completed, result) = watch::channel(None);
    let (cleaned, cleanup) = watch::channel(None);
    let (tell, started) = oneshot::channel();
    let (startup, joined) = oneshot::channel();
    let life = kill.track_thread(name.clone(), stuck);
    let owner = Arc::downgrade(&handle);
    let owner_kill = kill.clone();
    let owner_name = name.clone();
    let (joining, thread) = oneshot::channel();
    let observer_kill = kill.clone();
    let observer_name = name.clone();
    let observer_served = served.clone();

    // Joins the thread as soon as the actor is done, so its completion doesn't wait for the
    // group to stop. The group gets the outcome after the join.
    let observing = spawner.spawn_named("aktor observer", async move {
        let Some(outcome) = thread_served.recv().await else {
            return;
        };
        let Ok(thread) = thread.await else {
            let _ = observer_served.send(outcome);
            return;
        };
        let outcome = ThreadJoin::wait(thread, &observer_kill, observer_name, kind, outcome).await;

        let _ = observer_served.send(Served {
            returned: Vec::new(),
            owner_loss: None,
            outcome,
            panic: None,
            later: Vec::new(),
            cleanup: None,
            cut: false,
        });
    });

    if let Err(error) = observing.er(AktorStartReason::InvalidSetup) {
        never_ran(&served, name, kind, &kill, &error);
        return Err(error);
    }

    let owner_served_outer = owner_served.clone();
    let dedicated = Dedicated::spawn(
        name.clone(),
        Some(life),
        handle.finished.clone(),
        move || {
            let driver = match Driver::new(standard).er(()) {
                Ok(driver) => driver,
                Err(error) => {
                    never_ran(&owner_served, owner_name, kind, &owner_kill, &error);
                    let _ = tell.send(Err(error));
                    return Some(());
                }
            };

            driver.block_on(serve::serve::<S, Message<S>, LocalWork, E, _, _, _, _>(
                Serve {
                    report: serve::Report::new(
                        owner_served,
                        owner_name.clone(),
                        kind,
                        owner_kill.clone(),
                        mailbox.admission.clone(),
                    ),
                    name: owner_name,
                    kind,
                    kill: owner_kill,
                    mailbox,
                    handle: owner,
                    force,
                    start: async move { start().await },
                    end,
                    hooks,
                    intervals,
                    stuck,
                    clock: clock(standard),
                    yields: false,
                    ready: move |started| {
                        let _ = tell.send(Ok(started));
                    },
                    cleaned,
                },
            ));

            Some(())
        },
    );

    let dedicated = match dedicated.er(AktorStartReason::InvalidSetup) {
        Ok(dedicated) => dedicated,
        Err(error) => {
            never_ran(&owner_served_outer, name, kind, &kill, &error);
            return Err(error);
        }
    };

    let _ = joining.send(ThreadJoin {
        thread: dedicated,
        spawner,
        completed,
        startup,
    });

    Ok(Launched {
        actor: Aktor {
            handle: Handle::new(handle),
            completion: OwnerCompletion { cleanup, result },
            kill,
        },
        started,
        joined,
    })
}

fn clock(standard: bool) -> Clock {
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    if standard {
        return Clock::Std;
    }

    let _ = standard;

    #[cfg(feature = "tokio")]
    return Clock::Tokio;

    #[cfg(not(feature = "tokio"))]
    Clock::Std
}

fn never_ran<E: core::error::Error + 'static>(
    served: &mpsc::UnboundedSender<Served>,
    actor: String,
    kind: AktorExecution,
    kill: &AktorKillSwitch,
    error: &ErTree<E>,
) {
    let message = error.er_report();
    kill.fail_startup(AktorFailureInput::actor(
        actor.clone(),
        kind,
        crate::AktorPhase::Start,
        message.clone(),
    ));

    let _ = served.send(Served {
        returned: Vec::new(),
        owner_loss: None,
        outcome: OwnerOutcome {
            retirement: AktorRetirement::Released,
            init: crate::AktorInitEvidence::NeverEntered,
            ran_past_grace: false,
        },
        panic: None,
        later: Vec::new(),
        cleanup: None,
        cut: false,
    });
}

#[derive(Er)]
struct ThreadEscaped;
impl ThreadJoin {
    async fn wait(
        self,
        kill: &AktorKillSwitch,
        name: String,
        kind: AktorExecution,
        served: Served,
    ) -> OwnerOutcome {
        let Served {
            mut returned,
            owner_loss: _,
            mut outcome,
            panic,
            later,
            cleanup,
            cut,
        } = served;
        let joined = self.spawner.join(self.thread).await;
        let escaped = joined.err().map(Arc::new);
        let physically = escaped.as_ref().is_none_or(|cause| cause.physically_joined);

        let join_report = if let Some(cause) = &escaped {
            let error = ErBranch::from(ThreadEscaped)
                .er_add(crate::panic::panic_tree(&cause.payload.lock()));
            let message = error.er_report();
            let failure =
                AktorFailureInput::actor(name, kind, crate::AktorPhase::Join, message.clone());
            let receipt = kill.record_failure(failure, false, |_| {});
            let id = receipt.0;
            returned.push(receipt);

            Err((id, message))
        } else {
            Ok(())
        };
        let _ = self.startup.send(join_report);

        let (cause, join) = match panic {
            Some(payload) => (
                Some(Arc::new(DedicatedJoinError {
                    physically_joined: physically,
                    payload: parking_lot::Mutex::new(payload),
                })),
                escaped,
            ),
            None => (escaped, None),
        };

        let result = completion(cause, join, cleanup, later, cut);
        self.completed.send_replace(Some((result, returned)));

        outcome.retirement = match (physically, outcome.retirement) {
            (false, _) => AktorRetirement::StillRunning,
            (true, AktorRetirement::StillRunning) => AktorRetirement::Terminated,
            (true, retirement @ (AktorRetirement::Released | AktorRetirement::Terminated)) => {
                retirement
            }
        };

        outcome
    }
}

fn completion(
    cause: Option<Arc<DedicatedJoinError>>,
    join: Option<Arc<DedicatedJoinError>>,
    cleanup: Option<Result<(), String>>,
    later: Vec<ErBranch<serve::impls::ServePanic>>,
    cut: bool,
) -> CompletionResult {
    let cut_is_primary = cause.is_none() && matches!(cleanup, Some(Ok(())));
    let mut error = match cause {
        Some(cause) => {
            let top = if matches!(cleanup, Some(Err(_))) || !later.is_empty() {
                OwnerError::PanickedWithCleanup {
                    cause: cause.clone(),
                }
            } else {
                OwnerError::Panicked(cause.clone())
            };
            ErBranch::from(top).er_add(crate::panic::panic_tree(&cause.payload.lock()))
        }
        None => match cleanup {
            Some(Err(report)) if later.is_empty() && !cut => {
                return Err((Arc::new(OwnerError::Cleanup), report));
            }
            Some(Err(_)) => OwnerError::Cleanup.into(),
            None => OwnerError::CleanupUnconfirmed.into(),
            Some(Ok(())) if cut => OwnerError::Cancelled.into(),
            Some(Ok(())) => return Ok(()),
        },
    };

    if let Some(Err(report)) = cleanup {
        error = error.er_add((AktorCleanupError, report));
    }
    for panic in later {
        error = error.er_add(panic);
    }

    if let Some(cause) = join {
        error = error.er_add(
            ErBranch::from(ThreadEscaped).er_add(crate::panic::panic_tree(&cause.payload.lock())),
        );
    }
    if cut && !cut_is_primary {
        error = error.er_add(crate::report::ShutdownCut);
    }

    Err(impls::completion_error(crate::report::unlocated(error)))
}
