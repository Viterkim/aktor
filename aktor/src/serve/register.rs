use super::*;
use crate::{
    AktorFailureInput, AktorGroup, AktorRetirement, AktorStartReason, group::shutdown::contain,
    panic::panic_tree,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
use tokio::sync::oneshot;

pub(crate) struct Registered {
    pub force: watch::Receiver<bool>,
    pub served: mpsc::UnboundedSender<Served>,
    #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
    pub join: oneshot::Sender<Join>,
}

/// How the group makes sure a task is gone after its actor reported. A thread host joins its
/// thread on its own, before it reports.
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub struct Join {
    pub task: TaskJoin,
    /// The task's state. A lease someone kept can hold it past the task.
    pub state: Weak<dyn Any + Send + Sync>,
}

#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub enum TaskJoin {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(feature = "bevy")]
    Bevy(bevy_tasks::Task<()>),
}

#[derive(Er)]
pub(crate) struct RegisterErr {
    pub reason: AktorStartReason,
}
pub(crate) fn register<M: Send + 'static>(
    group: &mut AktorGroup,
    name: String,
    kind: AktorExecution,
    handle: &Arc<HandleInner<M>>,
    stuck: AktorStuck,
) -> ErResult<Registered, RegisterErr> {
    let (forcing, force) = watch::channel(false);
    let (served, mut outcome) = mpsc::unbounded_channel();
    #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
    let (join, joined) = oneshot::channel::<Join>();
    let closing = handle.clone();
    let cancelled = handle.clone();
    let kill = group.killswitch();
    let jobs = handle.jobs.upgrade().er(|| RegisterErr {
        reason: AktorStartReason::InvalidSetup,
    })?;
    let job_owner = Arc::new(crate::owner_tasks::JobOwner {
        starting: true.into(),
        admission: Arc::downgrade(&handle.admission),
        name: name.clone(),
        kind,
        kill: kill.clone(),
    });
    let _ = jobs.owner.set(job_owner.clone());
    let starting = kill.clone();
    let cancelling = kill.clone();
    let label = name.clone();

    // Waking a waiting caller runs its waker, which can panic. That's the caller's failure, the
    // group's shutdown goes on.
    group
        .register_owner(
            name,
            kind,
            Box::new(move || {
                contain(
                    || closing.request_close(),
                    &starting,
                    crate::AktorFailureSource::ApplicationTask,
                );
            }),
            Box::new(move || {
                let source = crate::AktorFailureSource::ApplicationTask;
                if stuck == AktorStuck::Kill {
                    contain(
                        || cancelled.admission.cut_jobs(&job_owner),
                        &cancelling,
                        source.clone(),
                    );
                    contain(
                        || {
                            forcing.send_replace(true);
                        },
                        &cancelling,
                        source.clone(),
                    );
                }
                contain(|| cancelled.sender.nudge(), &cancelling, source);
            }),
            None,
            None,
            Box::pin(async move {
                let (outcome, panic, owner_loss) = match outcome.recv().await {
                    Some(Served {
                        returned,
                        owner_loss,
                        outcome,
                        panic,
                        later,
                        cleanup,
                        cut,
                    }) => {
                        let _ = (returned, later, cleanup, cut);
                        (outcome, panic, owner_loss)
                    }
                    None => {
                        let id = kill.fail(AktorFailureInput::actor(
                            label.clone(),
                            kind,
                            crate::AktorPhase::Join,
                            er_new!(ObserveErr::Stopped).er_report(),
                        ));
                        (
                            OwnerOutcome {
                                retirement: AktorRetirement::StillRunning,
                                init: crate::AktorInitEvidence::Unknown,
                                ran_past_grace: false,
                            },
                            None,
                            Some(id),
                        )
                    }
                };

                #[cfg(not(any(feature = "tokio", feature = "bevy")))]
                let _ = owner_loss;

                // A thread keeps the panic for its completion, nobody else needs it.
                if let Some(payload) = panic
                    && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload)))
                {
                    failed(
                        &kill,
                        &label,
                        kind,
                        crate::AktorPhase::Drop,
                        ErBranch::from(ObserveErr::Drop)
                            .er_add(panic_tree(&payload))
                            .er_report(),
                    );
                    crate::panic::dispose_secondary(payload);
                }

                #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
                if let Ok(join) = joined.await {
                    return join.wait(&kill, &label, kind, outcome, owner_loss).await;
                }

                outcome
            }),
        )
        .er_with(|err| err.start_reason())?;

    Ok(Registered {
        force,
        served,
        #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
        join,
    })
}

#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
impl Join {
    async fn wait(
        self,
        kill: &AktorKillSwitch,
        name: &str,
        kind: AktorExecution,
        mut outcome: OwnerOutcome,
        owner_loss: Option<crate::FailureId>,
    ) -> OwnerOutcome {
        match self.task {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            TaskJoin::Tokio(join) => {
                if let Err(error) = join.await
                    && !(error.is_cancelled() && owner_loss.is_some())
                {
                    let error = er_new!(ObserveErr::Join, error);
                    failed(kill, name, kind, crate::AktorPhase::Join, error.er_report());
                    crate::group::shutdown::contain_drop(
                        error,
                        kill,
                        crate::AktorFailureSource::Actor {
                            id: (),
                            name: name.into(),
                            kind,
                            phase: crate::AktorPhase::Drop,
                        },
                    );
                }
            }
            #[cfg(feature = "bevy")]
            TaskJoin::Bevy(join) => {
                use futures_util::FutureExt;

                let joined = std::panic::AssertUnwindSafe(join).catch_unwind().await;

                if let Err(payload) = joined {
                    if owner_loss.is_none() {
                        failed(
                            kill,
                            name,
                            kind,
                            crate::AktorPhase::Join,
                            ErBranch::from(ObserveErr::Join)
                                .er_add(panic_tree(&payload))
                                .er_report(),
                        );
                    }
                    crate::group::shutdown::contain_drop(
                        payload,
                        kill,
                        crate::AktorFailureSource::Actor {
                            id: (),
                            name: name.into(),
                            kind,
                            phase: crate::AktorPhase::Drop,
                        },
                    );
                }
            }
        }

        // The task is gone, and so is whatever `end` didn't release, unless a lease still holds
        // the state.
        if outcome.retirement == AktorRetirement::StillRunning && self.state.strong_count() == 0 {
            outcome.retirement = AktorRetirement::Terminated;
        }

        outcome
    }
}

#[derive(Er)]
enum ObserveErr {
    Stopped,
    Drop,
    #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
    Join,
}

fn failed(
    kill: &AktorKillSwitch,
    name: &str,
    kind: AktorExecution,
    phase: crate::AktorPhase,
    message: String,
) {
    kill.fail(AktorFailureInput::actor(name.into(), kind, phase, message));
}
