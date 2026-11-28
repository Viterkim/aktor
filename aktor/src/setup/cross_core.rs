use super::*;
use crate::{cross_core, local::hooks::CallHooks};

use core::ops::AsyncFnOnce;
use kind::EmbassyCrossCore;

struct NotifyGroupStopped<S>(cross_core::WeakHandle<S>);
impl<S> Drop for NotifyGroupStopped<S> {
    fn drop(&mut self) {
        self.0.group_stopped();
    }
}

impl<S: 'static, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, EmbassyCrossCore, End, (EM, E), Before, After>
where
    Before:
        crate::setup::closures::EachHook<<EmbassyCrossCore as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<EmbassyCrossCore as crate::setup::AktorMode>::Each<S>>,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Error = E;
    type Handles = cross_core::Handle<S>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.spawn_driver(self.kind.spawn.clone())
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn closed(_name: &str, group: &Self::Group) -> Self::Handles {
        let (handle, owner) = cross_core::closed_channel(group.killswitch().registered_id());
        drop(owner);
        handle
    }

    fn launch(self, mut context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let setup = this;
        let capacity = setup.capacity;
        let start = setup.start;
        let end = setup.end;
        let AktorHooks {
            intervals,
            before_each,
            after_each,
        } = setup.hooks;

        let (handle, mut owner) = cross_core::channel(capacity);

        owner.hooks = CallHooks {
            before_each,
            after_each,
            intervals: Vec::new(),
        };
        owner.set_intervals(intervals);
        owner.manage(setup.name.clone(), context.group.killswitch());

        let (sent, initialized) = super::ready::channel();
        let kill = context.group.killswitch();
        let ready = super::ready::startup(
            move || kill.is_startup_aborted(),
            move |result| sent.send(result),
        );
        let ending = context.group.killswitch();
        let startup_kill = context.group.killswitch();
        let startup_name = setup.name.clone();
        let control = handle.control();
        let shutdown = control.clone();
        let cancel = control;
        let completion = handle.completion();

        let watching = NotifyGroupStopped(handle.downgrade());
        let kept = handle.clone();
        let stopping = context.group.killswitch();

        context
            .group
            .register_observed_owner(
                setup.name,
                AktorExecution::EmbassyCrossCore,
                Box::new(move || {
                    shutdown.shutdown();
                }),
                Some(Box::new(move || {
                    cancel.cancel_calls();
                })),
                Box::new(move || completion.owner_status()),
                Box::pin(async move {
                    // The group owns the actor, its handle lives as long as the actor runs.
                    let _kept = kept;
                    let watching = watching;
                    let mut running = core::pin::pin!(async move {
                        owner
                            .run_prepared(
                                async move || match start().await {
                                    Ok(state) => {
                                        ready.send(Ok(()));
                                        Ok(state)
                                    }
                                    Err(error) => {
                                        let mut failure = LaunchError::init(error);
                                        let message = failure.1.clone();
                                        let primary = startup_kill.startup_error(
                                            startup_name,
                                            AktorExecution::EmbassyCrossCore,
                                            message.clone(),
                                        );
                                        failure.0.failure_id = Some(primary);
                                        ready.send(Err(failure));

                                        Err((cross_core::RunErr { id: Some(primary) }, message))
                                    }
                                },
                                async move |state| {
                                    ending.end_within(end.run(state)).await.map(|result| {
                                        match result {
                                            Ok(()) => Ok(()),
                                            Err((_, report)) => {
                                                Err((cross_core::RunErr { id: None }, report))
                                            }
                                        }
                                    })
                                },
                            )
                            .await
                            .err()
                            .filter(|(error, _)| error.id.is_none())
                            .map(|(_, report)| report)
                    });

                    // The group lives on this core, `handle.run(work)` hears about it closing here.
                    // An actor that ends closes its group too, so `run` hears about it either way.
                    let mut watch = core::pin::pin!(async {
                        stopping.wait_stopping().await;
                        watching.0.group_stopped();
                        core::future::pending::<()>().await
                    });

                    core::future::poll_fn(|cx| {
                        let _ = watch.as_mut().poll(cx);
                        running.as_mut().poll(cx)
                    })
                    .await
                }),
            )
            .er_with(|error| LaunchError::new(error.start_reason()))?;

        let waiting = handle.clone();
        let kill = context.group.killswitch();
        let ready: crate::message::LocalFuture<'static, _> = Box::pin(async move {
            if let Some(result) = initialized.await {
                return result;
            }

            if let Some(failure) = kill.driver_loss() {
                return Err(failure);
            }

            if let Err(failure) = waiting.observe_ready().await {
                if waiting.completion().owner_status().ran_past_grace {
                    er_bail!(LaunchError::new(AktorStartReason::Stopping), failure);
                }

                er_bail!(LaunchError::new(AktorStartReason::Init), failure);
            }

            Ok(())
        });

        Ok((handle, ready))
    }
}
