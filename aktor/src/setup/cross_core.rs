use super::*;
use crate::AktorError;
use crate::{cross_core, local::hooks::CallHooks};
use core::ops::AsyncFnOnce;
use kind::EmbassyCrossCore;

struct NotifyGroupStopped<S>(cross_core::WeakHandle<S>);
impl<S> Drop for NotifyGroupStopped<S> {
    fn drop(&mut self) {
        self.0.group_stopped();
    }
}

impl<S: 'static, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, EmbassyCrossCore, End, (CM, E, M)>
where
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Error = E::Error;
    type Handles = cross_core::Handle<S>;
    type Startup =
        crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
        .map_err(|report| AktorStartCause::Setup(AktorError::new(report, ())))
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self;

        Box::pin(async move {
            let capacity = setup.capacity;
            let start = setup.start;
            let end = setup.end;
            let AktorHooks {
                intervals,
                before_each,
                after_each,
            } = setup.hooks;

            if intervals.iter().any(|interval| interval.every.is_zero()) {
                return Err(AktorStartError::not_run(validation_error(
                    &context.group,
                    &setup.name,
                    AktorExecution::EmbassyCrossCore,
                    AktorStartCause::Setup(AktorError::new(
                        "interval duration must be positive",
                        (),
                    )),
                )));
            }

            if capacity == 0 {
                return Err(AktorStartError::not_run(validation_error(
                    &context.group,
                    &setup.name,
                    AktorExecution::EmbassyCrossCore,
                    AktorStartCause::Setup(AktorError::new(
                        alloc::format!("{}", crate::message::AktorStateError::InvalidCapacity),
                        (),
                    )),
                )));
            }

            let (handle, mut owner) = cross_core::channel(capacity).map_err(|error| {
                AktorStartError::not_run(AktorStartCause::Setup(AktorError::new(
                    alloc::format!("{error}"),
                    (),
                )))
            })?;

            owner.hooks = CallHooks {
                before_each: before_each.map(|hook| hook.0),
                after_each: after_each.map(|hook| hook.0),
                intervals: Vec::new(),
            };
            owner
                .set_intervals(
                    intervals
                        .into_iter()
                        .map(|interval| (interval.every, interval.run))
                        .collect(),
                )
                .map_err(|error| AktorStartError::not_run(AktorStartCause::Setup(error)))?;
            owner.manage(setup.name.clone(), context.group.killswitch());

            let (ready, initialized) = super::ready::channel();
            let startup_kill = context.group.killswitch();
            let startup_name = setup.name.clone();
            let shutdown = handle.clone();
            let cancel = handle.clone();
            let completion = handle.completion();

            let watching = NotifyGroupStopped(handle.downgrade());
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
                                            let (data, report) = error.split();
                                            let error = startup_kill.startup_error(
                                                startup_name,
                                                AktorExecution::EmbassyCrossCore,
                                                crate::AktorError::new(report, ()),
                                            );
                                            let report = error.clone();

                                            ready.send(Err(AktorStartError {
                                                init: crate::AktorInitOutcome::Failed(data),
                                                cause: AktorStartCause::Init(error),
                                                report: None,
                                            }));
                                            Err(report)
                                        }
                                    },
                                    async move |state| end.run(state).await,
                                    |_| {},
                                )
                                .await
                                .err()
                                .filter(|error| !error.primary)
                                .map(|error| AktorError::new(error.diagnostics, ()))
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
                .map_err(|error| AktorStartError::not_run(AktorStartCause::Group(error)))?;

            if let Some(result) = initialized.await {
                result?;
            } else if let Err((_, report)) = handle.ready().await {
                if handle.completion().owner_status().ran_past_grace {
                    return Err(AktorGroupError::Stopping.into());
                }

                return Err(AktorError::new(report, ()).into());
            }

            Ok(handle)
        })
    }
}
