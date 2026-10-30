use super::*;
use crate::AktorError;
use crate::{cross_core, local::hooks::AktorHooks};
use core::ops::AsyncFnOnce;
use kind::EmbassyCrossCore;

impl<S: 'static, Start, Role, End, CM> AktorStart<CM>
    for AktorNew<S, Start, EmbassyCrossCore, Role, End>
where
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Handles = cross_core::Handle<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            let options = setup.options;
            let AktorClosures {
                start,
                mut end,
                intervals,
                before_each,
                after_each,
            } = setup.closures;

            if intervals.iter().any(|interval| interval.every.is_zero()) {
                return Err(validation_error(
                    &context.group,
                    &setup.name.name,
                    AktorExecution::EmbassyCrossCore,
                    AktorStartError::Setup(AktorSetupError::new(
                        "interval duration must be positive",
                        (),
                    )),
                ));
            }

            if options.capacity == 0 {
                return Err(validation_error(
                    &context.group,
                    &setup.name.name,
                    AktorExecution::EmbassyCrossCore,
                    AktorStartError::Setup(AktorSetupError::new(
                        alloc::format!("{}", crate::message::AktorStateError::InvalidCapacity),
                        (),
                    )),
                ));
            }

            let (handle, mut owner) = cross_core::channel(options.capacity).map_err(|error| {
                AktorStartError::Setup(AktorSetupError::new(alloc::format!("{error}"), ()))
            })?;

            owner.hooks = AktorHooks {
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
                .map_err(AktorStartError::Setup)?;
            owner.manage(setup.name.name.clone(), context.group.killswitch());

            let (ready, initialized) = super::ready::channel();
            let startup_kill = context.group.killswitch();
            let startup_name = setup.name.name.clone();
            let shutdown = handle.new_handle();

            context
                .group
                .register_owner(
                    setup.name.name,
                    AktorExecution::EmbassyCrossCore,
                    Box::new(move || {
                        shutdown.shutdown();
                    }),
                    Box::pin(async move {
                        owner
                            .run_prepared(
                                async move || match start().await {
                                    Ok(state) => {
                                        ready.send(Ok(()));
                                        Ok(state)
                                    }
                                    Err(error) => {
                                        let error = startup_kill.startup_error(
                                            startup_name,
                                            AktorExecution::EmbassyCrossCore,
                                            error,
                                        );
                                        let report = error.clone();

                                        ready.send(Err(AktorStartError::Init(error)));
                                        Err(report)
                                    }
                                },
                                async move |state| end.run(state).await,
                            )
                            .await
                            .err()
                            .filter(|error| !error.primary)
                            .map(|error| AktorError::new(error.diagnostics, ()))
                    }),
                )
                .map_err(|error| {
                    AktorStartError::Setup(AktorSetupError::new(alloc::format!("{error}"), ()))
                })?;
            initialized.await.ok_or_else(|| {
                AktorStartError::Setup(AktorSetupError::new(
                    "cross core actor stopped during startup",
                    (),
                ))
            })??;
            Ok(handle.with_role::<Role>())
        })
    }
}
