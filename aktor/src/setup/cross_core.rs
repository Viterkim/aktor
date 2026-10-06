use super::*;
use crate::{cross_core, local::hooks::AktorHooks};
use core::ops::AsyncFnOnce;
use kind::EmbassyCrossCore;

impl<S: 'static, Start, Role> AktorStart for AktorSetup<S, Start, EmbassyCrossCore, Role>
where
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::embassy::AktorGroup;
    type Handles = cross_core::Handle<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            let options = setup.options.unwrap_or_default();
            let AktorClosures {
                start,
                mut end,
                intervals,
                before_each,
                after_each,
            } = setup.closures;

            if intervals.iter().any(|interval| interval.every.is_zero()) {
                return Err(AktorStartError::Setup(AktorSetupError::new(
                    "interval duration must be positive",
                )));
            }

            let (handle, mut owner) = cross_core::channel(options.capacity).map_err(|error| {
                AktorStartError::Setup(AktorSetupError::new(alloc::format!("{error}")))
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
                            .run_with(
                                async move || start().await,
                                async move |state| {
                                    if let Some(end) = &mut end {
                                        end.0.run(state).await
                                    } else {
                                        drop(state);
                                        Ok(())
                                    }
                                },
                            )
                            .await
                            .err()
                    }),
                )
                .map_err(|error| {
                    AktorStartError::Setup(AktorSetupError::new(alloc::format!("{error}")))
                })?;
            handle.ready().await.map_err(AktorStartError::Setup)?;
            Ok(handle.with_role::<Role>())
        })
    }
}
