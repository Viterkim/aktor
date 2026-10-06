use super::*;
use crate::{
    AktorError, AktorRunner,
    local::{self, clock::AktorGroupClock},
    message::LocalFuture,
};
use core::ops::AsyncFnOnce;

impl<S: 'static, Start, Clock, Runner, Role> AktorStart
    for AktorSetup<S, Start, kind::Custom<Clock, Runner>, Role>
where
    Clock: AktorGroupClock,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
    Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S>) -> Result<(), AktorError> + 'static,
{
    type Error = AktorStartError;
    type Group = local::AktorGroup<Clock>;
    type Handles = local::Handle<S, 0, (), Role, Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

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

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let kind::Custom {
            spawn,
            runner,
            clock,
        } = self.kind;

        let AktorClosures {
            start,
            end,
            intervals,
            before_each,
            after_each,
        } = self.closures;

        let setup = AktorSetup {
            name: self.name,
            role: AktorNoRole,
            kind: kind::Custom {
                spawn: spawn.clone(),
                runner: (),
                clock,
            },
            closures: AktorClosures {
                start,
                end,
                intervals: intervals
                    .into_iter()
                    .map(|interval| AktorInterval {
                        every: interval.every,
                        run: interval.run,
                    })
                    .collect(),
                before_each,
                after_each,
            },
            options: self.options,
        };

        Box::pin(async move {
            super::driven::start_custom(setup, context, move |future| spawn(future), runner)
                .await
                .map(local::Handle::with_role)
        })
    }
}
