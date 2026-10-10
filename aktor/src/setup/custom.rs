use super::*;
use crate::{
    AktorError, AktorRunner,
    local::{self, clock::AktorGroupClock},
    message::LocalFuture,
};
use core::ops::AsyncFnOnce;

impl<S: 'static, Start, Clock, Runner, Role, End, CM> AktorStart<CM>
    for AktorNew<S, Start, kind::Custom<Clock, Runner>, Role, End>
where
    Clock: AktorGroupClock,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
    Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, 0, ()>) -> Result<(), AktorError> + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Handles = local::Handle<S, 0, (), Role, Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

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

        let setup = AktorNew {
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
