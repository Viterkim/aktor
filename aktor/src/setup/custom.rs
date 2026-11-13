use super::*;
use crate::{
    AktorError, AktorRunner,
    local::{self, clock::AktorGroupClock},
    message::LocalFuture,
};
use core::ops::AsyncFnOnce;

impl<S: 'static, Start, Clock, Runner, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, kind::Custom<Clock, Runner>, End, (CM, E, M)>
where
    Clock: AktorGroupClock,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
    Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, 0, ()>) -> Result<(), AktorError> + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Error = E::Error;
    type Handles = local::Handle<S, 0, (), (), Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
        .map_err(|report| AktorStartCause::Setup(AktorError::new(report, ())))
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let kind::Custom {
            spawn,
            runner,
            clock,
        } = self.kind;

        let setup = AktorNew {
            name: self.name,
            kind: kind::Custom {
                spawn: spawn.clone(),
                runner: (),
                clock,
            },
            start: self.start,
            end: self.end,
            hooks: AktorHooks {
                intervals: self
                    .hooks
                    .intervals
                    .into_iter()
                    .map(|interval| AktorInterval {
                        every: interval.every,
                        run: interval.run,
                    })
                    .collect(),
                before_each: self.hooks.before_each,
                after_each: self.hooks.after_each,
            },
            capacity: self.capacity,
            stuck: self.stuck,
        };

        Box::pin(async move {
            super::driven::start_custom(
                setup,
                context,
                move |future| spawn(future).map_err(|report| AktorError::new(report, ())),
                runner,
            )
            .await
        })
    }
}
