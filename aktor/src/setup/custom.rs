use super::*;
use crate::{
    AktorRunner,
    local::{self, clock::AktorGroupClock},
};
use core::ops::AsyncFnOnce;

impl<S: 'static, Start, Clock, Runner, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, kind::Custom<Clock, Runner>, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<
            <kind::Custom<Clock, Runner> as crate::setup::AktorMode>::Each<S>,
        >,
    After: crate::setup::closures::EachHook<
            <kind::Custom<Clock, Runner> as crate::setup::AktorMode>::Each<S>,
        >,
    Clock: AktorGroupClock,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
    Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S>) -> Result<(), (local::RunnerError, String)>
        + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Error = E;
    type Handles = local::Handle<S, (), Clock>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.spawn_driver(self.kind.spawn.clone())
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn closed(name: &str, group: &Self::Group) -> Self::Handles {
        super::driven::closed_handle(name, group)
    }

    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let kind::Custom {
            spawn,
            runner,
            clock,
        } = this.kind;

        let setup = AktorNew {
            name: this.name,
            kind: kind::Custom {
                spawn: spawn.clone(),
                runner: (),
                clock,
            },
            start: this.start,
            end: this.end,
            hooks: AktorHooks {
                intervals: this
                    .hooks
                    .intervals
                    .into_iter()
                    .map(|interval| AktorInterval {
                        every: interval.every,
                        run: interval.run,
                    })
                    .collect(),
                before_each: this.hooks.before_each,
                after_each: this.hooks.after_each,
            },
            capacity: this.capacity,
            stuck: this.stuck,
        };

        super::driven::launch_custom(setup, context, move |future| spawn(future), runner)
    }
}
