use super::*;
use crate::local;
use core::ops::AsyncFnOnce;
use kind::BevyLocal;
#[cfg(target_family = "wasm")]
use local::clock::Browser as Clock;
#[cfg(not(target_family = "wasm"))]
use local::clock::Std as Clock;

impl<'pool, S: 'static, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, BevyLocal<'pool>, End, (EM, E), Before, After>
where
    Before:
        crate::setup::closures::EachHook<<BevyLocal<'pool> as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<BevyLocal<'pool> as crate::setup::AktorMode>::Each<S>>,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = local::Handle<S, (), Clock>;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        begin(group)
    }

    fn closed(name: &str, group: &crate::AktorGroup) -> Self::Handles {
        super::local::closed_local_handle(name, group, true)
    }

    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let executor = this.kind.executor;
        let spawn = move |future| executor.spawn_local(future).detach();

        let completion = context.group.completion();
        let (handle, ready) =
            super::local::launch_local::<_, _, _, Clock, _, _, _>(this, context, spawn, |every| {
                use local::clock::AktorGroupClock;
                Clock::wait(Clock::deadline(every))
            })?;

        Ok((handle, Box::pin(super::local::readiness(ready, completion))))
    }
}

impl<'pool, S, Start, Fut, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, kind::BevyTask<'pool>, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<
            <kind::BevyTask<'pool> as crate::setup::AktorMode>::Each<S>,
        >,
    After: crate::setup::closures::EachHook<
            <kind::BevyTask<'pool> as crate::setup::AktorMode>::Each<S>,
        >,
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, X>> + Send + 'static,
    X: Into<(E, String)> + Send + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, EM>,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = crate::AktorTask<S>;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        begin(group)
    }

    fn closed(_name: &str, group: &crate::AktorGroup) -> Self::Handles {
        crate::AktorTask::closed_handle(group.killswitch(), CLOCK)
    }

    fn launch(self, mut context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start_fn, EM);

        let executor = this.kind.executor;

        let (handle, started) =
            crate::task::start_sync_on(this, &mut context.group, CLOCK, |future| {
                crate::serve::TaskJoin::Bevy(executor.spawn(future))
            })?;

        Ok((handle, crate::task::ready(started, context.group)))
    }
}

#[cfg(not(target_family = "wasm"))]
const CLOCK: crate::serve::Clock = crate::serve::Clock::Std;
#[cfg(target_family = "wasm")]
const CLOCK: crate::serve::Clock = crate::serve::Clock::Browser;

fn begin(group: &mut crate::AktorGroup) -> Result<(), (AktorStartReason, String)> {
    #[cfg(not(target_family = "wasm"))]
    let started = group.start_standard();
    #[cfg(target_family = "wasm")]
    let started = group.start();

    started.er_with(|error| error.start_reason())?;
    Ok(())
}
