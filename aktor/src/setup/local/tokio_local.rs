use super::{closed_local_handle, launch_local, readiness};
use crate::{
    AktorNew,
    local::{self, Handle},
    setup::{AktorLaunch, AktorStart, AktorStartContext, AktorStartReason, kind::TokioLocal},
};
use core::ops::AsyncFnOnce;
use er::*;

impl<'a, S, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, crate::AktorInit<Start>, TokioLocal<'a>, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<TokioLocal<'a> as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<TokioLocal<'a> as crate::setup::AktorMode>::Each<S>>,
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = Handle<S, (), local::clock::Tokio>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.start().er_with(|error| error.start_reason())?;

        Ok(())
    }

    fn closed(name: &str, group: &crate::AktorGroup) -> Self::Handles {
        closed_local_handle(name, group, false)
    }

    fn launch(self, context: AktorStartContext<crate::AktorGroup>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let executor = this.kind.executor;
        let spawn = move |future| {
            executor.spawn_local(future);
        };

        let completion = context.group.completion();
        let (handle, ready) = launch_local(this, context, spawn, |duration| {
            Box::pin(::tokio::time::sleep(duration))
        })?;

        Ok((handle, Box::pin(readiness(ready, completion))))
    }
}
