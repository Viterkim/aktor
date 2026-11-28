use super::{closed_local_handle, launch_local};
use crate::timeout::browser_sleep;
use crate::{
    AktorNew,
    local::{self, Handle},
    setup::{AktorLaunch, AktorStart, AktorStartContext, AktorStartReason, kind::BrowserLocal},
};
use core::ops::AsyncFnOnce;
use er::*;

impl<S, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, crate::AktorInit<Start>, BrowserLocal, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<BrowserLocal as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<BrowserLocal as crate::setup::AktorMode>::Each<S>>,
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = Handle<S, (), local::clock::Browser>;

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

        let completion = context.group.completion();
        let (handle, ready) = launch_local(
            this,
            context,
            wasm_bindgen_futures::spawn_local,
            |duration| Box::pin(browser_sleep(duration)),
        )?;
        Ok((handle, Box::pin(super::readiness(ready, completion))))
    }
}
