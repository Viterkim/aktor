use super::start_local;
use crate::timeout::browser_sleep;
use crate::{
    AktorNew, AktorNoRole, AktorSetupError,
    local::{self, Handle},
    message::LocalFuture,
    setup::{AktorStart, AktorStartContext, AktorStartError, kind::BrowserLocal},
};
use core::ops::AsyncFnOnce;

impl<S, Start, Role, End, CM> AktorStart<CM> for AktorNew<S, Start, BrowserLocal, Role, End>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::AktorGroup;
    type Handles = Handle<S, 0, (), Role, local::clock::Browser>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ())
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            start_local(
                setup,
                context,
                wasm_bindgen_futures::spawn_local,
                |duration| Box::pin(browser_sleep(duration)),
            )
            .await
            .map(Handle::with_role)
        })
    }
}
