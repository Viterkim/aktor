use super::start_local;
use crate::timeout::browser_sleep;
use crate::{
    AktorNew,
    local::{self, Handle},
    message::LocalFuture,
    setup::{
        AktorStart, AktorStartCause, AktorStartContext, AktorStartError, initial::InitError,
        kind::BrowserLocal,
    },
};
use core::ops::AsyncFnOnce;

impl<S, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, BrowserLocal, End, (CM, E, M)>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = Handle<S, 0, (), (), local::clock::Browser>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let setup = self;

        Box::pin(async move {
            start_local(
                setup,
                context,
                wasm_bindgen_futures::spawn_local,
                |duration| Box::pin(browser_sleep(duration)),
            )
            .await
        })
    }
}
