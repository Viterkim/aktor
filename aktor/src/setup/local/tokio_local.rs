use super::start_local;
use crate::{
    AktorNew,
    local::{self, Handle},
    message::LocalFuture,
    setup::{
        AktorStart, AktorStartCause, AktorStartContext, AktorStartError, initial::InitError,
        kind::TokioLocal,
    },
};
use core::ops::AsyncFnOnce;

impl<'a, S, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, TokioLocal<'a>, End, (CM, E, M)>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = Handle<S, 0, (), (), local::clock::Tokio>;
    type Startup = LocalFuture<'a, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self;

        Box::pin(async move {
            start_local(
                setup,
                context,
                move |future| {
                    executor.spawn_local(future);
                },
                |duration| Box::pin(::tokio::time::sleep(duration)),
            )
            .await
        })
    }
}
