use super::start_local;
use crate::{
    AktorNew, AktorNoRole, AktorSetupError,
    local::{self, Handle},
    message::LocalFuture,
    setup::{AktorStart, AktorStartContext, AktorStartError, kind::TokioLocal},
};
use core::ops::AsyncFnOnce;

impl<'a, S, Start, Role> AktorStart for AktorNew<S, Start, TokioLocal<'a>, Role>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = Handle<S, 0, (), Role, local::clock::Tokio>;
    type Startup = LocalFuture<'a, Result<Self::Handles, AktorStartError>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ())
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self.with_role(AktorNoRole);

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
            .map(Handle::with_role)
        })
    }
}
