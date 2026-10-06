use super::super::kind::TokioLocal;
use super::*;

impl<'a, S, Start, Role> AktorStart for AktorSetup<S, Start, TokioLocal<'a>, Role>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = Handle<S, 0, (), Role, local::clock::Tokio>;
    type Startup = LocalFuture<'a, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
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
