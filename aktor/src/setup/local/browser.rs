use super::super::kind::BrowserLocal;
use super::*;
use crate::timeout::browser_sleep;

impl<S, Start, Role> AktorStart for AktorSetup<S, Start, BrowserLocal, Role>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = Handle<S, 0, (), Role, local::clock::Browser>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

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
