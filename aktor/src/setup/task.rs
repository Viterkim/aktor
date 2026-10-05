use super::*;
use crate::{AktorTask, task};
use kind::TokioTask;

impl<S, Start, Fut, Role> AktorStart for AktorSetup<S, Start, TokioTask, Role>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, AktorSetupError>> + Send + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = AktorTask<S, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ())
    }

    fn start_in(self, mut context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            task::start(setup, &mut context.group)
                .await
                .map(AktorTask::with_role)
        })
    }
}
