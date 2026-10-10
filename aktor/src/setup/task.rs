use super::*;
use crate::{AktorTask, task};
use kind::TokioTask;

impl<S, Start, Fut, Role, End, CM> AktorStart<CM> for AktorNew<S, Start, TokioTask, Role, End>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, crate::AktorSetupError>> + Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
{
    type Group = crate::AktorGroup;
    type Handles = AktorTask<S, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

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
