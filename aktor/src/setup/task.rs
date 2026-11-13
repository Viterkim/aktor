use super::*;
use crate::{AktorTask, task};
use kind::TokioTask;

impl<S, Start, Fut, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, TokioTask, End, (CM, E, M)>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = AktorTask<S>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, mut context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let setup = self;

        Box::pin(async move { task::start(setup, &mut context.group).await })
    }
}
