use super::*;
use crate::{AktorTask, task};
use kind::TokioTask;

impl<S, Start, Fut, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, TokioTask, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<TokioTask as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<TokioTask as crate::setup::AktorMode>::Each<S>>,
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, X>> + Send + 'static,
    X: Into<(E, String)> + Send + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, EM>,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = AktorTask<S>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group
            .start_threaded()
            .er_with(|error| error.start_reason())?;

        Ok(())
    }

    fn closed(_name: &str, group: &crate::AktorGroup) -> Self::Handles {
        AktorTask::closed_handle(group.killswitch(), crate::serve::Clock::Tokio)
    }

    fn launch(self, mut context: AktorStartContext<crate::AktorGroup>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start_fn, EM);

        let (handle, started) = task::start_sync(this, &mut context.group)?;
        let ready = task::ready(started, context.group);

        Ok((handle, ready))
    }
}
