use super::*;
use crate::{local, message::LocalFuture};
use core::ops::AsyncFnOnce;
use kind::BevyLocal;
#[cfg(target_family = "wasm")]
use local::clock::Browser as Clock;
#[cfg(not(target_family = "wasm"))]
use local::clock::Std as Clock;

impl<'pool, S: 'static, Start, Role> AktorStart for AktorSetup<S, Start, BevyLocal<'pool>, Role>
where
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = local::Handle<S, 0, (), Role, Clock>;
    type Startup = LocalFuture<'pool, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        #[cfg(not(target_family = "wasm"))]
        {
            group.start_standard().map(|_| ())
        }
        #[cfg(target_family = "wasm")]
        {
            group.start().map(|_| ())
        }
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            super::local::start_local::<_, _, _, Clock>(
                setup,
                context,
                |future| executor.spawn_local(future).detach(),
                |every| {
                    use local::clock::AktorGroupClock;
                    Clock::wait(Clock::deadline(every))
                },
            )
            .await
            .map(local::Handle::with_role)
        })
    }
}

impl<'pool, S, Start, Fut, Role> AktorStart for AktorSetup<S, Start, kind::BevyTask<'pool>, Role>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, AktorSetupError>> + Send + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = crate::AktorTask<S, Role>;
    #[cfg(not(target_family = "wasm"))]
    type Startup =
        Pin<Box<dyn Future<Output = Result<Self::Handles, AktorStartError>> + Send + 'pool>>;
    #[cfg(target_family = "wasm")]
    type Startup = LocalFuture<'pool, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        #[cfg(not(target_family = "wasm"))]
        {
            group.start_standard().map(|_| ())
        }
        #[cfg(target_family = "wasm")]
        {
            group.start().map(|_| ())
        }
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self.with_role(AktorNoRole);
        #[cfg(not(target_family = "wasm"))]
        let clock = crate::task::TaskClock::Std;
        #[cfg(target_family = "wasm")]
        let clock = crate::task::TaskClock::Browser;

        Box::pin(async move {
            crate::task::start_on(setup, &mut context.group, clock, |future| {
                crate::task::TaskJoin::Bevy(executor.spawn(future))
            })
            .await
            .map(crate::AktorTask::with_role)
        })
    }
}
