use super::*;
use crate::{local, message::LocalFuture};
use core::ops::AsyncFnOnce;
use kind::BevyLocal;
#[cfg(target_family = "wasm")]
use local::clock::Browser as Clock;
#[cfg(not(target_family = "wasm"))]
use local::clock::Std as Clock;

impl<'pool, S: 'static, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, BevyLocal<'pool>, End, (CM, E, M)>
where
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = local::Handle<S, 0, (), (), Clock>;
    type Startup = LocalFuture<'pool, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        #[cfg(not(target_family = "wasm"))]
        {
            group
                .start_standard()
                .map(|_| ())
                .map_err(AktorStartCause::group_error)
        }
        #[cfg(target_family = "wasm")]
        {
            group
                .start()
                .map(|_| ())
                .map_err(AktorStartCause::group_error)
        }
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self;

        Box::pin(async move {
            super::local::start_local::<_, _, _, Clock, _, _, _, _>(
                setup,
                context,
                |future| executor.spawn_local(future).detach(),
                |every| {
                    use local::clock::AktorGroupClock;
                    Clock::wait(Clock::deadline(every))
                },
            )
            .await
        })
    }
}
impl<'pool, S, Start, Fut, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, kind::BevyTask<'pool>, End, (CM, E, M)>
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
    type Handles = crate::AktorTask<S>;
    #[cfg(not(target_family = "wasm"))]
    type Startup = Pin<
        Box<
            dyn Future<Output = Result<Self::Handles, AktorStartError<Self::Error>>> + Send + 'pool,
        >,
    >;
    #[cfg(target_family = "wasm")]
    type Startup = LocalFuture<'pool, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        #[cfg(not(target_family = "wasm"))]
        {
            group
                .start_standard()
                .map(|_| ())
                .map_err(AktorStartCause::group_error)
        }
        #[cfg(target_family = "wasm")]
        {
            group
                .start()
                .map(|_| ())
                .map_err(AktorStartCause::group_error)
        }
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let executor = self.kind.executor;
        let setup = self;
        #[cfg(not(target_family = "wasm"))]
        let clock = crate::task::TaskClock::Std;
        #[cfg(target_family = "wasm")]
        let clock = crate::task::TaskClock::Browser;

        Box::pin(async move {
            crate::task::start_on(setup, &mut context.group, clock, |future| {
                crate::task::TaskJoin::Bevy(executor.spawn(future))
            })
            .await
        })
    }
}
