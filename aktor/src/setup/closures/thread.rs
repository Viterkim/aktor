use super::AktorEndFuture;
use crate::AktorCleanupError;
use alloc::boxed::Box;
use alloc::string::String;
use core::{future::Future, marker::PhantomData};

// Which kind of `end` a setup has. They're only types, `AktorNew` infers one from its `end`.
// The error goes into the marker so an `end` returning anything that turns into `(E, String)`,
// like an ErResult, still pins down its E.
#[doc(hidden)]
pub struct AktorNoCleanup;
#[doc(hidden)]
pub struct AktorCleanupCallback<E>(PhantomData<E>);
#[doc(hidden)]
pub struct AktorCleanupCallbackUnit;
#[doc(hidden)]
pub struct AktorCleanupSync<E>(PhantomData<E>);
#[doc(hidden)]
pub struct AktorCleanupSyncUnit;

fn failed<E, X: Into<(E, String)>>(error: X) -> (AktorCleanupError, String) {
    let (_, report) = error.into();
    crate::report::with_report(AktorCleanupError, report)
}

#[doc(hidden)]
pub trait AktorEndCall<S, Mode> {
    type Error;
    fn run_typed(self, state: S) -> impl Future<Output = Result<(), (Self::Error, String)>>;

    fn run(self, state: S) -> impl Future<Output = Result<(), (AktorCleanupError, String)>>
    where
        Self: Sized,
    {
        async move {
            match self.run_typed(state).await {
                Ok(()) => Ok(()),
                Err(error) => Err(failed(error)),
            }
        }
    }
}
impl<S> AktorEndCall<S, AktorNoCleanup> for () {
    type Error = core::convert::Infallible;

    async fn run_typed(self, state: S) -> Result<(), (Self::Error, String)> {
        drop(state);
        Ok(())
    }
}
impl<S, F, X, E> AktorEndCall<S, AktorCleanupCallback<E>> for F
where
    F: AsyncFnOnce(S) -> Result<(), X>,
    X: Into<(E, String)>,
{
    type Error = E;

    async fn run_typed(self, state: S) -> Result<(), (Self::Error, String)> {
        match self(state).await {
            Ok(()) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
impl<S, F> AktorEndCall<S, AktorCleanupCallbackUnit> for F
where
    F: AsyncFnOnce(S),
{
    type Error = core::convert::Infallible;

    async fn run_typed(self, state: S) -> Result<(), (Self::Error, String)> {
        self(state).await;
        Ok(())
    }
}

impl<S, F, X, E> AktorEndCall<S, AktorCleanupSync<E>> for F
where
    F: FnOnce(S) -> Result<(), X>,
    X: Into<(E, String)>,
{
    type Error = E;

    async fn run_typed(self, state: S) -> Result<(), (Self::Error, String)> {
        match self(state) {
            Ok(()) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
impl<S, F> AktorEndCall<S, AktorCleanupSyncUnit> for F
where
    F: FnOnce(S),
{
    type Error = core::convert::Infallible;

    async fn run_typed(self, state: S) -> Result<(), (Self::Error, String)> {
        self(state);
        Ok(())
    }
}

#[doc(hidden)]
pub trait AktorSendEnd<S, Mode>: Send + 'static {
    fn finish(self, state: S) -> AktorEndFuture;
}
impl<S: Send + 'static> AktorSendEnd<S, AktorNoCleanup> for () {
    fn finish(self, state: S) -> AktorEndFuture {
        Box::pin(async move {
            drop(state);
            Ok(())
        })
    }
}
impl<S: Send + 'static, F, Fut, X, E> AktorSendEnd<S, AktorCleanupCallback<E>> for F
where
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), X>> + Send + 'static,
    X: Into<(E, String)> + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        let future = self(state);

        Box::pin(async move {
            match future.await {
                Ok(()) => Ok(()),
                Err(error) => Err(failed(error)),
            }
        })
    }
}
impl<S: Send + 'static, F, Fut> AktorSendEnd<S, AktorCleanupCallbackUnit> for F
where
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        let future = self(state);

        Box::pin(async move {
            future.await;
            Ok(())
        })
    }
}

impl<S: Send + 'static, F, X, E> AktorSendEnd<S, AktorCleanupSync<E>> for F
where
    F: FnOnce(S) -> Result<(), X> + Send + 'static,
    X: Into<(E, String)> + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        Box::pin(async move {
            match self(state) {
                Ok(()) => Ok(()),
                Err(error) => Err(failed(error)),
            }
        })
    }
}
impl<S: Send + 'static, F> AktorSendEnd<S, AktorCleanupSyncUnit> for F
where
    F: FnOnce(S) + Send + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        Box::pin(async move {
            self(state);
            Ok(())
        })
    }
}
