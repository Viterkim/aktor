use super::AktorEndFuture;
use crate::{AktorCleanupOutput, AktorFailure};
use alloc::boxed::Box;
use core::future::Future;

pub struct AktorNoCleanup;
pub struct AktorCleanupCallback;
pub struct AktorCleanupSync;

#[doc(hidden)]
pub trait AktorEndCall<S, Mode> {
    fn run(self, state: S) -> impl Future<Output = Result<(), AktorFailure>>;
}
impl<S> AktorEndCall<S, AktorNoCleanup> for () {
    async fn run(self, state: S) -> Result<(), AktorFailure> {
        drop(state);
        Ok(())
    }
}
impl<S, F, Output, Mode> AktorEndCall<S, (AktorCleanupCallback, Mode)> for F
where
    F: AsyncFnOnce(S) -> Output,
    Output: AktorCleanupOutput<Mode>,
{
    async fn run(self, state: S) -> Result<(), AktorFailure> {
        self(state).await.into_cleanup().map_err(AktorFailure::from)
    }
}

impl<S, F, Output, Mode> AktorEndCall<S, (AktorCleanupSync, Mode)> for F
where
    F: FnOnce(S) -> Output,
    Output: AktorCleanupOutput<Mode>,
{
    async fn run(self, state: S) -> Result<(), AktorFailure> {
        self(state).into_cleanup().map_err(AktorFailure::from)
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
impl<S: Send + 'static, F, Fut, Output, Mode> AktorSendEnd<S, (AktorCleanupCallback, Mode)> for F
where
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = Output> + Send + 'static,
    Output: AktorCleanupOutput<Mode> + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        let future = self(state);

        Box::pin(async move { future.await.into_cleanup().map_err(AktorFailure::from) })
    }
}

impl<S: Send + 'static, F, Output, Mode> AktorSendEnd<S, (AktorCleanupSync, Mode)> for F
where
    F: FnOnce(S) -> Output + Send + 'static,
    Output: AktorCleanupOutput<Mode>,
{
    fn finish(self, state: S) -> AktorEndFuture {
        Box::pin(async move { self(state).into_cleanup().map_err(AktorFailure::from) })
    }
}
