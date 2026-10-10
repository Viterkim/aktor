use super::AktorEndFuture;
use crate::{AktorCleanupOutput, AktorFailure};
use alloc::boxed::Box;
use core::future::Future;

pub struct AktorNoCleanup;
pub struct AktorCleanupCallback;
pub struct AktorCleanupSync;

pub trait AktorEndCall<S, Mode> {
    fn run(&mut self, state: S) -> impl Future<Output = Result<(), AktorFailure>>;
}
impl<S> AktorEndCall<S, AktorNoCleanup> for () {
    async fn run(&mut self, state: S) -> Result<(), AktorFailure> {
        drop(state);
        Ok(())
    }
}
impl<S, F, Output> AktorEndCall<S, AktorCleanupCallback> for F
where
    F: AsyncFnMut(S) -> Output,
    Output: AktorCleanupOutput,
{
    async fn run(&mut self, state: S) -> Result<(), AktorFailure> {
        self(state).await.into_cleanup().map_err(AktorFailure::from)
    }
}

impl<S, F, Output> AktorEndCall<S, AktorCleanupSync> for F
where
    F: FnMut(S) -> Output,
    Output: AktorCleanupOutput,
{
    async fn run(&mut self, state: S) -> Result<(), AktorFailure> {
        self(state).into_cleanup().map_err(AktorFailure::from)
    }
}

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
impl<S: Send + 'static, F, Fut, Output> AktorSendEnd<S, AktorCleanupCallback> for F
where
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = Output> + Send + 'static,
    Output: AktorCleanupOutput + 'static,
{
    fn finish(self, state: S) -> AktorEndFuture {
        let future = self(state);

        Box::pin(async move { future.await.into_cleanup().map_err(AktorFailure::from) })
    }
}

impl<S: Send + 'static, F, Output> AktorSendEnd<S, AktorCleanupSync> for F
where
    F: FnOnce(S) -> Output + Send + 'static,
    Output: AktorCleanupOutput,
{
    fn finish(self, state: S) -> AktorEndFuture {
        Box::pin(core::future::ready(
            self(state).into_cleanup().map_err(AktorFailure::from),
        ))
    }
}
