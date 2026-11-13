use crate::AktorError;

#[doc(hidden)]
pub trait AktorCleanupOutput<Mode = ()> {
    fn into_cleanup(self) -> Result<(), AktorError>;
}
#[doc(hidden)]
pub struct AktorShutdownSync;
#[doc(hidden)]
pub struct AktorShutdownAsync;

#[doc(hidden)]
pub trait AktorShutdownOutput<Mode> {
    type Future: core::future::Future<Output = Result<(), AktorError>>;

    fn into_shutdown(self) -> Self::Future;
}

#[doc(hidden)]
pub struct AktorShutdownFuture<F, Mode> {
    pub(crate) mode: core::marker::PhantomData<fn() -> Mode>,
    pub future: core::pin::Pin<alloc::boxed::Box<F>>,
}
