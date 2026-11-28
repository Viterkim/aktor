use crate::AktorCleanupError;
use alloc::string::String;

pub(crate) type CleanupResult = Result<(), (AktorCleanupError, String)>;

#[doc(hidden)]
pub trait AktorCleanupOutput<E> {
    fn into_cleanup(self) -> CleanupResult;
}
#[doc(hidden)]
pub struct AktorShutdownSync;
#[doc(hidden)]
pub struct AktorShutdownAsync;

#[doc(hidden)]
pub trait AktorShutdownOutput<Mode> {
    type Future: core::future::Future<Output = CleanupResult>;

    fn into_shutdown(self) -> Self::Future;
}

#[doc(hidden)]
pub struct AktorShutdownFuture<F: core::future::Future> {
    pub future: core::pin::Pin<alloc::boxed::Box<F>>,
    pub convert: fn(F::Output) -> CleanupResult,
}

#[doc(hidden)]
pub trait AktorJobOutput<E> {
    type Value;

    fn into_job(self) -> Result<Self::Value, String>;
}
impl AktorJobOutput<()> for () {
    type Value = ();

    fn into_job(self) -> Result<(), String> {
        Ok(())
    }
}
impl<T, E, X: Into<(E, String)>> AktorJobOutput<E> for Result<T, X> {
    type Value = T;

    fn into_job(self) -> Result<T, String> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.into().1),
        }
    }
}
