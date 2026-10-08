use super::super::*;
use core::future::{IntoFuture, poll_fn};

impl<E> Completion<E> {
    #[doc(hidden)]
    pub fn panic_reported(&self) -> bool {
        self.inner.panic_reported.get()
    }

    #[doc(hidden)]
    pub fn diagnostics(&self) -> alloc::vec::Vec<crate::AktorError> {
        self.inner.diagnostics.borrow().clone()
    }

    pub fn new_observer(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    /// Observe completion without retaining the owner's typed failure data.
    pub async fn wait(&self) -> Result<(), OwnerError> {
        self.wait_with_data()
            .await
            .map_err(|error| error.error.report())
    }

    /// Observe the exact lifecycle error and its data on this executor.
    pub async fn wait_with_data(&self) -> Result<(), SharedOwnerError<E>> {
        let changed = self.inner.changed.listen();

        poll_fn(|context| {
            changed.register(context);

            match self.inner.result.borrow().clone() {
                Some(result) => Poll::Ready(result.map_err(|error| SharedOwnerError { error })),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl<E: 'static> IntoFuture for Completion<E> {
    type Output = Result<(), OwnerError>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: 'a> IntoFuture for &'a Completion<E> {
    type Output = Result<(), OwnerError>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}
