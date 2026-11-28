use super::super::*;
use core::future::{IntoFuture, poll_fn};

impl<E> Completion<E> {
    #[cfg(any(
        all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
        all(
            feature = "browser_local",
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn cleanup_completed(&self) -> bool {
        self.inner.cleanup_completed.get()
    }

    pub fn init(&self) -> crate::AktorInitEvidence {
        self.inner.init.get()
    }

    /// Shutdown cut its calls or its `end`, so it ran past its grace.
    #[doc(hidden)]
    pub fn ran_past_grace(&self) -> bool {
        self.inner.cut_call.get() || self.inner.ran_out.get()
    }

    /// Wait for the owner to finish, returning a diagnostic on failure.
    pub async fn wait(&self) -> Result<(), (OwnerError, String)> {
        match self.observe().await {
            Ok(()) => Ok(()),
            Err((error, report)) => {
                self.receive();
                Err((error.without_data(), report))
            }
        }
    }

    /// Wait for the owner to finish and get the error with its application data.
    pub async fn wait_with_data(&self) -> Result<(), (SharedOwnerError<E>, String)> {
        let result = self.observe().await;
        if result.is_err() {
            self.receive();
        }
        result
    }

    pub(super) fn receive(&self) {
        for receipt in self.inner.returned.borrow().iter() {
            receipt.receive();
        }
    }

    pub(crate) async fn observe(&self) -> Result<(), (SharedOwnerError<E>, String)> {
        let changed = self.inner.changed.listen();

        poll_fn(|context| {
            changed.register(context);

            match self.result.borrow().clone() {
                Some(Ok(())) => Poll::Ready(Ok(())),
                Some(Err(error)) => Poll::Ready(Err(SharedOwnerError::from_failure(
                    error,
                    &self.inner.diagnostics.borrow(),
                ))),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl<E: 'static> IntoFuture for Completion<E> {
    type Output = Result<(), (OwnerError, String)>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: 'a> IntoFuture for &'a Completion<E> {
    type Output = Result<(), (OwnerError, String)>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}
impl<E> Clone for Completion<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            result: self.result.clone(),
        }
    }
}
