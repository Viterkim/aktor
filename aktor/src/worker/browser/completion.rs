use super::*;
use core::future::IntoFuture;

impl<E> Completion<E> {
    #[doc(hidden)]
    pub async fn wait_diagnostics(&self) -> Vec<crate::AktorError> {
        let _result = observe(self.result.clone()).await;
        self.diagnostics.borrow().clone()
    }

    /// The lifecycle report, without decoding the optional application data.
    pub async fn wait_report(&self) -> Result<(), WorkerError> {
        observe(self.result.clone())
            .await
            .map_err(WireError::without_data)
    }
}
impl<E: DeserializeOwned> Completion<E> {
    pub fn new_observer(&self) -> Self {
        Self {
            result: self.result.clone(),
            diagnostics: self.diagnostics.clone(),
            data: PhantomData,
        }
    }

    pub async fn wait(&self) -> Result<(), WorkerError<E>> {
        observe(self.result.clone()).await.map_err(typed)
    }
}
impl<E: DeserializeOwned + 'static> IntoFuture for Completion<E> {
    type Output = Result<(), WorkerError<E>>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: DeserializeOwned + 'a> IntoFuture for &'a Completion<E> {
    type Output = Result<(), WorkerError<E>>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

pub async fn observe(
    mut result: watch::Receiver<Option<Result<(), WireError>>>,
) -> Result<(), WireError> {
    loop {
        if let Some(result) = result.borrow().clone() {
            return result;
        }

        if result.changed().await.is_err() {
            return Err(WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Closed,
            ));
        }
    }
}

pub fn typed<E: DeserializeOwned>(error: WireError) -> WorkerError<E> {
    let mut report: WorkerError<E> = WorkerError {
        outcome: error.outcome,
        cause: error.cause,
        diagnostics: error.diagnostics,
        data: None,
    };

    if let Some(bytes) = error.data {
        match decode(&bytes) {
            Ok(data) => report.data = Some(data),
            Err(codec) => report = report.data_error(codec),
        }
    }

    report
}
