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

    pub(crate) fn unavailable(
        error: WorkerError,
        mut dependency: watch::Receiver<Option<Result<(), crate::AktorError>>>,
    ) -> Self {
        let error = error.without_data();
        let (finished, result) = watch::channel(Some(Err(error.clone())));
        let retired = Rc::new(Cell::new(dependency.borrow().is_some()));
        let diagnostics = Rc::new(RefCell::new(vec![crate::AktorError::new(
            format!("{error:#}"),
            (),
        )]));

        if !retired.get() {
            let retired = retired.clone();
            let diagnostics = diagnostics.clone();

            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    let joined = dependency.borrow_and_update().clone();

                    if let Some(result) = joined {
                        if let Err(error) = result {
                            diagnostics.borrow_mut().push(error);
                        }

                        retired.set(true);
                        finished.send_replace(Some(Err(error)));
                        return;
                    }

                    if dependency.changed().await.is_err() {
                        return;
                    }
                }
            });
        }

        Self {
            result,
            diagnostics,
            retired,
            data: PhantomData,
        }
    }

    pub fn new_observer(&self) -> Self {
        Self {
            result: self.result.clone(),
            diagnostics: self.diagnostics.clone(),
            retired: self.retired.clone(),
            data: PhantomData,
        }
    }
}
impl<E: DeserializeOwned> Completion<E> {
    /// Wait for the outcome, including whether cleanup has been confirmed.
    pub async fn wait(&self) -> Retirement<E> {
        let result = self.report().await;

        if self.retired.get() {
            Retirement::Confirmed(result)
        } else {
            match result {
                Err(error) => Retirement::Unconfirmed(error),
                Ok(()) => Retirement::Unconfirmed(completion::typed(WireError::new(
                    CallError::OutcomeUnknown,
                    WorkerCause::Closed,
                ))),
            }
        }
    }

    /// Wait for a cleanup receipt after an earlier unconfirmed failure.
    /// This stays pending while the physical owner has not acknowledged retirement.
    pub async fn wait_retired(&self) -> Retirement<E> {
        let mut result = self.result.clone();

        while !self.retired.get() {
            if result.changed().await.is_err() {
                break;
            }
        }

        self.wait().await
    }

    /// Observe an early failure report without waiting for a cleanup receipt.
    pub async fn report(&self) -> Result<(), WorkerError<E>> {
        observe(self.result.clone()).await.map_err(typed)
    }
}
impl<E: DeserializeOwned + 'static> IntoFuture for Completion<E> {
    type Output = Retirement<E>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: DeserializeOwned + 'a> IntoFuture for &'a Completion<E> {
    type Output = Retirement<E>;
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
