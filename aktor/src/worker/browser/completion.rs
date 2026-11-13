use super::*;
use core::future::IntoFuture;

impl<E> Completion<E> {
    /// Read the current outcome of `end` without waiting or decoding its error data.
    pub fn cleanup_status(&self) -> crate::AktorCleanupStatus {
        use crate::AktorCleanupStatus;

        let closed = self.end.has_changed().is_err();
        match &*self.end.borrow() {
            End::Returned(Ok(())) => AktorCleanupStatus::Succeeded,
            End::Returned(Err(_)) => AktorCleanupStatus::Failed,
            End::Waiting if !closed => AktorCleanupStatus::Pending,
            End::Waiting | End::Unconfirmed(_) => AktorCleanupStatus::Unconfirmed,
        }
    }

    #[doc(hidden)]
    pub async fn wait_diagnostics(&self) -> Vec<crate::AktorError> {
        self.retirement_status().await;
        self.diagnostics.borrow().clone()
    }

    /// The lifecycle report, without decoding the optional application data.
    pub async fn wait_report(&self) -> Result<(), (WorkerError, String)> {
        observe(self.result.clone())
            .await
            .map_err(WireError::report_without_data)
    }

    pub(crate) async fn retirement_status(&self) -> AktorRetireStatus {
        let mut result = self.result.clone();
        let _receipt = result.wait_for(Option::is_some).await;
        *self.retirement.borrow()
    }

    pub(crate) async fn await_retirement(&self) -> AktorRetireStatus {
        let mut retirement = self.retirement.clone();
        retirement
            .wait_for(|status| *status != AktorRetireStatus::Unconfirmed)
            .await
            .map_or(AktorRetireStatus::Unconfirmed, |status| *status)
    }

    pub(crate) fn unavailable(
        error: WorkerFailure,
        mut dependency: watch::Receiver<Option<Result<(), crate::AktorError>>>,
    ) -> Self {
        let error = error.without_data();
        let status = if dependency.borrow().is_some() {
            AktorRetireStatus::Confirmed
        } else {
            AktorRetireStatus::Unconfirmed
        };
        let (retired, retirement) = watch::channel(status);
        let diagnostics = Rc::new(RefCell::new(vec![crate::AktorError::new(
            format!("{error:#}"),
            (),
        )]));
        let (_, result) = watch::channel(Some(Err(error)));

        if let Some(Err(error)) = &*dependency.borrow() {
            diagnostics.borrow_mut().push(error.clone());
        }

        if status == AktorRetireStatus::Unconfirmed {
            let diagnostics = diagnostics.clone();

            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    let joined = dependency.borrow_and_update().clone();

                    if let Some(result) = joined {
                        if let Err(error) = result {
                            diagnostics.borrow_mut().push(error);
                        }

                        retired.send_replace(AktorRetireStatus::Confirmed);
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
            retirement,
            end: watch::channel(End::Unconfirmed(None)).1,
            data: PhantomData,
        }
    }
}
impl<E: DeserializeOwned> Completion<E> {
    /// Wait for this worker's `end`. An earlier failed request keeps its own result.
    pub async fn wait_cleanup(&self) -> Result<(), (WorkerCleanupError<E>, String)> {
        let mut end = self.end.clone();
        let receipt = end.wait_for(|end| !matches!(end, End::Waiting)).await;
        match receipt.as_deref() {
            Ok(End::Returned(Ok(()))) => Ok(()),
            Ok(End::Returned(Err(error))) => {
                let (error, report) = typed(error.clone());
                Err((WorkerCleanupError::Failed(error.data), report))
            }
            Ok(End::Unconfirmed(Some(report))) => {
                Err((WorkerCleanupError::Unconfirmed, report.clone()))
            }
            Err(_) | Ok(End::Waiting | End::Unconfirmed(None)) => Err((
                WorkerCleanupError::Unconfirmed,
                "Worker cleanup result is unconfirmed".into(),
            )),
        }
    }

    /// Wait for the worker's result and see whether retirement has been confirmed yet.
    pub async fn wait(&self) -> Retirement<E> {
        let result = self.report().await;

        let missing = || WorkerFailure::new(CallError::OutcomeUnknown, WorkerCause::Closed).into();
        match *self.retirement.borrow() {
            AktorRetireStatus::Confirmed => Retirement::Confirmed(result),
            AktorRetireStatus::Terminated => {
                Retirement::Terminated(result.err().unwrap_or_else(missing))
            }
            AktorRetireStatus::Unconfirmed => {
                Retirement::Unconfirmed(result.err().unwrap_or_else(missing))
            }
        }
    }

    /// Keep waiting for a retirement receipt or forced termination after an early failure.
    /// Use [`wait()`](Self::wait) to read the status while retirement is still unconfirmed.
    ///
    /// A standalone worker that loses contact has five seconds to finish cleanup after
    /// any external join. A worker that explicitly retains unfinished work can stay alive.
    pub async fn wait_retired(&self) -> Retirement<E> {
        self.await_retirement().await;
        self.wait().await
    }

    /// Read the same application data as `.wait()` without waiting for cleanup confirmation.
    pub async fn report(&self) -> Result<(), (WorkerError<E>, String)> {
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

pub(super) async fn observe<E: Clone + From<WireError>>(
    mut result: watch::Receiver<Option<Result<(), E>>>,
) -> Result<(), E> {
    loop {
        if let Some(result) = result.borrow().clone() {
            return result;
        }

        if result.changed().await.is_err() {
            return Err(WireError::new(CallError::OutcomeUnknown, WorkerCause::Closed).into());
        }
    }
}

impl<T> From<WorkerFailure<T>> for ReadyFailure<T> {
    fn from(error: WorkerFailure<T>) -> Self {
        Self {
            error,
            init: crate::AktorInitOutcome::Unconfirmed,
        }
    }
}

impl<T> ReadyFailure<T> {
    pub(crate) fn not_run(error: WorkerFailure<T>) -> Self {
        Self {
            error,
            init: crate::AktorInitOutcome::NotRun,
        }
    }

    pub(crate) fn report<E>(
        self,
        decode: impl FnOnce(WorkerFailure<T>) -> (WorkerError<E>, String),
    ) -> (WorkerStartError<E>, String) {
        use crate::AktorInitOutcome::*;

        let (error, report) = decode(self.error);
        let init = match self.init {
            NotRun => NotRun,
            Unconfirmed => error.data.map_or(Unconfirmed, Failed),
            Failed(never) => match never {},
        };

        (
            WorkerStartError {
                init,
                outcome: error.outcome,
                cause: error.cause,
            },
            report,
        )
    }
}

impl<E> WorkerStartError<E> {
    pub(crate) fn not_run(failure: WorkerFailure) -> (Self, String) {
        ReadyFailure::not_run(failure).report(WorkerFailure::report_without_data)
    }

    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        crate::worker::impls::write_error(f, self.outcome, &self.cause)
    }
}

pub fn typed<E: DeserializeOwned>(error: WireError) -> (WorkerError<E>, String) {
    let mut report: WorkerFailure<E> = WorkerFailure {
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

    report.into()
}
impl<E> Clone for Completion<E> {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            diagnostics: self.diagnostics.clone(),
            retirement: self.retirement.clone(),
            end: self.end.clone(),
            data: PhantomData,
        }
    }
}
