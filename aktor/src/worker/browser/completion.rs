use super::*;
use core::future::IntoFuture;

impl<E> Completion<E> {
    pub(crate) async fn retirement_status(&self) -> AktorRetirement {
        let mut result = self.result.clone();
        let _receipt = result.wait_for(Option::is_some).await;
        self.retirement
            .borrow()
            .unwrap_or(AktorRetirement::StillRunning)
    }

    pub(crate) async fn await_retirement(&self) -> AktorRetirement {
        let mut retirement = self.retirement.clone();
        retirement
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|status| *status)
            .unwrap_or(AktorRetirement::StillRunning)
    }

    pub(crate) fn unavailable(
        error: WireFailure,
        name: String,
        kill: crate::AktorKillSwitch,
        dependency: Option<JoinReceipt>,
    ) -> Self {
        let (retired, retirement) =
            watch::channel(dependency.is_none().then_some(AktorRetirement::Released));
        let group = kill.clone();
        let (_, result) = watch::channel(Some(Err(error.clone())));

        let record_join = move |error: String| {
            kill.observe_failure(crate::AktorFailureInput::actor(
                name.clone(),
                crate::AktorExecution::BrowserWebWorker,
                crate::AktorPhase::Worker(crate::AktorWorkerPhase::ExternalJoin),
                error,
            ));
        };

        if let Some(mut dependency) = dependency {
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(receipt) = dependency.wait_for(Option::is_some).await {
                    match receipt.as_ref() {
                        Some(WorkerJoinOutcome::Stopped(result)) => {
                            if let Err((_, report)) = result {
                                record_join(report.clone());
                            }
                            retired.send_replace(Some(AktorRetirement::Released));
                        }
                        Some(WorkerJoinOutcome::Unconfirmed(report)) => {
                            record_join(report.clone());
                            retired.send_replace(Some(AktorRetirement::StillRunning));
                        }
                        None => unreachable!(),
                    }
                }
            });
        }

        Self {
            result,
            group,
            retirement,
            end: watch::channel(End::Unconfirmed(error)).1,
            data: PhantomData,
        }
    }
}
impl<E: AktorData + 'static> Completion<E> {
    pub fn cleanup(&self) -> AktorCleanup<E> {
        let end = self.end.borrow();
        match &*end {
            End::Waiting if self.end.has_changed().is_ok() => AktorCleanup::Pending,
            End::Waiting => AktorCleanup::Unconfirmed(self.receipt(
                None,
                er_new!(WorkerCleanupError::<E>::Unconfirmed(None)).er_report(),
            )),
            End::Unconfirmed(error) => AktorCleanup::Unconfirmed(
                self.receipt(error.error.failure_id, error.report.clone()),
            ),
            End::Returned(Ok(())) => AktorCleanup::Returned(Ok(())),
            End::Returned(Err(error)) => {
                let (failure, report) = typed(error.clone());
                AktorCleanup::Returned(Err(self.receipt(
                    failure.failure_id,
                    WorkerEndError {
                        data: failure.data,
                        failure_id: failure.failure_id,
                        report,
                    },
                )))
            }
        }
    }

    fn receipt<T>(&self, id: Option<crate::FailureId>, error: T) -> WorkerFailureReceipt<T> {
        WorkerFailureReceipt {
            error,
            delivery: id.and_then(|id| self.group.delivery(id)),
        }
    }

    /// Wait for this worker's `end`. An earlier failed request keeps its own result.
    pub async fn wait_cleanup(&self) -> Result<(), (WorkerCleanupError<E>, String)> {
        let mut end = self.end.clone();
        let receipt = end
            .wait_for(|end| match end {
                End::Waiting => false,
                End::Returned(_) | End::Unconfirmed(_) => true,
            })
            .await;
        match receipt {
            Ok(end) => self.end_result(&end),
            Err(_) => self.end_result(&self.end.borrow()),
        }
    }

    /// Check for a final receipt without waiting. An owner that can still confirm release is pending.
    #[allow(clippy::type_complexity)]
    pub fn retired(
        &self,
    ) -> crate::AktorRetiredStatus<Result<(), WorkerFailureReceipt<(WorkerError<E>, String)>>> {
        use crate::{AktorRetired, AktorRetiredStatus};

        let status = *self.retirement.borrow();
        if status.is_none() && self.retirement.has_changed().is_ok() {
            return AktorRetiredStatus::Pending;
        }
        let result = self.result.borrow().clone().map(|result| match result {
            Ok(()) => Ok(()),
            Err(failure) => Err(self.receipt(failure.error.failure_id, typed(failure))),
        });
        match (status, result) {
            (Some(AktorRetirement::Released), Some(result)) => {
                AktorRetiredStatus::Confirmed(AktorRetired::Released(result))
            }
            (Some(AktorRetirement::Terminated), Some(result)) => {
                AktorRetiredStatus::Confirmed(AktorRetired::Terminated(result))
            }
            (_, result) => AktorRetiredStatus::Unconfirmed(result),
        }
    }

    /// Wait for the worker's result and see whether retirement has been confirmed yet.
    pub async fn wait(&self) -> WorkerRetired<E> {
        let result = self.report().await;

        let missing = || {
            WireFailure::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Closed)
                .without_data()
        };
        match self
            .retirement
            .borrow()
            .unwrap_or(AktorRetirement::StillRunning)
        {
            AktorRetirement::Released => Ok(crate::AktorRetired::Released(result)),
            AktorRetirement::Terminated => Ok(crate::AktorRetired::Terminated(result)),
            AktorRetirement::StillRunning => Err(result.err().unwrap_or_else(missing)),
        }
    }

    /// Keep waiting for a retirement receipt or forced termination after an early failure.
    /// Use [`wait()`](Self::wait) to read the status while retirement is still unconfirmed.
    pub async fn wait_retired(&self) -> WorkerRetired<E> {
        self.await_retirement().await;
        self.wait().await
    }

    /// Read the same application data as `.wait()` without waiting for cleanup confirmation.
    pub async fn report(&self) -> Result<(), (WorkerError<E>, String)> {
        self.receive(observe(self.result.clone()).await)
    }

    fn receive(&self, result: Result<(), WireFailure>) -> Result<(), (WorkerError<E>, String)> {
        match result {
            Ok(()) => Ok(()),
            Err(failure) => {
                let error = typed(failure);
                deliver(error.0.failure_id, &self.group);
                Err(error)
            }
        }
    }
}

impl<E: AktorData + 'static> IntoFuture for Completion<E> {
    type Output = WorkerRetired<E>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: AktorData + 'static> IntoFuture for &'a Completion<E> {
    type Output = WorkerRetired<E>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl<E: AktorData + 'static> Completion<E> {
    fn end_result(&self, end: &End) -> Result<(), (WorkerCleanupError<E>, String)> {
        match end {
            End::Returned(Ok(())) => Ok(()),
            End::Returned(Err(error)) => {
                let (failure, report) = typed(error.clone());
                deliver(failure.failure_id, &self.group);
                Err((
                    WorkerCleanupError::Failed(failure.data, failure.failure_id),
                    report,
                ))
            }
            End::Unconfirmed(failure) => {
                let id = failure.error.failure_id;
                deliver(id, &self.group);
                Err((WorkerCleanupError::Unconfirmed(id), failure.report.clone()))
            }
            End::Waiting => er_bail!(WorkerCleanupError::Unconfirmed(None)),
        }
    }
}

pub async fn observe<E: Clone + From<WireFailure>>(
    mut result: watch::Receiver<Option<Result<(), E>>>,
) -> Result<(), E> {
    loop {
        if let Some(result) = result.borrow().clone() {
            return result;
        }

        if result.changed().await.is_err() {
            return Err(WireFailure::new(
                AktorDeliveryOutcome::OutcomeUnknown,
                WorkerCause::Closed,
            )
            .into());
        }
    }
}

impl From<WireFailure> for ReadyFailure {
    fn from(error: WireFailure) -> Self {
        Self {
            reason: match error.error.cause {
                WorkerCause::Closed => crate::AktorStartReason::Stopping,
                WorkerCause::Setup => crate::AktorStartReason::Init,
                _ => crate::AktorStartReason::InvalidSetup,
            },
            error,
            evidence: crate::AktorInitEvidence::Unknown,
        }
    }
}

impl ReadyFailure {
    pub(crate) fn entered(error: WireFailure) -> Self {
        Self {
            evidence: crate::AktorInitEvidence::Entered,
            ..Self::from(error)
        }
    }

    pub(crate) fn not_run(error: WireFailure) -> Self {
        Self {
            reason: if error.error.cause == WorkerCause::Closed {
                crate::AktorStartReason::Stopping
            } else {
                crate::AktorStartReason::InvalidSetup
            },
            error,
            evidence: crate::AktorInitEvidence::NeverEntered,
        }
    }

    pub(crate) fn report<E>(
        self,
        decode: impl FnOnce(WireFailure) -> (WorkerError<E>, String),
    ) -> (WorkerStartError<E>, String) {
        let (error, report) = decode(self.error);
        let failure = WorkerStartFailure {
            reason: self.reason,
            evidence: self.evidence,
            failure_id: error.failure_id,
            outcome: error.outcome,
            cause: error.cause,
        };
        let startup = match (self.evidence, error.data) {
            (crate::AktorInitEvidence::NeverEntered, _) => WorkerStartError::Refused(failure),
            (_, Some(error)) => WorkerStartError::Initializer { error, failure },
            (_, None) => WorkerStartError::Failed(failure),
        };
        (startup, report)
    }
}

impl<E> WorkerStartError<E> {
    pub fn failure(&self) -> &WorkerStartFailure {
        match self {
            Self::Refused(failure) | Self::Initializer { failure, .. } | Self::Failed(failure) => {
                failure
            }
        }
    }
}

#[derive(Er)]
struct DecodeLifecycleErr;
pub fn typed<E: AktorData>(failure: WireFailure) -> (WorkerError<E>, String) {
    let WireFailure { error, mut report } = failure;
    let mut typed = WorkerError::new(error.outcome, error.cause);

    typed.failure_id = error.failure_id;
    if let Some(bytes) = error.data {
        match decode(&bytes) {
            Ok(data) => typed.data = Some(data),
            Err((_, codec)) => {
                report = er_new!(
                    DecodeLifecycleErr
                        .er_save("worker failure", report)
                        .er_save("codec", codec)
                )
                .er_report()
            }
        }
    }

    (typed, report)
}
impl<E> Clone for Completion<E> {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            group: self.group.clone(),
            retirement: self.retirement.clone(),
            end: self.end.clone(),
            data: PhantomData,
        }
    }
}
