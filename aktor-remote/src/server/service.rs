use super::{Failures, ServerFailure};
use crate::{AktorDeliveryOutcome, Cause, Rejection, WireFailure};
use aktor::{
    message::LocalFuture,
    worker::{Operations, Registry, WorkerCause, WorkerError},
};
use er::*;

pub(super) type Run<S> = for<'a> fn(
    &'a Registry,
    &'a mut S,
    String,
    Vec<u8>,
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>>;

pub struct Service<S> {
    pub registry: Registry,
    pub operations: Operations,
    pub run: Run<S>,
    #[cfg(test)]
    pub pending: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pub cancelled: std::sync::Arc<tokio::sync::Notify>,
}

impl<S> core::ops::Deref for Service<S> {
    type Target = Registry;

    fn deref(&self) -> &Registry {
        &self.registry
    }
}

#[derive(Er)]
pub struct DispatchErr {
    pub outcome: AktorDeliveryOutcome,
    pub cause: Cause,
    pub reason: Rejection,
}
impl<S> Service<S> {
    pub async fn dispatch(
        &self,
        state: &mut S,
        name: String,
        input: Vec<u8>,
    ) -> ErResult<Vec<u8>, DispatchErr> {
        (self.run)(&self.registry, state, name, input)
            .await
            .er_with(|err| {
                let (cause, reason) = match &err.cause {
                    WorkerCause::Codec => (Cause::Codec, Rejection::Dispatch),
                    WorkerCause::Closed => (Cause::Closed, Rejection::ActorClosed),
                    WorkerCause::Full => (Cause::Closed, Rejection::AdmissionRefused),
                    WorkerCause::UnknownOperation(_) => {
                        (Cause::Protocol, Rejection::UnknownOperation)
                    }
                    WorkerCause::Operations { .. } => {
                        (Cause::Protocol, Rejection::UnsupportedOperation)
                    }
                    WorkerCause::Setup
                    | WorkerCause::Cleanup
                    | WorkerCause::Crashed
                    | WorkerCause::Protocol
                    | WorkerCause::Transport => (Cause::Protocol, Rejection::Dispatch),
                };
                DispatchErr {
                    outcome: err.outcome,
                    cause,
                    reason,
                }
            })
    }
}

pub fn answer(
    output: ErResult<Vec<u8>, DispatchErr>,
    errors: &Failures,
) -> Result<Vec<u8>, WireFailure> {
    match output {
        Ok(bytes) => Ok(bytes),
        Err(error) => {
            let mut failure =
                WireFailure::rejected(error.top.outcome, error.top.cause, error.top.reason);
            failure.owner_report = Some(error.er_report());
            errors.record(er_new!(ServerFailure::session(failure.cause), error));
            Err(failure)
        }
    }
}
