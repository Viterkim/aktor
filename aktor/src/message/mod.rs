mod error;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
mod native;

#[doc(hidden)]
pub use error::consumed;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use native::*;

use alloc::boxed::Box;
use core::{future::Future, pin::Pin};
use er::*;

#[derive(Clone, Copy, Er, PartialEq, Eq)]
pub enum AktorStateError {
    NotStarted,
    CapacityTooLarge,
    Closed,
}

pub type AktorStateResult<T> = Result<T, AktorStateError>;

#[cfg_attr(feature = "transport", derive(aktor_macros::AktorData))]
#[derive(Clone, Copy, Er, PartialEq, Eq)]
pub enum AktorDeliveryOutcome {
    /// The call did not enter the queue.
    NotAdmitted,
    /// The call was dropped before its operation ran.
    Discarded,
    /// The operation may have run, but its answer was lost.
    OutcomeUnknown,
}

/// A call that could not be delivered, or whose answer was lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AktorDeliveryError {
    pub outcome: AktorDeliveryOutcome,
    pub report: alloc::string::String,
    /// The matching shutdown report entry when an owner failure caused this loss.
    pub failure_id: Option<crate::FailureId>,
}

impl core::fmt::Display for AktorDeliveryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.report)
    }
}

impl core::error::Error for AktorDeliveryError {}

impl From<AktorDeliveryOutcome> for AktorDeliveryError {
    fn from(outcome: AktorDeliveryOutcome) -> Self {
        Self {
            outcome,
            report: er_new!(outcome).er_report(),
            failure_id: None,
        }
    }
}

pub type LocalFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

#[doc(hidden)]
pub fn boxed<'a, F: Future + 'a>(future: F) -> LocalFuture<'a, F::Output> {
    Box::pin(future)
}
