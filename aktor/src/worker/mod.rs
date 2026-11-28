#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub(crate) mod browser;
mod impls;
mod registry;
mod wire;

pub use crate::dispatch::{Native, Remote};
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use browser::{
    AktorCleanup, Completion, LatestResults, LatestSender, OptionalRequest, OptionalRequestSend,
    OptionalWorker, Worker, WorkerCleanupError, WorkerEndError, WorkerFailureReceipt, WorkerJoin,
    WorkerJoinOutcome, WorkerReply, WorkerRequest, WorkerRequestCast, WorkerRequestSend,
    WorkerStartError, WorkerStartFailure, join,
};
#[cfg(all(
    feature = "wasm_browser_worker_server",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use browser::{AktorReady, AktorServe, ServeFailure, serve_setup};
#[doc(hidden)]
pub use inventory;
pub use registry::Operations;
#[doc(hidden)]
pub use registry::{Exporter, Register, Registered, Registration, Registry};
#[doc(hidden)]
pub use wire::Codec;

use crate::message::AktorDeliveryOutcome;
use er::*;

pub const VERSION: u32 = 0;

#[derive(Debug, Clone, aktor_macros::AktorData)]
pub struct Options {
    /// Same on both sides. Change it when the wire data format changes.
    pub build: String,
    /// Ordinary calls waiting for replies, including the running call.
    pub capacity: core::num::NonZeroUsize,
    /// Soft byte budget for admitted calls. Oversized calls take the whole budget.
    /// Waiting inputs and latest sessions are outside it.
    pub max_outstanding_bytes: core::num::NonZeroUsize,
}

/// What went wrong with a worker. Fallible calls return it with the report as `(WorkerError, String)`.
#[derive(Clone, PartialEq, Eq, Er, aktor_macros::AktorData)]
#[er(format = "worker call {outcome:?}: {cause}")]
#[non_exhaustive]
pub struct WorkerError<T = ()> {
    pub outcome: AktorDeliveryOutcome,
    pub cause: WorkerCause,
    pub data: Option<T>,
    /// Identifies a recorded worker failure. A grouped worker uses the same ID in its shutdown report.
    #[aktor(skip)]
    pub failure_id: Option<crate::FailureId>,
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(Clone, Debug, aktor_macros::AktorData)]
pub(crate) struct WireFailure {
    pub(crate) error: WorkerError<Vec<u8>>,
    pub(crate) report: String,
}

#[derive(Clone, PartialEq, Eq, ErFormat, aktor_macros::AktorData)]
pub enum WorkerCause {
    Closed,
    Full,
    Codec,
    #[er(format = "worker setup failed")]
    Setup,
    #[er(format = "worker cleanup failed")]
    Cleanup,
    Crashed,
    Protocol,
    UnknownOperation(String),
    #[er(format = "worker operations do not match: expected {expected:?}, got {actual:?}")]
    Operations {
        expected: Vec<String>,
        actual: Vec<String>,
    },
    /// The browser couldn't prepare a transfer buffer or post the message.
    Transport,
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(aktor_macros::AktorData)]
enum Incoming {
    Call {
        id: u64,
        operation: String,
        input: Vec<u8>,
    },
    Shutdown,
    Initialize {
        config: Vec<u8>,
    },
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(Clone, aktor_macros::AktorData)]
enum Outgoing {
    Configure {
        config_type: String,
        payload_type: String,
        version: u32,
        options: Options,
        operations: Operations,
    },
    Ready {
        payload_type: String,
        payload: Vec<u8>,
        version: u32,
        options: Options,
        operations: Operations,
    },
    SetupFailed(WireFailure),
    Started {
        id: u64,
    },
    Answer {
        id: u64,
        output: Result<Vec<u8>, WireFailure>,
    },
    Finished(Result<(), WireFailure>),
    SetupRetired,
    Unconfirmed(WireFailure),
    InitNotRun {
        version: u32,
        error: WireFailure,
    },
    SetupPending(WireFailure),
    ReadyFailed(WireFailure),
    EndReturned(WireFailure),
    Retired,
}

#[doc(hidden)]
#[cfg(not(all(
    target_family = "wasm",
    feature = "wasm_browser_workers",
    not(feature = "wasm_browser_worker_server")
)))]
pub async fn run_export<E, C>(
    export: E,
    state: &mut E::State,
    input: &[u8],
) -> Result<Vec<u8>, (WorkerError, String)>
where
    E: crate::dispatch::Export<C>,
    C: Codec<E::Input> + Codec<E::Output>,
{
    let input = crate::worker::wire::codec(<C as Codec<E::Input>>::decode(input), || {
        WorkerError::new(AktorDeliveryOutcome::Discarded, WorkerCause::Codec)
    })?;

    let output = export.run(state, input).await;

    Ok(crate::worker::wire::codec(
        <C as Codec<E::Output>>::encode(&output),
        || WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Codec),
    )?)
}
