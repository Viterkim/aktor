use crate::message::CallError;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub(crate) mod browser;
pub mod bytes;
mod codec;
mod impls;
mod registry;
mod wire;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use browser::{
    AktorServe, Completion, LatestResults, LatestSender, OptionalReply, OptionalRequest,
    OptionalWorker, Server, Worker, WorkerCleanupError, WorkerJoin, WorkerReply, WorkerRequest,
    WorkerStartError, join, report_cleanup_pending, report_setup_pending, retain_cleanup,
    retain_setup, serve, serve_for, serve_setup, serve_with, setup_failed,
};
pub use codec::{decode, encode};
#[doc(hidden)]
pub use inventory;
pub use registry::Operations;
#[doc(hidden)]
pub use registry::{Exporter, Register, Registered, Registration, Registry, dispatch};
#[doc(hidden)]
pub use wire::Codec;

pub use crate::dispatch::{Native, Remote};

pub const VERSION: u32 = 4;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
type WireError = WorkerFailure<Vec<u8>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Options {
    /// Same on both sides. Change it when the wire data format changes.
    pub build: String,
    /// Ordinary calls waiting for replies, including the running call.
    pub capacity: usize,
    /// Soft byte budget for admitted calls. Oversized calls take the whole budget.
    /// Waiting inputs and latest sessions are outside it.
    pub max_outstanding_bytes: usize,
}

/// What happened to the worker when it stopped or lost contact with the page.
#[must_use = "check retirement before releasing or replacing the owner"]
#[derive(Debug, Clone)]
pub enum Retirement<E = ()> {
    /// The worker reported back, or startup ended before it was created.
    /// An earlier startup or call failure stays in the result.
    Confirmed(Result<(), (WorkerError<E>, String)>),
    /// The worker may still be using its resources.
    Unconfirmed((WorkerError<E>, String)),
    /// The page stopped the worker. Its cleanup may have been cut short.
    Terminated((WorkerError<E>, String)),
}

/// What went wrong with a worker. Fallible calls return it with the report as `(WorkerError, String)`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, er::Er)]
#[er(no_constructors, format = Self::fmt_display)]
#[non_exhaustive]
pub struct WorkerError<T = ()> {
    pub outcome: CallError,
    pub cause: WorkerCause,
    pub data: Option<T>,
}

#[doc(hidden)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, er::Er)]
#[er(no_constructors, display = Self::fmt_display, display_alt = "{diagnostics}", debug = Self::fmt_display)]
#[non_exhaustive]
pub struct WorkerFailure<T = ()> {
    pub outcome: CallError,
    pub cause: WorkerCause,
    pub diagnostics: Option<String>,
    pub data: Option<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerCause {
    Closed,
    Full,
    Codec(String),
    Setup,
    Cleanup,
    Crashed(String),
    Protocol,
    UnknownOperation(String),
    Operations {
        expected: Vec<String>,
        actual: Vec<String>,
    },
    /// The browser couldn't prepare a transfer buffer or post the message.
    Transport(String),
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(Serialize, Deserialize)]
enum Incoming {
    Call {
        id: u64,
        operation: String,
        #[serde(with = "bytes")]
        input: Vec<u8>,
    },
    Shutdown,
    Initialize {
        #[serde(with = "bytes")]
        config: Vec<u8>,
    },
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(Serialize, Deserialize)]
enum Outgoing {
    Configure {
        version: u32,
        options: Options,
        operations: Operations,
    },
    Ready {
        version: u32,
        options: Options,
        operations: Operations,
    },
    SetupFailed(WireError),
    Started {
        id: u64,
    },
    Answer {
        id: u64,
        #[serde(with = "bytes::result")]
        output: Result<Vec<u8>, WireError>,
    },
    Finished(Result<(), WireError>),
    SetupRetired,
    Unconfirmed(WireError),
    InitNotRun {
        version: u32,
        error: WireError,
    },
    SetupPending(String),
}

#[doc(hidden)]
pub async fn run_export<E, C>(
    export: E,
    state: &mut E::State,
    input: &[u8],
) -> Result<Vec<u8>, WorkerFailure>
where
    E: crate::dispatch::Export<C>,
    C: Codec<E::Input> + Codec<E::Output>,
{
    let input = <C as Codec<E::Input>>::decode(input).map_err(|mut error| {
        error.outcome = CallError::Discarded;
        error
    })?;

    let output = export.run(state, input).await;

    <C as Codec<E::Output>>::encode(&output).map_err(|mut error| {
        error.outcome = CallError::OutcomeUnknown;
        error.into()
    })
}
