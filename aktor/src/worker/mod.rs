use crate::message::CallError;
use er::Er;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub mod bytes;
mod codec;
mod wire;
#[doc(hidden)]
pub use wire::Codec;
mod impls;
pub use codec::{decode, encode};
mod registry;
#[doc(hidden)]
pub use inventory;
pub use registry::Operations;
#[doc(hidden)]
pub use registry::{Exporter, Register, Registered, Registration, dispatch};

#[derive(Er)]
pub enum TrySendError<R> {
    #[er(format = "actor mailbox is full")]
    Full(R),
    #[er(format = "actor closed")]
    Closed(R),
    #[er(format = "{1}")]
    Rejected(R, #[er(source)] WorkerError),
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod browser;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub use browser::{
    Completion, LatestResults, LatestSender, Server, Worker, WorkerReply, WorkerRequest, serve,
    serve_for, serve_setup, serve_with, setup_failed,
};

pub use crate::dispatch::{Native, Remote};

pub const VERSION: u32 = 2;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
type WireError = WorkerError<Vec<u8>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Options {
    /// Same on both sides. Change it when the wire data format changes.
    pub build: String,
    /// Ordinary calls waiting for replies, including the running call.
    pub capacity: usize,
    /// Ordinary admission budget. Oversized calls take the whole budget, latest sessions bypass it.
    pub max_outstanding_bytes: usize,
}

#[derive(Er, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[er(format = "worker call {outcome:?}: {cause:?}", no_constructors)]
pub struct WorkerError<T = ()> {
    pub outcome: CallError,
    pub cause: WorkerCause,
    #[er(skip)]
    pub data: Option<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerCause {
    Closed,
    Full,
    Codec(String),
    Setup(String),
    Cleanup(String),
    Crashed(String),
    Protocol,
    UnknownOperation(String),
    Operations {
        expected: Vec<String>,
        actual: Vec<String>,
    },
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
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

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
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
}

#[doc(hidden)]
pub async fn run_export<E, C>(
    export: E,
    state: &mut E::State,
    input: &[u8],
) -> Result<Vec<u8>, WorkerError>
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
        error
    })
}
