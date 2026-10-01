use crate::message::CallError;
use er::Er;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

mod impls;

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
    CheckedWorkerReply, Completion, Server, Worker, WorkerReply, WorkerRequest, serve, serve_with,
    setup_failed,
};

pub use crate::target::{Native, Remote};

pub const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Options {
    pub build: String,
    pub timeout_ms: u32,
    pub capacity: usize,
    pub max_payload_bytes: usize,
    pub max_outstanding_bytes: usize,
}

#[derive(Er, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[er(format = "worker call {outcome:?}: {cause:?}")]
pub struct WorkerError {
    pub outcome: CallError,
    pub cause: WorkerCause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerCause {
    Closed,
    Full,
    Timeout,
    PayloadTooLarge,
    Codec(String),
    Setup(String),
    Cleanup(String),
    Crashed(String),
    Protocol,
    UnknownOperation(String),
    Superseded,
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
#[derive(Serialize, Deserialize)]
enum Incoming {
    Call {
        id: u64,
        operation: String,
        input: String,
    },
    Shutdown,
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
#[derive(Serialize, Deserialize)]
enum Outgoing {
    Ready {
        version: u32,
        options: Options,
    },
    SetupFailed(WorkerError),
    Started {
        id: u64,
    },
    Answer {
        id: u64,
        output: Result<String, WorkerError>,
    },
    Finished(Result<(), WorkerError>),
}

pub fn encode<T: Serialize>(value: &T) -> Result<String, WorkerError> {
    serde_json::to_string(value).map_err(|error| {
        WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Codec(error.to_string()),
        )
    })
}

pub fn decode<T: DeserializeOwned>(value: &str) -> Result<T, WorkerError> {
    serde_json::from_str(value).map_err(|error| {
        WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Codec(error.to_string()),
        )
    })
}

#[doc(hidden)]
pub async fn run_export<E>(
    export: E,
    state: &mut E::State,
    input: &str,
) -> Result<String, WorkerError>
where
    E: crate::target::Export,
    E::Input: DeserializeOwned,
    E::Output: Serialize,
{
    let input = decode(input).map_err(|mut error| {
        error.outcome = CallError::Discarded;
        error
    })?;

    let output = export.run(state, input).await;

    encode(&output).map_err(|mut error| {
        error.outcome = CallError::OutcomeUnknown;
        error
    })
}

#[macro_export]
macro_rules! worker_routes {
    ($name:ident, $state:ty, [$($operation:path),+ $(,)?]) => {
        fn $name(state: &mut $state, operation: String, input: String)
            -> $crate::message::LocalFuture<'_, Result<String, $crate::worker::WorkerError>>
        {
            Box::pin(async move {
                $(
                    {
                        use $operation as exported;

                        if operation == exported::NAME {
                            return $crate::worker::run_export(exported::export(), state, &input).await;
                        }
                    }
                )+

                Err($crate::worker::WorkerError::new($crate::message::CallError::Discarded, $crate::worker::WorkerCause::UnknownOperation(operation)))
            })
        }
    };
}
