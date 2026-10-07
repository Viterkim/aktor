use super::AktorExecution;
use crate::{ActorOutcome, KillSwitch};
use tokio::sync::oneshot;

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
mod impls;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio_local;

pub use impls::start_local;

struct Completion {
    name: String,
    kind: AktorExecution,
    kill: KillSwitch,
    completed: Option<oneshot::Sender<ActorOutcome>>,
}
