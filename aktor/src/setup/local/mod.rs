#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
mod impls;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio_local;

pub use impls::{closed_local_handle, launch_local, readiness};

use crate::{AktorKillSwitch, OwnerOutcome};
use tokio::sync::oneshot;

struct Completion {
    name: String,
    kill: AktorKillSwitch,
    completed: Option<oneshot::Sender<OwnerOutcome>>,
}
