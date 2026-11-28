use crate::AktorGroupError;
use alloc::string::String;
use core::error::Error;
use er::*;

#[cfg(any(
    feature = "local",
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    )
))]
#[derive(Er)]
#[er(format = "shutdown interrupted or discarded actor work")]
pub(crate) struct ShutdownCut;

#[cfg(feature = "local")]
#[derive(Er)]
#[er(format = "during owner teardown")]
pub(crate) struct DuringTeardown;

// Converting a branch with `into()` gives it the caller's location.
pub(crate) fn unlocated<E>(branch: ErBranch<E>) -> ErTree<E> {
    ErTree {
        top: branch.value,
        saved: branch.saved,
        nodes: branch.nodes,
        src_location: None,
    }
}

#[track_caller]
pub(crate) fn with_report<E: Error + 'static>(top: E, report: String) -> (E, String) {
    if report.trim().is_empty() {
        er_new!(top).er_split()
    } else {
        unlocated(top.er_save("report", report)).into()
    }
}

impl From<AktorGroupError> for (AktorGroupError, String) {
    #[track_caller]
    fn from(error: AktorGroupError) -> Self {
        if let AktorGroupError::StartFailed { report } = &error {
            let text = report.render(None);
            return unlocated(error.er_save("shutdown", text)).into();
        }

        er_new!(error).er_split()
    }
}

#[cfg(any(feature = "local", all(feature = "tokio", not(target_family = "wasm"))))]
#[derive(Er)]
#[er(format = "application task cut at shutdown grace")]
pub(crate) struct ApplicationCut;
