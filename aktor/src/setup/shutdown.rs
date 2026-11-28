#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
use crate::AktorGroup;
use crate::{AktorGroupError, AktorShutdownOutput, AktorShutdownReport};
use alloc::string::String;

#[doc(hidden)]
pub trait AktorShutdown<Cleanup, CleanupOutput, Mode> {
    fn on_shutdown(&self, cleanup: Cleanup) -> Result<(), (AktorGroupError, String)>;
}

#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
impl<Cleanup, Output, Mode> AktorShutdown<Cleanup, Output, Mode> for AktorGroup
where
    Cleanup: FnOnce(AktorShutdownReport) -> Output + Send + 'static,
    Output: AktorShutdownOutput<Mode>,
    Output::Future: Send + 'static,
{
    fn on_shutdown(&self, cleanup: Cleanup) -> Result<(), (AktorGroupError, String)> {
        AktorGroup::on_shutdown(self, cleanup)
    }
}
#[cfg(all(
    any(feature = "browser_local", feature = "wasm_browser_workers"),
    target_family = "wasm",
    target_os = "unknown"
))]
impl<Cleanup, Output, Mode> AktorShutdown<Cleanup, Output, Mode> for AktorGroup
where
    Cleanup: FnOnce(AktorShutdownReport) -> Output + 'static,
    Output: AktorShutdownOutput<Mode>,
    Output::Future: 'static,
{
    fn on_shutdown(&self, cleanup: Cleanup) -> Result<(), (AktorGroupError, String)> {
        AktorGroup::on_shutdown(self, cleanup)
    }
}

#[cfg(feature = "local")]
impl<Clock, Cleanup, Output, Mode> AktorShutdown<Cleanup, Output, Mode>
    for crate::local::AktorGroup<Clock>
where
    Clock: crate::local::clock::AktorGroupClock,
    Cleanup: FnOnce(AktorShutdownReport) -> Output + 'static,
    Output: AktorShutdownOutput<Mode>,
    Output::Future: 'static,
{
    fn on_shutdown(&self, cleanup: Cleanup) -> Result<(), (AktorGroupError, String)> {
        crate::local::AktorGroup::on_shutdown(self, cleanup)
    }
}
