use super::*;
use crate::ShutdownReport;

#[doc(hidden)]
pub trait AktorSetupGroup: Sized + Unpin {
    type KillSwitch: Clone + Unpin;
    type Completion: Clone + Unpin;
    type Closing: Future<Output = ShutdownReport>;

    fn new(grace: Duration) -> Self;
    fn registration(&self) -> Self;
    fn track_startup(&mut self) {}
    fn source_is_failure(_kill: &Self::KillSwitch) -> bool {
        false
    }
    fn check_started(&self) -> Result<(), AktorSetupError>;
    fn killswitch(&self) -> Self::KillSwitch;
    fn completion(&self) -> Self::Completion;
    fn stop(kill: &Self::KillSwitch);
    fn is_stopping(kill: &Self::KillSwitch) -> bool;
    fn set_starting(kill: &Self::KillSwitch, starting: bool) -> bool;
    fn closing(&self) -> Self::Closing;
}

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
impl AktorSetupGroup for crate::AktorGroup {
    type KillSwitch = crate::KillSwitch;
    type Completion = crate::GroupCompletion;
    type Closing = AktorClosingFuture;

    fn new(grace: Duration) -> Self {
        let group = Self::with_grace(grace);

        group.killswitch().set_starting(true);
        group
    }

    fn registration(&self) -> Self {
        self.new_registration()
    }

    fn track_startup(&mut self) {
        self.track_startup();
    }

    fn source_is_failure(kill: &Self::KillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), AktorSetupError> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::KillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::KillSwitch) {
        kill.stop();
    }

    fn is_stopping(kill: &Self::KillSwitch) -> bool {
        kill.is_stopping()
    }

    fn set_starting(kill: &Self::KillSwitch, starting: bool) -> bool {
        kill.set_starting(starting)
    }

    fn closing(&self) -> Self::Closing {
        let completion = self.completion();
        Box::pin(async move { completion.wait().await })
    }
}

#[cfg(feature = "local")]
impl<Clock: crate::local::clock::AktorGroupClock> AktorSetupGroup
    for crate::local::AktorGroup<Clock>
{
    type KillSwitch = crate::local::KillSwitch<Clock>;
    type Completion = crate::local::GroupCompletion<Clock>;
    type Closing = crate::message::LocalFuture<'static, ShutdownReport>;

    fn new(grace: Duration) -> Self {
        let group = Self::with_grace(grace);

        group.killswitch().set_starting(true);
        group
    }

    fn registration(&self) -> Self {
        self.new_registration()
    }

    fn track_startup(&mut self) {
        self.track_startup();
    }

    fn source_is_failure(kill: &Self::KillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), AktorSetupError> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::KillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::KillSwitch) {
        kill.stop();
    }

    fn is_stopping(kill: &Self::KillSwitch) -> bool {
        kill.is_stopping()
    }

    fn set_starting(kill: &Self::KillSwitch, starting: bool) -> bool {
        kill.set_starting(starting)
    }

    fn closing(&self) -> Self::Closing {
        let completion = self.completion();
        Box::pin(async move { completion.wait().await })
    }
}
