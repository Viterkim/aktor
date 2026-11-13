use super::*;
use crate::AktorShutdownReport;

#[doc(hidden)]
pub trait AktorSetupGroup: Sized + Unpin {
    type AktorKillSwitch: Clone + Unpin;
    type Completion: Clone + Unpin + core::future::IntoFuture<Output = AktorShutdownReport>;

    fn new(grace: Duration) -> Self;
    fn registration(&self) -> Self;
    fn track_startup(&mut self) {}
    fn source_is_failure(_kill: &Self::AktorKillSwitch) -> bool {
        false
    }
    fn check_started(&self) -> Result<(), AktorGroupError>;
    fn killswitch(&self) -> Self::AktorKillSwitch;
    fn completion(&self) -> Self::Completion;
    fn stop(kill: &Self::AktorKillSwitch);
    fn fail_startup(kill: &Self::AktorKillSwitch, failure: crate::AktorFailureReport) -> bool;
    fn is_stopping(kill: &Self::AktorKillSwitch) -> bool;
    fn set_starting(kill: &Self::AktorKillSwitch, starting: bool) -> bool;
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
impl crate::AktorGroup {
    /// Start an actor in this group and get its handle back.
    /// Pass a tuple to start several actors and get their handles in the same order.
    ///
    /// ```rust,ignore
    /// let counter = group.spawn(AktorNew { .. }).await.er(())?;
    /// ```
    pub async fn spawn<Setup>(
        &self,
        setup: Setup,
    ) -> Result<Setup::Handles, (AktorStartError<Setup::Error>, String)>
    where
        Setup: AktorStart<Group = Self>,
    {
        aktor_start_in(self, setup).await
    }
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
    type AktorKillSwitch = crate::AktorKillSwitch;
    type Completion = crate::AktorGroupCompletion;

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

    fn source_is_failure(kill: &Self::AktorKillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), AktorGroupError> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::AktorKillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::AktorKillSwitch) {
        kill.stop();
    }

    fn fail_startup(kill: &Self::AktorKillSwitch, failure: crate::AktorFailureReport) -> bool {
        kill.fail_startup(failure)
    }

    fn is_stopping(kill: &Self::AktorKillSwitch) -> bool {
        kill.is_stopping()
    }

    fn set_starting(kill: &Self::AktorKillSwitch, starting: bool) -> bool {
        kill.set_starting(starting)
    }
}

#[cfg(feature = "local")]
impl<Clock: crate::local::clock::AktorGroupClock> crate::local::AktorGroup<Clock> {
    /// Start an actor in this group and get its handle back.
    /// Pass a tuple to start several actors and get their handles in the same order.
    pub async fn spawn<Setup>(
        &self,
        setup: Setup,
    ) -> Result<Setup::Handles, (AktorStartError<Setup::Error>, String)>
    where
        Setup: AktorStart<Group = Self>,
    {
        aktor_start_in(self, setup).await
    }
}

#[cfg(feature = "local")]
impl<Clock: crate::local::clock::AktorGroupClock> AktorSetupGroup
    for crate::local::AktorGroup<Clock>
{
    type AktorKillSwitch = crate::local::AktorKillSwitch<Clock>;
    type Completion = crate::local::AktorGroupCompletion<Clock>;

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

    fn source_is_failure(kill: &Self::AktorKillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), AktorGroupError> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::AktorKillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::AktorKillSwitch) {
        kill.stop();
    }

    fn fail_startup(kill: &Self::AktorKillSwitch, failure: crate::AktorFailureReport) -> bool {
        kill.fail_startup(failure)
    }

    fn is_stopping(kill: &Self::AktorKillSwitch) -> bool {
        kill.is_stopping()
    }

    fn set_starting(kill: &Self::AktorKillSwitch, starting: bool) -> bool {
        kill.set_starting(starting)
    }
}
