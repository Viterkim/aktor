use crate::AktorFailureReport;

#[derive(Clone)]
pub enum StopSignal {
    Local(alloc::rc::Rc<dyn LocalStop>),
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "wasm_browser_workers",
        all(feature = "browser_local", target_family = "wasm")
    ))]
    Native {
        kill: crate::AktorKillSwitch,
        standard: bool,
    },
}
impl StopSignal {
    pub fn is_stopping(&self) -> bool {
        match self {
            Self::Local(kill) => kill.is_stopping(),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.is_stopping(),
        }
    }

    /// Run an actor's `end` for at most one grace. A local group times its actors itself,
    /// it drops the whole owner when they're out of time.
    pub async fn end_within<F: Future>(&self, end: F) -> Option<F::Output> {
        match self {
            Self::Local(_) => Some(end.await),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, standard } => kill.end_within(end, *standard).await,
        }
    }

    pub async fn wait_stopping(&self) {
        match self {
            Self::Local(kill) => kill.stopping().await,
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.wait_stopping().await,
        }
    }

    pub fn fail(&self, failure: AktorFailureReport) -> bool {
        match self {
            Self::Local(kill) => kill.fail(failure),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.fail(failure),
        }
    }

    pub fn fail_startup(&self, failure: AktorFailureReport) -> bool {
        match self {
            Self::Local(kill) => kill.fail_startup(failure),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.fail_startup(failure),
        }
    }
}
pub trait LocalStop {
    fn is_stopping(&self) -> bool;
    fn stopping(&self) -> crate::message::LocalFuture<'static, ()>;
    fn fail(&self, failure: AktorFailureReport) -> bool;
    fn fail_startup(&self, failure: AktorFailureReport) -> bool;
}
impl<Clock: super::clock::AktorGroupClock> LocalStop for super::group::AktorKillSwitch<Clock> {
    fn is_stopping(&self) -> bool {
        self.is_stopping()
    }

    fn stopping(&self) -> crate::message::LocalFuture<'static, ()> {
        let kill = self.clone();

        alloc::boxed::Box::pin(async move { kill.wait_stopping().await })
    }

    fn fail(&self, failure: AktorFailureReport) -> bool {
        self.fail(failure)
    }

    fn fail_startup(&self, failure: AktorFailureReport) -> bool {
        self.fail_startup(failure)
    }
}

impl<Clock: super::clock::AktorGroupClock> From<super::group::AktorKillSwitch<Clock>>
    for StopSignal
{
    fn from(kill: super::group::AktorKillSwitch<Clock>) -> Self {
        Self::Local(alloc::rc::Rc::new(kill))
    }
}
