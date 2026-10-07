use crate::ActorFailure;

#[derive(Clone)]
pub enum StopSignal {
    Local(alloc::rc::Rc<dyn LocalStop>),
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "wasm_browser_workers",
        all(feature = "browser_local", target_family = "wasm")
    ))]
    Native(crate::KillSwitch),
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
            Self::Native(kill) => kill.is_stopping(),
        }
    }

    pub fn fail(&self, failure: ActorFailure) -> bool {
        match self {
            Self::Local(kill) => kill.fail(failure),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native(kill) => kill.fail(failure),
        }
    }

    pub fn fail_startup(&self, failure: ActorFailure) -> bool {
        match self {
            Self::Local(kill) => kill.fail_startup(failure),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native(kill) => kill.fail_startup(failure),
        }
    }
}
pub trait LocalStop {
    fn is_stopping(&self) -> bool;
    fn fail(&self, failure: ActorFailure) -> bool;
    fn fail_startup(&self, failure: ActorFailure) -> bool;
}
impl<Clock: super::clock::AktorGroupClock> LocalStop for super::group::KillSwitch<Clock> {
    fn is_stopping(&self) -> bool {
        self.is_stopping()
    }

    fn fail(&self, failure: ActorFailure) -> bool {
        self.fail(failure)
    }

    fn fail_startup(&self, failure: ActorFailure) -> bool {
        self.fail_startup(failure)
    }
}

impl<Clock: super::clock::AktorGroupClock> From<super::group::KillSwitch<Clock>> for StopSignal {
    fn from(kill: super::group::KillSwitch<Clock>) -> Self {
        Self::Local(alloc::rc::Rc::new(kill))
    }
}
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm")
))]
impl From<crate::KillSwitch> for StopSignal {
    fn from(kill: crate::KillSwitch) -> Self {
        Self::Native(kill)
    }
}
