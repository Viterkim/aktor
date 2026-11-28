use crate::AktorFailureInput;

#[derive(Clone)]
pub(crate) enum StopSignal {
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
    pub(crate) fn actor_id(&self) -> Option<crate::ActorId> {
        match self {
            Self::Local(kill) => kill.actor_id(),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.registered_id(),
        }
    }

    pub(crate) fn delivery(&self, id: crate::FailureId) -> Option<crate::Delivery> {
        match self {
            Self::Local(kill) => kill.delivery(id),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.delivery(id),
        }
    }

    pub(crate) fn driver_failure(&self) -> Option<crate::FailureId> {
        match self {
            Self::Local(kill) => kill.driver_failure(),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { .. } => None,
        }
    }

    pub(crate) fn init_evidence(&self, init: crate::AktorInitEvidence) {
        match self {
            Self::Local(kill) => kill.init_evidence(init),
            #[cfg(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "wasm_browser_workers",
                all(feature = "browser_local", target_family = "wasm")
            ))]
            Self::Native { kill, .. } => kill.init_evidence(init),
        }
    }

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

    /// Run an actor's `end` for at most one grace, counted from when `end` starts.
    pub async fn end_within<F: Future>(&self, end: F) -> Option<F::Output> {
        match self {
            Self::Local(kill) => within(end, kill.grace()).await,
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

    pub fn fail(&self, failure: AktorFailureInput) -> crate::FailureId {
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

    pub fn fail_startup(&self, failure: AktorFailureInput) -> crate::FailureId {
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
/// `end` against a timer, `None` when the timer wins.
pub async fn within<F: Future>(
    end: F,
    mut timer: crate::message::LocalFuture<'static, ()>,
) -> Option<F::Output> {
    let mut end = core::pin::pin!(end);

    core::future::poll_fn(|cx| {
        if let core::task::Poll::Ready(output) = end.as_mut().poll(cx) {
            return core::task::Poll::Ready(Some(output));
        }

        timer.as_mut().poll(cx).map(|()| None)
    })
    .await
}

pub(crate) trait LocalStop {
    fn actor_id(&self) -> Option<crate::ActorId>;
    fn delivery(&self, id: crate::FailureId) -> Option<crate::Delivery>;
    fn driver_failure(&self) -> Option<crate::FailureId>;
    fn init_evidence(&self, init: crate::AktorInitEvidence);
    fn is_stopping(&self) -> bool;
    fn stopping(&self) -> crate::message::LocalFuture<'static, ()>;
    /// Done one grace from now.
    fn grace(&self) -> crate::message::LocalFuture<'static, ()>;
    fn fail(&self, failure: AktorFailureInput) -> crate::FailureId;
    fn fail_startup(&self, failure: AktorFailureInput) -> crate::FailureId;
}
impl<Clock: super::clock::AktorGroupClock> LocalStop for super::group::AktorKillSwitch<Clock> {
    fn actor_id(&self) -> Option<crate::ActorId> {
        self.registered_id()
    }
    fn delivery(&self, id: crate::FailureId) -> Option<crate::Delivery> {
        self.delivery(id)
    }

    fn driver_failure(&self) -> Option<crate::FailureId> {
        self.driver_failure()
    }

    fn init_evidence(&self, init: crate::AktorInitEvidence) {
        self.init_evidence(init);
    }

    fn is_stopping(&self) -> bool {
        self.is_stopping()
    }

    fn stopping(&self) -> crate::message::LocalFuture<'static, ()> {
        let kill = self.clone();

        alloc::boxed::Box::pin(async move { kill.wait_stopping().await })
    }

    fn grace(&self) -> crate::message::LocalFuture<'static, ()> {
        self.wait_grace()
    }

    fn fail(&self, failure: AktorFailureInput) -> crate::FailureId {
        self.fail(failure)
    }

    fn fail_startup(&self, failure: AktorFailureInput) -> crate::FailureId {
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
