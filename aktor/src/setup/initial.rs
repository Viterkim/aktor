#[cfg(feature = "local")]
use super::StartFailure;
use super::{AktorStartReason, LaunchError};
use alloc::string::String;
use core::convert::Infallible;

/// Select an infallible or fallible initializer on an actor setup.
pub struct AktorInit<F>(pub(crate) F);

#[allow(non_snake_case)]
impl<F> AktorInit<F> {
    pub fn Try<Fut: core::future::Future>(
        start: F,
    ) -> AktorInit<impl FnOnce() -> TryState<Fut> + AsyncFnOnce() -> Fut::Output>
    where
        F: FnOnce() -> Fut,
    {
        AktorInit(move || TryState { future: start() })
    }

    #[allow(clippy::type_complexity)]
    pub fn Ready<S, Fut>(
        start: F,
    ) -> AktorInit<
        impl FnOnce() -> ReadyState<Fut> + AsyncFnOnce() -> Result<S, (Infallible, String)>,
    >
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = S>,
    {
        AktorInit(move || ReadyState { future: start() })
    }
}

pin_project_lite::pin_project! {
    #[doc(hidden)]
    pub struct ReadyState<F> {
        #[pin]
        future: F,
    }
}

impl<F: core::future::Future> core::future::Future for ReadyState<F> {
    type Output = Result<F::Output, (Infallible, String)>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.project().future.poll(cx).map(Ok)
    }
}

pin_project_lite::pin_project! {
    #[doc(hidden)]
    pub struct TryState<F> {
        #[pin]
        future: F,
    }
}

impl<F: core::future::Future> core::future::Future for TryState<F> {
    type Output = F::Output;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

/// Which actor in a setup tuple returned its initializer error, in the same order as the handles.
#[derive(Debug)]
pub enum AktorBranch<
    A,
    B,
    C = Infallible,
    D = Infallible,
    E = Infallible,
    F = Infallible,
    G = Infallible,
    H = Infallible,
> {
    First(A),
    Second(B),
    Third(C),
    Fourth(D),
    Fifth(E),
    Sixth(F),
    Seventh(G),
    Eighth(H),
}

impl<E> LaunchError<E> {
    pub(crate) fn new(reason: AktorStartReason) -> Self {
        Self {
            init: None,
            reason,
            failure_id: None,
        }
    }

    #[cfg(feature = "local")]
    #[track_caller]
    pub(crate) fn init((data, report): (E, String)) -> StartFailure<E>
    where
        E: 'static,
    {
        let error = Self {
            init: Some(data),
            ..Self::new(AktorStartReason::Init)
        };

        crate::report::with_report(error, report)
    }

    pub(crate) fn map_init<T>(self, f: impl FnOnce(E) -> T) -> LaunchError<T> {
        LaunchError {
            init: self.init.map(f),
            reason: self.reason,
            failure_id: self.failure_id,
        }
    }
}

impl crate::AktorGroupError {
    pub(crate) fn start_reason(&self) -> AktorStartReason {
        match self {
            Self::Stopping | Self::Stopped => AktorStartReason::Stopping,
            Self::NotStarted
            | Self::ShutdownTaken
            | Self::ListenerTaken
            | Self::NoTokio
            | Self::NoDefaultRuntime
            | Self::StartFailed { .. } => AktorStartReason::InvalidSetup,
        }
    }
}
