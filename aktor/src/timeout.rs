use alloc::boxed::Box;
use core::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// Timing out after admission stops waiting, the operation still belongs to Aktor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AktorTimeoutError {
    pub duration: Duration,
    pub admitted: bool,
}
impl fmt::Display for AktorTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor call timed out after {:?}", self.duration)
    }
}
impl core::error::Error for AktorTimeoutError {}

#[cfg(not(target_family = "wasm"))]
type Timer = Pin<Box<dyn Future<Output = ()> + Send>>;
#[cfg(target_family = "wasm")]
type Timer = Pin<Box<dyn Future<Output = ()>>>;

#[doc(hidden)]
pub struct WaitStatus {
    pub admitted: bool,
    pub stopping: bool,
}

/// The duration starts when you begin awaiting it.
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    duration: Duration,
    timer: Option<Timer>,
    clock: fn(Duration) -> Timer,
    status: fn(&F) -> WaitStatus,
}
impl<F> Timeout<F> {
    #[cfg(feature = "tokio")]
    #[doc(hidden)]
    pub fn native(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        #[cfg(not(target_family = "wasm"))]
        {
            Self::new(future, duration, status, |duration| {
                Box::pin(tokio::time::sleep(duration))
            })
        }
        #[cfg(target_family = "wasm")]
        {
            Self::browser(future, duration, status)
        }
    }

    #[cfg(all(
        any(feature = "tokio", feature = "wasm_browser_workers"),
        target_family = "wasm"
    ))]
    #[doc(hidden)]
    pub fn browser(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        Self::new(future, duration, status, |duration| {
            Box::pin(async move {
                let mut remaining =
                    duration.as_millis() + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
                loop {
                    let chunk = remaining.min(u128::from(u32::MAX)) as u32;
                    gloo_timers::future::TimeoutFuture::new(chunk).await;
                    remaining -= u128::from(chunk);
                    if remaining == 0 {
                        return;
                    }
                }
            })
        })
    }

    #[cfg(feature = "embassy")]
    #[doc(hidden)]
    pub fn local(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        Self::new(future, duration, status, |duration| {
            Box::pin(async move {
                let duration = embassy_time::Duration::from_micros(
                    duration.as_micros().min(u128::from(u64::MAX)) as u64,
                );
                embassy_time::Timer::after(duration).await;
            })
        })
    }

    #[cfg(any(feature = "tokio", feature = "embassy", target_family = "wasm"))]
    fn new(
        future: F,
        duration: Duration,
        status: fn(&F) -> WaitStatus,
        clock: fn(Duration) -> Timer,
    ) -> Self {
        Self {
            future: Box::pin(future),
            duration,
            timer: None,
            status,
            clock,
        }
    }
}
impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, AktorTimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let timer = this
            .timer
            .get_or_insert_with(|| (this.clock)(this.duration));

        if let Poll::Ready(value) = this.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(value));
        }

        let status = (this.status)(this.future.as_ref().get_ref());
        if status.stopping {
            return Poll::Pending;
        }
        if timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(AktorTimeoutError {
                duration: this.duration,
                admitted: status.admitted,
            }));
        }
        Poll::Pending
    }
}
