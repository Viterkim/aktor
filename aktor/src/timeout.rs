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
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    #[doc(hidden)]
    pub fn standard(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        Self::new(future, duration, status, |duration| {
            let deadline = crate::group::shutdown_deadline(std::time::Instant::now(), duration);
            Box::pin(crate::executor::sleep_until(deadline))
        })
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    #[doc(hidden)]
    pub fn queued(
        future: F,
        duration: Duration,
        status: fn(&F) -> WaitStatus,
        standard: bool,
    ) -> Self {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return Self::standard(future, duration, status);
        }

        let _ = standard;

        #[cfg(feature = "tokio")]
        {
            Self::native(future, duration, status)
        }
        #[cfg(not(feature = "tokio"))]
        {
            Self::standard(future, duration, status)
        }
    }

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
        any(
            feature = "tokio",
            feature = "wasm_browser_workers",
            feature = "browser_local"
        ),
        target_family = "wasm"
    ))]
    #[doc(hidden)]
    pub fn browser(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        Self::new(future, duration, status, |duration| {
            Box::pin(browser_sleep(duration))
        })
    }

    #[cfg(feature = "embassy")]
    #[doc(hidden)]
    pub fn local(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Self {
        Self::new(future, duration, status, |duration| {
            Box::pin(async move {
                let deadline = embassy_deadline(embassy_time::Instant::now(), duration);
                embassy_time::Timer::at(deadline).await;
            })
        })
    }

    #[cfg(any(
        any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ),
        feature = "embassy",
        target_family = "wasm"
    ))]
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

#[cfg(any(
    all(
        target_family = "wasm",
        any(
            feature = "tokio",
            feature = "wasm_browser_workers",
            feature = "browser_local"
        )
    ),
    all(test, feature = "tokio")
))]
pub fn browser_millis(duration: Duration) -> u32 {
    let millis = duration.as_nanos().div_ceil(1_000_000);
    millis.min(i32::MAX as u128) as u32
}

#[cfg(any(
    all(
        target_family = "wasm",
        any(
            feature = "tokio",
            feature = "wasm_browser_workers",
            feature = "browser_local"
        )
    ),
    all(test, feature = "tokio")
))]
async fn browser_timer<F: Future<Output = ()>>(
    duration: Duration,
    mut schedule: impl FnMut(u32) -> F,
) {
    let mut remaining = duration;

    loop {
        let chunk = browser_millis(remaining);

        schedule(chunk).await;
        remaining = remaining.saturating_sub(Duration::from_millis(u64::from(chunk)));

        if remaining.is_zero() {
            return;
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn browser_delays() {
        let max = i32::MAX as u64;

        for (duration, expected) in [
            (Duration::ZERO, vec![0]),
            (Duration::from_nanos(1), vec![1]),
            (Duration::from_micros(1500), vec![2]),
            (Duration::from_millis(max), vec![max as u32]),
            (Duration::from_millis(max + 1), vec![max as u32, 1]),
            (
                Duration::from_millis(2 * max + 17),
                vec![max as u32, max as u32, 17],
            ),
        ] {
            let mut scheduled = Vec::new();

            browser_timer(duration, |chunk| {
                assert!(i32::try_from(chunk).is_ok());
                scheduled.push(chunk);
                core::future::ready(())
            })
            .await;
            assert_eq!(scheduled, expected);
        }
    }
}

#[cfg(feature = "embassy")]
pub fn embassy_deadline(now: embassy_time::Instant, grace: Duration) -> embassy_time::Instant {
    let ticks = grace
        .as_nanos()
        .saturating_mul(u128::from(embassy_time::TICK_HZ))
        .div_ceil(1_000_000_000)
        .min(u128::from(u64::MAX)) as u64;

    embassy_time::Instant::from_ticks(now.as_ticks().saturating_add(ticks))
}

#[cfg(all(test, feature = "embassy"))]
mod embassy_tests {
    use super::*;
    use embassy_time::Instant;

    #[test]
    fn deadlines() {
        let now = Instant::from_ticks(17);

        assert_eq!(embassy_deadline(now, Duration::ZERO), now);
        assert_eq!(
            embassy_deadline(now, Duration::from_nanos(1)).as_ticks(),
            18
        );
        assert_eq!(
            embassy_deadline(now, Duration::from_secs(5)),
            now + embassy_time::Duration::from_secs(5)
        );
        assert_eq!(embassy_deadline(now, Duration::MAX).as_ticks(), u64::MAX);
        assert_eq!(
            embassy_deadline(Instant::from_ticks(u64::MAX - 1), Duration::from_secs(1)).as_ticks(),
            u64::MAX
        );
    }
}

#[cfg(all(
    target_family = "wasm",
    any(
        feature = "tokio",
        feature = "wasm_browser_workers",
        feature = "browser_local"
    )
))]
#[doc(hidden)]
pub async fn browser_sleep(duration: Duration) {
    browser_timer(duration, gloo_timers::future::TimeoutFuture::new).await;
}
