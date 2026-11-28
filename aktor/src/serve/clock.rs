use crate::{AktorTimeout, timeout::WaitStatus};
use core::{future::Future, time::Duration};

pub async fn yield_owner() {
    #[cfg(target_family = "wasm")]
    crate::timeout::browser_sleep(Duration::ZERO).await;

    #[cfg(not(target_family = "wasm"))]
    {
        let mut yielded = false;

        core::future::poll_fn(|cx| {
            if yielded {
                core::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                core::task::Poll::Pending
            }
        })
        .await;
    }
}

/// The time an actor's host keeps, for its intervals, its `end` grace and call timeouts.
#[derive(Clone, Copy)]
pub enum Clock {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std,
    #[cfg(target_family = "wasm")]
    Browser,
}
impl Clock {
    pub async fn wait_for_force(self, kill: &crate::AktorKillSwitch) {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if matches!(self, Self::Std) {
            kill.wait_for_force_on(true).await;
            return;
        }

        let _ = self;

        kill.wait_for_force().await;
    }

    pub async fn end_within<F: Future>(
        self,
        kill: &crate::AktorKillSwitch,
        end: F,
    ) -> Option<F::Output> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if matches!(self, Self::Std) {
            return kill.end_within(end, true).await;
        }

        let _ = self;
        kill.end_within(end, false).await
    }

    pub fn timeout<F>(
        self,
        future: F,
        duration: Duration,
        status: fn(&F) -> WaitStatus,
    ) -> AktorTimeout<F> {
        match self {
            // The caller may not be on Tokio even when the actor is.
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio => AktorTimeout::queued(future, duration, status, false),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => AktorTimeout::standard(future, duration, status),
            #[cfg(target_family = "wasm")]
            Self::Browser => AktorTimeout::browser(future, duration, status),
        }
    }

    pub fn deadline(self, duration: Duration) -> Option<Deadline> {
        match self {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio => tokio::time::Instant::now()
                .checked_add(duration)
                .map(Deadline::Tokio),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => Some(Deadline::Std(crate::group::shutdown_deadline(
                std::time::Instant::now(),
                duration,
            ))),
            #[cfg(target_family = "wasm")]
            Self::Browser => Some(Deadline::Browser(crate::group::shutdown_deadline(
                crate::group::Instant::now(),
                duration,
            ))),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Deadline {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio(tokio::time::Instant),
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std(std::time::Instant),
    #[cfg(target_family = "wasm")]
    Browser(crate::group::Instant),
}
impl Deadline {
    pub async fn wait(self) {
        match self {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio(deadline) => tokio::time::sleep_until(deadline).await,
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std(deadline) => crate::executor::sleep_until(deadline).await,
            #[cfg(target_family = "wasm")]
            Self::Browser(deadline) => {
                let _elapsed =
                    crate::group::shutdown::bounded(deadline, core::future::pending::<()>()).await;
            }
        }
    }
}
