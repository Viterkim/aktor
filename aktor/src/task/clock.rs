use crate::{Timeout, timeout::WaitStatus};
use core::time::Duration;

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

#[derive(Clone, Copy)]
pub enum TaskClock {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std,
    #[cfg(target_family = "wasm")]
    Browser,
}
impl TaskClock {
    pub async fn wait_for_force(self, kill: &crate::KillSwitch) {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if matches!(self, Self::Std) {
            kill.wait_for_force_on(true).await;
            return;
        }

        let _ = self;

        kill.wait_for_force().await;
    }

    pub fn timeout<F>(
        self,
        future: F,
        duration: Duration,
        status: fn(&F) -> WaitStatus,
    ) -> Timeout<F> {
        match self {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio => Timeout::native(future, duration, status),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => Timeout::standard(future, duration, status),
            #[cfg(target_family = "wasm")]
            Self::Browser => Timeout::browser(future, duration, status),
        }
    }

    pub fn deadline(self, duration: Duration) -> Option<TaskDeadline> {
        match self {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio => tokio::time::Instant::now()
                .checked_add(duration)
                .map(TaskDeadline::Tokio),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => Some(TaskDeadline::Std(crate::group::shutdown_deadline(
                std::time::Instant::now(),
                duration,
            ))),
            #[cfg(target_family = "wasm")]
            Self::Browser => Some(TaskDeadline::Browser(crate::group::shutdown_deadline(
                crate::group::Instant::now(),
                duration,
            ))),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskDeadline {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio(tokio::time::Instant),
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std(std::time::Instant),
    #[cfg(target_family = "wasm")]
    Browser(crate::group::Instant),
}
impl TaskDeadline {
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
