use super::*;
use core::{future::poll_fn, task::Poll};

/// Wait for the last tick to finish so a busy actor never accumulates missed ticks.
pub async fn tick<M>(handle: Weak<HandleInner<M>>, mut interval: Interval<M>, clock: Clock) {
    loop {
        if let Some(deadline) = clock.deadline(interval.every) {
            deadline.wait().await;
        } else {
            core::future::pending::<()>().await;
        }

        let (due, state) = Due::new();
        let message = (interval.message)(due);
        let Some(handle) = handle.upgrade() else {
            return;
        };

        match handle.sender.commit_service(message, false) {
            Ok(ready) => ready.wake(),
            Err(_) => return,
        }

        drop(handle);

        poll_fn(|cx| {
            state.waker.register(cx.waker());

            match state.done() {
                true => Poll::Ready(()),
                false => Poll::Pending,
            }
        })
        .await;
    }
}
