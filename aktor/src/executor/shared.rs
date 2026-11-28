//! The supervisor thread for builds without Tokio's runtime. Supervisors and observers mostly
//! wait, a thread each made a hundred small groups run hundreds of idle threads.

use core::{future::Future, pin::Pin};
use er::*;
use std::sync::Mutex;

type Work = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Er)]
pub(crate) enum SharedErr {
    Start,
    Stopped,
}

/// Every future gets polled on one thread, a panic in one of them only ends that one.
pub(crate) fn spawn(work: Work) -> ErResult<(), SharedErr> {
    use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
    use tokio::sync::mpsc;

    static SHARED: Mutex<Option<mpsc::UnboundedSender<Work>>> = Mutex::new(None);

    let mut shared = SHARED.lock().unwrap_or_else(|error| error.into_inner());

    // Each future runs under catch_unwind, the thread itself doesn't stop.
    if let Some(sender) = &*shared {
        if sender.send(work).is_err() {
            er_bail!(SharedErr::Stopped);
        }
        return Ok(());
    }

    let (sender, mut receiver) = mpsc::unbounded_channel::<Work>();

    std::thread::Builder::new()
        .name("aktor".into())
        .spawn(move || {
            super::block_on(async move {
                let mut running = FuturesUnordered::new();

                core::future::poll_fn(|cx| {
                    while let core::task::Poll::Ready(Some(work)) = receiver.poll_recv(cx) {
                        running.push(core::panic::AssertUnwindSafe(work).catch_unwind());
                    }

                    while let core::task::Poll::Ready(Some(finished)) = running.poll_next_unpin(cx)
                    {
                        if let Err(payload) = finished {
                            crate::panic::dispose_secondary(payload);
                        }
                    }

                    core::task::Poll::<()>::Pending
                })
                .await
            })
        })
        .er(|| SharedErr::Start)?;

    let sent = sender.send(work);

    *shared = Some(sender);
    if sent.is_err() {
        er_bail!(SharedErr::Stopped);
    }
    Ok(())
}
