use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Wake, Waker},
};

struct BrokenWake;
impl Wake for BrokenWake {
    fn wake(self: Arc<Self>) {
        panic!("completion waker failed");
    }
}

#[tokio::test]
async fn completion() {
    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "completion teardown".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, std::convert::Infallible>(()),
        cleanup: |_| Err("cleanup failed"),
    })
    .await
    .unwrap();

    let mut completion = handle.completion();
    let mut observing = Box::pin(completion.wait());
    let waker = Waker::from(Arc::new(BrokenWake));

    assert!(
        observing
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let _result = actor.shutdown().await;
    let error = thread.join_async().await.unwrap_err();

    assert_eq!(error.to_string(), "completion waker failed");

    let cleanup = actor.cleanup_errors();

    assert_eq!(cleanup.errors.len(), 1);
    assert_eq!(*cleanup.errors[0], "cleanup failed");
}

struct BrokenState {
    handle: Handle<BrokenState>,
    closed: Arc<AtomicBool>,
}
impl Drop for BrokenState {
    fn drop(&mut self) {
        self.closed
            .store(self.handle.is_closed(), Ordering::Relaxed);
        panic!("state destructor failed");
    }
}

#[tokio::test]
async fn state() {
    tokio::task::LocalSet::new()
        .run_until(async {
            if let Ok(case) = std::env::var("AKTOR_CHILD") {
                let cancelled = matches!(case.as_str(), "cancel" | "unpolled");
                let (handle, mut listener) = channel::<BrokenState>(1).unwrap();
                let closed = Arc::new(AtomicBool::new(false));
                let state = BrokenState {
                    handle: handle.new_handle(),
                    closed: closed.clone(),
                };

                listener.failure = FailurePolicy::shutdown(move |failure| {
                    assert!(
                        closed.load(Ordering::Relaxed),
                        "admission remained open during cleanup"
                    );
                    assert!(matches!(
                        (cancelled, failure.kind),
                        (true, FailureKind::Cancelled) | (false, FailureKind::Operation(_))
                    ));
                    eprintln!("HOOK_RAN_AFTER_STATE_DROP");
                });

                let task = if case == "blocking" {
                    tokio::task::spawn_blocking(move || listener.run_blocking(state))
                } else {
                    tokio::task::spawn_local(listener.run(state))
                };

                if cancelled {
                    if case == "cancel" {
                        call(&handle, |_, ()| (), ()).await;
                    }

                    task.abort();
                } else {
                    call(&handle, |_, ()| panic!("first failure"), ())
                        .cast()
                        .await;
                }

                assert!(task.await.is_err());
                return;
            }

            for case in ["task", "blocking", "cancel", "unpolled"] {
                let output = support::child("drop::state", case);
                let stderr = String::from_utf8_lossy(&output.stderr);

                assert!(output.status.success(), "{case}: {stderr}");
                assert!(
                    stderr.contains("HOOK_RAN_AFTER_STATE_DROP"),
                    "{case}: {stderr}"
                );
            }
        })
        .await
}

struct CleanupCapture {
    entered: Option<oneshot::Sender<()>>,
    released: Option<oneshot::Receiver<()>>,
}
impl Drop for CleanupCapture {
    fn drop(&mut self) {
        self.entered.take().unwrap().send(()).unwrap();
        self.released.take().unwrap().blocking_recv().unwrap();
        panic!("cleanup capture destructor failed");
    }
}

#[tokio::test]
async fn capture() {
    for cleanup_panics in [false, true] {
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let capture = CleanupCapture {
            entered: Some(entered),
            released: Some(released),
        };

        let (notified, notification) = std::sync::mpsc::channel();
        let (handle, actor, thread) = spawn(SpawnArgs {
            name: "cleanup capture".into(),
            capacity: 1,
            failure: FailurePolicy::shutdown(move |failure| {
                notified.send(failure.kind).unwrap();
            }),
            setup: || Ok::<_, std::convert::Infallible>(()),
            cleanup: move |_| {
                std::hint::black_box(&capture);

                if cleanup_panics {
                    panic!("cleanup failed");
                }

                Ok::<_, std::convert::Infallible>(())
            },
        })
        .await
        .unwrap();

        let mut stopping = Box::pin(actor.shutdown());

        assert!(poll(stopping.as_mut()).is_pending());
        entering.await.unwrap();

        let closed = handle.closed();
        let mut closed = std::pin::pin!(closed);
        let pending = poll(closed.as_mut()).is_pending();
        let shutdown_pending = poll(stopping.as_mut()).is_pending();

        release.send(()).unwrap();

        if shutdown_pending {
            let _result = stopping.await;
        } else {
            drop(stopping);
        }

        assert!(thread.join_async().await.is_err());
        assert!(
            shutdown_pending,
            "accepted shutdown returned during teardown"
        );
        assert!(
            pending,
            "completion was published before cleanup captures were dropped"
        );

        let phase = notification.try_recv().unwrap();

        assert!(matches!(
            (cleanup_panics, phase),
            (true, FailureKind::Cleanup) | (false, FailureKind::Teardown)
        ));
    }
}
