use super::*;

#[tokio::test]
async fn registrations() {
    use aktor::latest::Session;
    use futures_util::Stream;
    use std::{
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
    };
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let operation = aktor::operation::Operation {
        name: "registration",
        caller: std::panic::Location::caller(),
    };
    let (handle, mut listener) = channel::<usize>(1).unwrap();
    let (_input, mut output) =
        (&handle).session(operation, async |_: &mut usize, input: usize| input);

    assert!(
        Pin::new(&mut output)
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let mut sessions = Vec::new();

    for _ in 0..512 {
        sessions.push((&handle).session(operation, async |_: &mut usize, input: usize| input));
    }

    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    listener.close();
    assert_eq!(wake.0.swap(0, Ordering::Relaxed), 1);

    #[cfg(feature = "local")]
    {
        let (handle, _owner) = local::channel::<usize, 1, ()>().unwrap();
        let (_input, mut output) =
            (&handle).session(operation, async |_: &mut usize, input: usize| input);

        assert!(
            Pin::new(&mut output)
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let mut sessions = Vec::new();

        for _ in 0..512 {
            sessions.push((&handle).session(operation, async |_: &mut usize, input: usize| input));
        }

        assert_eq!(wake.0.load(Ordering::Relaxed), 0);
        drop(handle.shutdown());
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    }
}
use aktor::message::call_async;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct Rows(u32, std::marker::PhantomPinned);
struct QueryError(u32);
struct Database {
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<Mutex<Vec<u32>>>,
}

#[aktor]
async fn search(db: &mut Database, query: u32) -> Result<Rows, QueryError> {
    db.calls.lock().unwrap().push(query);

    if query == 1 {
        db.started.notify_one();
        db.release.notified().await;
    }

    if query == 0 {
        Err(QueryError(query))
    } else {
        Ok(Rows(query, std::marker::PhantomPinned))
    }
}

#[tokio::test]
async fn current_results() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (handle, listener) = channel::<Database>(1).unwrap();
    let (input, mut output) = search(&handle, 1).latest();
    let owner = listener.run(Database {
        started: started.clone(),
        release: release.clone(),
        calls: calls.clone(),
    });

    let client = async move {
        started.notified().await;

        for query in 2..=1000 {
            input.send(query);
        }

        let ordinary = call(&handle, |_, ()| 17, ()).send().await;

        release.notify_one();
        assert_eq!(ordinary.await, 17);

        let Some(Ok(rows)) = output.next().await else {
            panic!("latest output missing");
        };

        assert_eq!(rows.0, 1000);
        assert_eq!(*calls.lock().unwrap(), [1, 1000]);

        input.send(0);

        let Some(Err(error)) = output.next().await else {
            panic!("ordinary error missing");
        };

        assert_eq!(error.0, 0);
        input.send(7);
        drop(input);

        let Some(Ok(rows)) = output.next().await else {
            panic!("final output missing");
        };

        assert_eq!(rows.0, 7);
        assert!(output.next().await.is_none());
        drop(output);
        drop(handle);
    };

    tokio::join!(owner, client);

    let (handle, listener) = channel::<Database>(1).unwrap();
    let (input, mut output) = search::latest(&handle);

    drop(listener);
    input.send(9);
    assert!(
        std::panic::AssertUnwindSafe(output.next())
            .catch_unwind()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn idle_close() {
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Wake, Waker},
    };

    struct IdleWake(AtomicBool);
    impl Wake for IdleWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (handle, listener) = channel::<Database>(1).unwrap();
    let (input, mut output) = search::latest(&handle);
    let wake = Arc::new(IdleWake(AtomicBool::new(false)));
    let waker = Waker::from(wake.clone());
    let mut next = Box::pin(output.next());

    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(listener);
    handle.closed().await;
    assert!(handle.is_closed());
    assert!(wake.0.load(Ordering::Acquire));

    let failure = tokio::time::timeout(
        Duration::from_millis(200),
        AssertUnwindSafe(next).catch_unwind(),
    )
    .await
    .expect("idle latest results did not observe listener destruction");

    assert!(failure.is_err());
    drop(input);
}

#[tokio::test]
async fn pause_and_shutdown() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered = started.clone();
    let finish = release.clone();
    let (handle, actor, owner) = spawn(SpawnArgs {
        name: "latest search".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: move || {
            Ok::<_, ()>(Database {
                started: entered,
                release: finish,
                calls: observed,
            })
        },
        cleanup: |_| Ok::<_, ()>(()),
    })
    .await
    .unwrap();

    let ordinary = call_async(
        &handle,
        async |state, ()| {
            state.calls.lock().unwrap().push(1);
            state.started.notify_one();
            state.release.notified().await;
        },
        (),
    )
    .send()
    .await;

    started.notified().await;

    let (input, mut output) = search::latest(&handle);

    input.send(2);

    let pause = actor.pause();

    tokio::pin!(pause);

    let waiting = pause.as_mut().now_or_never().is_none();

    release.notify_one();
    assert!(waiting);
    ordinary.await;
    pause.await.unwrap();
    assert_eq!(*calls.lock().unwrap(), [1]);
    assert!(output.next().now_or_never().is_none());

    let observed = calls.clone();

    actor
        .resume(move || {
            Ok(Database {
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                calls: observed,
            })
        })
        .await
        .unwrap();

    let Some(Ok(rows)) = output.next().await else {
        panic!("queued latest input missing after resume");
    };

    assert_eq!(rows.0, 2);

    actor.pause().await.unwrap();
    input.send(2);
    input.send(3);
    assert!(output.next().now_or_never().is_none());

    let observed = calls.clone();

    actor
        .resume(move || {
            Ok(Database {
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                calls: observed,
            })
        })
        .await
        .unwrap();

    let Some(Ok(rows)) = output.next().await else {
        panic!("resumed output missing");
    };

    assert_eq!(rows.0, 3);

    input.send(4);
    actor.shutdown().await.unwrap();

    let Some(Ok(rows)) = output.next().await else {
        panic!("shutdown lost its accepted input");
    };

    assert_eq!(rows.0, 4);
    assert!(output.next().await.is_none());
    assert_eq!(*calls.lock().unwrap(), [1, 2, 3, 4]);
    owner.join_async().await.unwrap().unwrap();
}

struct LatestWake {
    sender: aktor::message::LatestSender<usize>,
    entered: std::sync::atomic::AtomicBool,
}
impl std::task::Wake for LatestWake {
    fn wake(self: Arc<Self>) {
        if !self.entered.swap(true, std::sync::atomic::Ordering::SeqCst) {
            aktor::latest::SendLatest::send(&self.sender, 2);
        }
    }
}

#[test]
fn callbacks_can_reenter() {
    if std::env::var_os("AKTOR_CHILD").is_some() {
        use aktor::latest::{SendLatest, Session};
        use std::{
            future::Future,
            task::{Context, Waker},
        };

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (handle, mut listener) = channel::<usize>(1).unwrap();
                let (sender, mut output) = (&handle).session(
                    aktor::operation::Operation {
                        name: "callback",
                        caller: std::panic::Location::caller(),
                    },
                    async |state, input: usize| {
                        *state = input;
                        input
                    },
                );

                let wake = Arc::new(LatestWake {
                    sender: sender.clone(),
                    entered: std::sync::atomic::AtomicBool::new(false),
                });
                let waker = Waker::from(wake.clone());
                let mut receiver = Box::pin(listener.recv());

                assert!(
                    receiver
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );

                sender.send(1);
                assert!(wake.entered.load(std::sync::atomic::Ordering::SeqCst));
                drop(receiver);

                let mut state = 0;

                listener.try_recv().unwrap().run(&mut state).await;
                assert_eq!(output.next().await, Some(2));
                assert!(listener.try_recv().is_err());

                let waker = Waker::from(Arc::new(LatestWake {
                    sender: sender.clone(),
                    entered: std::sync::atomic::AtomicBool::new(true),
                }));
                let mut next = Box::pin(output.next());

                assert!(
                    next.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                drop(waker);
                assert!(
                    next.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(next);
                drop(listener);
                sender.send(3);
                assert!(
                    AssertUnwindSafe(output.next())
                        .catch_unwind()
                        .await
                        .is_err()
                );
            });

        return;
    }

    let output = support::child("latest::callbacks_can_reenter", "latest");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn replacement_drop() {
    use aktor::latest::{SendLatest, Session};

    struct Output(usize);
    impl Drop for Output {
        fn drop(&mut self) {
            if self.0 == 1 {
                panic!("unread output dropped");
            }
        }
    }

    let (handle, mut listener) = channel::<usize>(1).unwrap();
    let (input, mut output) = (&handle).session(
        aktor::operation::Operation {
            name: "replacement",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut usize, input: usize| Output(input),
    );

    input.send(1);
    listener.try_recv().unwrap().run(&mut 0).await;
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| input.send(2))).is_err());
    listener
        .try_recv()
        .expect("new input stranded after replaced output panicked")
        .run(&mut 0)
        .await;
    assert_eq!(output.next().await.unwrap().0, 2);
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_replacement_drop() {
    use aktor::latest::{SendLatest, Session};
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    struct Output(usize);
    impl Drop for Output {
        fn drop(&mut self) {
            if self.0 == 1 {
                panic!("unread output dropped");
            }
        }
    }

    let (handle, owner) = local::channel::<usize, 1, ()>().unwrap();
    let (input, mut output) = (&handle).session(
        aktor::operation::Operation {
            name: "replacement",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut usize, input: usize| Output(input),
    );

    let mut running = Box::pin(owner.run(0, async |_| Ok(())));

    input.send(1);
    assert!(
        running
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| input.send(2))).is_err());
    assert!(
        running
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    let value = tokio::time::timeout(Duration::from_millis(500), output.next())
        .await
        .expect("new input stranded")
        .unwrap();

    assert_eq!(value.0, 2);
    drop(handle.shutdown());
    running.await.unwrap();
}

#[cfg(feature = "embassy_cross_core")]
#[tokio::test]
async fn cross_core_publication() {
    use aktor::latest::Session;
    use core::{
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use std::sync::{Arc, atomic::Ordering};
    struct Output(u32);
    impl Drop for Output {
        fn drop(&mut self) {
            if self.0 == 1 {
                panic!("old output died");
            }
        }
    }

    let (handle, owner) = aktor::cross_core::channel::<()>(1).unwrap();
    let mut running = Box::pin(owner.run_with(async || Ok(()), async |_| Ok(())));
    let wake = Arc::new(crate::support::CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);

    assert!(running.as_mut().poll(&mut cx).is_pending());

    let (input, mut output) = Session::session(
        &handle,
        operation::Operation {
            name: "publication",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut (), value: u32| Output(value),
    );

    input.send(1);
    assert!(running.as_mut().poll(&mut cx).is_pending());
    assert!(running.as_mut().poll(&mut cx).is_pending());
    wake.0.store(0, Ordering::SeqCst);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| input.send(2))).is_err());
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "committed latest job lost its owner wake"
    );
    assert!(running.as_mut().poll(&mut cx).is_pending());

    let value = match futures_util::Stream::poll_next(Pin::new(&mut output), &mut cx) {
        Poll::Ready(Some(value)) => value,
        _ => panic!("replacement did not execute"),
    };

    assert_eq!(value.0, 2);
    drop(value);

    let completion = handle.shutdown();

    running.await.unwrap();
    completion.await.unwrap();
    drop(input);
}
