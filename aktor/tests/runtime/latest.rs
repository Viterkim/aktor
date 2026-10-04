use super::*;
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
async fn idle_latest_observes_listener_drop() {
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
