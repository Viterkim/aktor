use super::*;
use core::num::NonZeroUsize;

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
    let (handle, _group) = thread_actor(0usize, 1).await;
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
    handle.shutdown().await.unwrap();
    assert!(wake.0.swap(0, Ordering::Relaxed) > 0);

    #[cfg(feature = "local")]
    {
        let (handle, _owner) = local::channel::<usize, ()>(NonZeroUsize::MIN);
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
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

struct Rows(u32, std::marker::PhantomPinned);
struct QueryError(u32);
struct Database {
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<Mutex<Vec<u32>>>,
}

#[aktor(latest)]
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

#[aktor]
async fn ordinary(_: &Database) -> usize {
    17
}

#[tokio::test]
async fn current_results() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (handle, _group) = thread_actor(
        Database {
            started: started.clone(),
            release: release.clone(),
            calls: calls.clone(),
        },
        1,
    )
    .await;
    let (input, mut output) = search(&handle, 1).latest();
    started.notified().await;

    for query in 2..=1000 {
        input.send(query);
    }

    let ordinary = ordinary(&handle).send().await.unwrap();

    release.notify_one();
    assert_eq!(ordinary.await, Ok(17));

    let Some(Ok(Ok(rows))) = output.next().await else {
        panic!("latest output missing");
    };

    assert_eq!(rows.0, 1000);
    assert_eq!(*calls.lock().unwrap(), [1, 1000]);

    input.send(0);

    let Some(Ok(Err(error))) = output.next().await else {
        panic!("ordinary error missing");
    };

    assert_eq!(error.0, 0);
    input.send(7);
    drop(input);

    let Some(Ok(Ok(rows))) = output.next().await else {
        panic!("final output missing");
    };

    assert_eq!(rows.0, 7);
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn grouped_stop_in_flight() {
    let entered = Arc::new(Notify::new());
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: support::thread_actor(
            Database {
                started: entered.clone(),
                release: Arc::new(Notify::new()),
                calls: Arc::default(),
            },
            1,
        ),
        shutdown: |_| {},
        grace: Duration::from_millis(20),
    });
    actors.ready().await.unwrap();

    let (input, mut output) = search(actors.handles(), 1).latest();
    entered.notified().await;
    let mut next = Box::pin(output.next());
    assert!(poll(next.as_mut()).is_pending());

    let report = actors.stop().await;
    let item = tokio::time::timeout(Duration::from_secs(1), next)
        .await
        .expect("latest parked after the actor stopped");
    let error = item.unwrap().err().unwrap();
    assert_eq!(error.outcome, aktor::AktorDeliveryOutcome::OutcomeUnknown);
    assert!(
        report
            .failure(error.failure_id.unwrap())
            .unwrap()
            .delivered()
    );
    assert!(output.next().await.is_none());
    drop(input);
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

    let (actor, _group) = thread_actor(0usize, 1).await;
    let (_input, mut output) = latest_value::latest(&actor);
    let wake = Arc::new(IdleWake(AtomicBool::new(false)));
    let waker = Waker::from(wake.clone());
    let mut next = Box::pin(output.next());

    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    actor.shutdown().await.unwrap();
    assert!(wake.0.load(Ordering::Acquire));
    assert_eq!(next.await, None);
}

#[tokio::test]
async fn idle_job_failure() {
    let (actor, _group) = thread_actor(0usize, 1).await;
    let (_input, mut output) = latest_value::latest(&actor);
    let job = actor
        .spawn(
            async { Err::<(), _>(((), String::from("write failed"))) },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();

    let error = output.next().await.unwrap().unwrap_err();
    let (AktorJobError::Failed(id), _) = job.await.unwrap_err() else {
        panic!("missing job failure");
    };
    assert_eq!(error.failure_id, Some(id));
    assert!(output.next().await.is_none());
}

#[aktor(latest)]
async fn latest_value(state: &usize, input: usize) -> usize {
    *state + input
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

    let (handle, _group) = thread_actor(0usize, 1).await;
    let (input, mut output) = (&handle).session(
        aktor::operation::Operation {
            name: "replacement",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut usize, input: usize| Output(input),
    );

    input.send(1);
    latest_value(&handle, 0).await.unwrap();
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| input.send(2))).is_err());
    assert_eq!(output.next().await.unwrap().unwrap().0, 2);
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

    let (handle, owner) = local::channel::<usize, ()>(NonZeroUsize::MIN);
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

    assert_eq!(value.unwrap().0, 2);
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

    let (handle, owner) = aktor::cross_core::channel::<()>(NonZeroUsize::MIN);
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

    assert_eq!(value.unwrap().0, 2);

    let completion = handle.shutdown();

    running.await.unwrap();
    completion.await.unwrap();
    drop(input);
}
