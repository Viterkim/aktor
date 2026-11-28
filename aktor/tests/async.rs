#![cfg(feature = "macros")]

use aktor::owner::OwnerError;
use aktor::*;
use core::num::NonZeroUsize;
use std::{cell::RefCell, rc::Rc, sync::Arc, thread, time::Duration};
use tokio::sync::oneshot;

struct State {
    values: Rc<RefCell<Vec<u32>>>,
    owner: thread::ThreadId,
    timer: std::pin::Pin<Box<tokio::time::Sleep>>,
}

fn start<Actors: AktorStart<Group = AktorGroup>>(
    actors: Actors,
) -> AktorStarted<Actors::Handles, AktorGroup, Actors::Error> {
    aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors,
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
}

#[aktor]
async fn append(state: &mut State, value: u32) -> usize {
    assert_eq!(thread::current().id(), state.owner);
    state.values.borrow_mut().push(value);

    state.values.borrow().len()
}

#[aktor]
async fn wait(
    state: &mut State,
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) -> usize {
    let values = state.values.clone();

    started.send(()).unwrap();
    release.await.unwrap();

    assert!(Rc::ptr_eq(&values, &state.values));
    append(state, 1).await
}

#[aktor]
async fn finish(state: &mut State) -> usize {
    state.timer.as_mut().await;

    append(state, 2).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownership() {
    let cleaned = Arc::new(parking_lot::Mutex::new(None));
    let observed = cleaned.clone();
    let actors = start(AktorNew {
        name: "async owner".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || {
            let state = State {
                values: Rc::new(RefCell::new(Vec::new())),
                owner: thread::current().id(),
                timer: Box::pin(tokio::time::sleep(Duration::from_millis(100))),
            };

            tokio::time::sleep(Duration::from_millis(1)).await;

            state
        }),
        end: async move |state: State| {
            tokio::time::sleep(Duration::from_millis(1)).await;
            assert_eq!(thread::current().id(), state.owner);
            *observed.lock() = Some(state.values.borrow().clone());
        },
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(2).unwrap(),
        stuck: AktorStuck::Kill,
    });
    actors.ready().await.unwrap();
    let database = actors.handles();

    let (started, running) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    let first_handle = database.handle();
    let first = tokio::spawn(async move { wait(&first_handle, started, blocked).await });

    running.await.unwrap();

    let mut second = finish(database).send().await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err()
    );

    release.send(()).unwrap();
    assert_eq!(first.await.unwrap(), Ok(1));
    assert_eq!(second.await, Ok(2));

    let completion = database.completion();
    drop(actors);
    completion.wait().await.unwrap();

    assert_eq!(*cleaned.lock(), Some(vec![1, 2]));
}

#[aktor(latest)]
async fn fail(state: &mut State) {
    tokio::task::yield_now().await;
    state.values.borrow_mut().push(9);
    panic!("after await");
}

#[tokio::test]
async fn panic_after_await() {
    let cleaned = Arc::new(parking_lot::Mutex::new(None));
    let observed = cleaned.clone();
    let actors = start(AktorNew {
        name: "async failure".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || State {
            values: Rc::new(RefCell::new(Vec::new())),
            owner: thread::current().id(),
            timer: Box::pin(tokio::time::sleep(Duration::from_millis(1))),
        }),
        end: async move |state: State| {
            tokio::task::yield_now().await;
            *observed.lock() = Some(state.values.borrow().clone());

            Err::<(), _>(((), String::from("cleanup failed too")))
        },
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    });
    actors.ready().await.unwrap();
    let database = actors.handles();

    // A grouped actor's lost reply parks, so only send it.
    drop(fail(database).send().await.unwrap());

    let (error, report) = database.completion.wait().await.unwrap_err();

    assert!(error.physically_joined());

    let OwnerError::PanickedWithCleanup { cause } = &*error else {
        panic!("expected operation panic and cleanup failure")
    };

    assert_eq!(
        cause.payload.lock().downcast_ref::<&str>(),
        Some(&"after await")
    );

    assert!(report.contains("after await"));
    assert!(report.contains("cleanup failed too"));
    assert_eq!(*cleaned.lock(), Some(vec![9]));
}

#[tokio::test]
async fn completion() {
    let actors = start(AktorNew {
        name: "owned cleanup".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || 0usize),
        end: async |_| Err::<(), _>(((), String::from("database connection stayed open"))),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    });
    actors.ready().await.unwrap();

    let completion = actors.handles().completion();
    drop(actors);

    let (first, report) = completion.wait().await.unwrap_err();
    let (second, repeated) = completion.wait().await.unwrap_err();

    assert!(Arc::ptr_eq(&first, &second));
    assert!(matches!(&*first, OwnerError::Cleanup));
    assert!(report.contains("database connection stayed open"));
    assert!(repeated.contains("database connection stayed open"));
}
