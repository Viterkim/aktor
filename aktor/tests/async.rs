#![cfg(feature = "macros")]

use aktor::message::{CallError, call};
use aktor::owner::OwnerError;
use aktor::*;
use std::{cell::RefCell, rc::Rc, sync::Arc, thread, time::Duration};
use tokio::sync::oneshot;

struct State {
    values: Rc<RefCell<Vec<u32>>>,
    owner: thread::ThreadId,
    timer: std::pin::Pin<Box<tokio::time::Sleep>>,
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
    let database = Arc::new(
        Aktor::spawn_async(SpawnArgs {
            name: "async owner".into(),
            capacity: 2,
            failure: FailurePolicy::Unwind,
            setup: async || {
                let state = State {
                    values: Rc::new(RefCell::new(Vec::new())),
                    owner: thread::current().id(),
                    timer: Box::pin(tokio::time::sleep(Duration::from_millis(100))),
                };
                tokio::time::sleep(Duration::from_millis(1)).await;

                Ok::<_, ()>(state)
            },
            cleanup: move |state: State| {
                let observed = observed.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    assert_eq!(thread::current().id(), state.owner);
                    *observed.lock() = Some(state.values.borrow().clone());

                    Ok::<_, ()>(())
                }
            },
        })
        .await
        .unwrap(),
    );

    let (started, running) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    let first_handle = database.new_handle().new_handle();
    let first = tokio::spawn(async move { wait(&first_handle, started, blocked).await });
    running.await.unwrap();

    let mut second = finish::request(&*database).send().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err()
    );

    let closing = database.clone();
    let completion = database.completion.new_observer();
    let mut shutdown = tokio::spawn(async move { closing.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut shutdown)
            .await
            .is_err()
    );

    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());

    release.send(()).unwrap();
    assert_eq!(first.await.unwrap(), 1);
    assert_eq!(second.await, 2);
    completion.wait().await.unwrap();
    database.shutdown().await.unwrap();

    assert_eq!(*cleaned.lock(), Some(vec![1, 2]));
}

#[aktor]
async fn fail(state: &mut State) {
    tokio::task::yield_now().await;
    state.values.borrow_mut().push(9);
    panic!("after await");
}

#[tokio::test]
async fn panic_after_await() {
    let cleaned = Arc::new(parking_lot::Mutex::new(None));
    let observed = cleaned.clone();
    let database = Aktor::spawn_async(SpawnArgs {
        name: "async failure".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: async || {
            Ok::<_, ()>(State {
                values: Rc::new(RefCell::new(Vec::new())),
                owner: thread::current().id(),
                timer: Box::pin(tokio::time::sleep(Duration::from_millis(1))),
            })
        },
        cleanup: move |state: State| {
            let observed = observed.clone();
            async move {
                tokio::task::yield_now().await;
                *observed.lock() = Some(state.values.borrow().clone());

                Ok::<_, ()>(())
            }
        },
    })
    .await
    .unwrap();

    assert_eq!(
        fail::request(&database).checked().await,
        Err(CallError::OutcomeUnknown)
    );

    let error = database.completion.wait().await.unwrap_err();

    let OwnerError::Panicked(error) = &*error else {
        panic!("expected operation panic")
    };
    assert_eq!(
        error.payload.lock().downcast_ref::<&str>(),
        Some(&"after await")
    );
    assert_eq!(*cleaned.lock(), Some(vec![9]));
}

#[tokio::test]
async fn completion() {
    let database = Aktor::spawn(SpawnArgs {
        name: "owned cleanup".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, ()>(0usize),
        cleanup: |_| Err("database connection stayed open"),
    })
    .await
    .unwrap();

    let handle = database.new_handle();
    let completion = database.completion.new_observer();
    drop(database);

    let reply = call(
        &handle,
        |state, ()| {
            *state = 1;
            *state
        },
        (),
    )
    .send()
    .await;

    drop(handle);

    assert_eq!(reply.await, 1);

    let first = completion.wait().await.unwrap_err();
    let second = completion.wait().await.unwrap_err();
    assert!(Arc::ptr_eq(&first, &second));

    let OwnerError::Cleanup(error) = &*first else {
        panic!("expected cleanup error")
    };
    assert_eq!(error.errors[0].as_ref(), &"database connection stayed open");
    assert!(
        first
            .to_string()
            .contains("database connection stayed open")
    );
}
