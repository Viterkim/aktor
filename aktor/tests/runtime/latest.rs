use super::*;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct Rows(u32);
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
        Ok(Rows(query))
    }
}

#[tokio::test]
async fn current_results() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (handle, listener) = channel::<Database>(1).unwrap();
    let (input, mut output) = search::latest(&handle);
    let owner = listener.run(Database {
        started: started.clone(),
        release: release.clone(),
        calls: calls.clone(),
    });
    let client = async move {
        input.send(1);
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
async fn pause_and_shutdown() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let (handle, actor, owner) = spawn(SpawnArgs {
        name: "latest search".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: move || {
            Ok::<_, ()>(Database {
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                calls: observed,
            })
        },
        cleanup: |_| Ok::<_, ()>(()),
    })
    .await
    .unwrap();
    let (input, mut output) = search::latest(&handle);
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
    assert_eq!(*calls.lock().unwrap(), [3, 4]);
    owner.join_async().await.unwrap().unwrap();
}
