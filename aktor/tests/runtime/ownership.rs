use super::*;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
};

struct Count(Arc<AtomicUsize>);
impl Drop for Count {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct SelfLinked {
    weak: WeakHandle<SelfLinked>,
}

#[tokio::test]
async fn weak_self() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handle, listener) = channel::<SelfLinked>(1).unwrap();
            let weak = handle.downgrade();
            let actor = tokio::task::spawn_local(listener.run(SelfLinked { weak: weak.clone() }));

            assert!(weak.upgrade().is_ok());
            drop(handle);

            let state = tokio::time::timeout(std::time::Duration::from_secs(1), actor)
                .await
                .unwrap()
                .unwrap();
            assert!(state.weak.upgrade().is_err());
        })
        .await
}

#[tokio::test]
async fn cancel() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handle, listener) = channel::<Vec<&'static str>>(1).unwrap();
            drop(call(&handle, |rows, ()| rows.push("unpolled"), ()));

            let mut first = Box::pin(call(
                &handle,
                |rows: &mut Vec<&'static str>, row| rows.push(row),
                "first",
            ));

            assert!(poll(first.as_mut()).is_pending());

            let input_drops = Arc::new(AtomicUsize::new(0));
            let mut canceled = Box::pin(call(
                &handle,
                |rows: &mut Vec<&'static str>, input: Count| {
                    drop(input);
                    rows.push("canceled");
                },
                Count(Arc::clone(&input_drops)),
            ));

            assert!(poll(canceled.as_mut()).is_pending());
            drop(canceled);

            assert_eq!(input_drops.load(Ordering::SeqCst), 1);

            let actor = tokio::task::spawn_local(listener.run(Vec::new()));
            first.await;
            drop(handle);

            assert_eq!(actor.await.unwrap(), ["first"]);
        })
        .await
}

#[tokio::test]
async fn closed() {
    let (handle, listener) = channel::<()>(1).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let reply = call(&handle, |_, input| drop(input), Count(drops.clone()))
        .send()
        .await;

    let mut waiting = call(&handle, |_, input| drop(input), Count(drops.clone()));
    assert!(poll(&mut waiting).is_pending());

    drop(listener);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(panics(reply).await);
    assert!(panics(waiting).await);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

struct Wake;
// Needs an owned waker so its destruction is observable.
#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for Wake {
    fn wake(self: Arc<Self>) {}
}

#[tokio::test]
async fn abandon() {
    let (handle, listener) = channel::<usize>(1).unwrap();
    let mut reply = call(&handle, |state, ()| *state += 1, ()).send().await;

    let wake = Arc::new(Wake);
    let weak = Arc::downgrade(&wake);
    let waker = Waker::from(wake);

    assert!(
        Pin::new(&mut reply)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(reply);

    assert!(weak.upgrade().is_none(), "abandoned reply retained a waker");

    drop(handle);
    assert_eq!(listener.run(0).await, 1);
}

#[tokio::test]
async fn output() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for finish_first in [false, true] {
                let (handle, mut listener) = channel::<()>(1).unwrap();
                let drops = Arc::new(AtomicUsize::new(0));
                let reply = call(&handle, |_, input| input, Count(drops.clone()))
                    .send()
                    .await;

                if finish_first {
                    listener.recv().await.unwrap().run(&mut ()).await;
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(reply);
                } else {
                    drop(reply);
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    listener.recv().await.unwrap().run(&mut ()).await;
                }

                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }

            let (handle, mut listener) = channel::<()>(1).unwrap();
            listener.failure = FailurePolicy::Unwind;

            let drops = Arc::new(AtomicUsize::new(0));
            let reply = call(
                &handle,
                |_, _input| panic!("operation failed"),
                Count(drops.clone()),
            )
            .send()
            .await;

            let task = tokio::task::spawn_local(listener.run(()));
            assert!(task.await.unwrap_err().is_panic());

            drop(reply);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
        .await
}
