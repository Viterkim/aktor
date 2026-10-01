use super::*;
use aktor::listener::Listener;
use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[test]
fn concurrent() {
    if std::env::var("AKTOR_CHILD").as_deref() != Ok("concurrent latest") {
        let output = support::child("latest::concurrent", "concurrent latest");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    struct Key {
        value: u32,
        barrier: Option<Arc<Barrier>>,
        first: AtomicBool,
    }
    impl PartialEq for Key {
        fn eq(&self, other: &Self) -> bool {
            if self.first.swap(false, Ordering::SeqCst)
                && let Some(barrier) = &self.barrier
            {
                barrier.wait();
            }

            self.value == other.value
        }
    }
    impl Eq for Key {}

    fn append(state: &mut Vec<u32>, value: u32) {
        state.push(value);
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for immediate in [false, true] {
                let capacity = if immediate { 3 } else { 2 };
                let (handle, listener) = channel::<Vec<u32>>(capacity).unwrap();

                let first = call(&handle, append, 0)
                    .latest(Key {
                        value: 0,
                        barrier: None,
                        first: AtomicBool::new(false),
                    })
                    .try_send()
                    .unwrap()
                    .checked();

                let barrier = Arc::new(Barrier::new(2));
                let replies = std::thread::scope(|scope| {
                    let threads: Vec<_> = (1..=2)
                        .map(|value| {
                            let handle = &handle;
                            let barrier = barrier.clone();
                            scope.spawn(move || {
                                let request = call(handle, append, value).latest(Key {
                                    value: 1,
                                    barrier: Some(barrier),
                                    first: AtomicBool::new(true),
                                });

                                if immediate {
                                    request.try_send().unwrap().checked()
                                } else {
                                    tokio::runtime::Builder::new_current_thread()
                                        .enable_all()
                                        .build()
                                        .unwrap()
                                        .block_on(request.checked_send())
                                        .unwrap()
                                }
                            })
                        })
                        .collect();

                    threads
                        .into_iter()
                        .map(|thread| thread.join().unwrap())
                        .collect::<Vec<_>>()
                });

                assert_eq!(handle.capacity(), capacity - 2);
                drop(handle);

                let state = listener.run(Vec::new()).await;
                assert_eq!(state.len(), 2);
                assert_eq!(state[0], 0);
                first.await.unwrap();

                let mut superseded = 0;
                for reply in replies {
                    match reply.await {
                        Ok(()) => {}
                        Err(CallError::Superseded) => superseded += 1,
                        outcome => panic!("unexpected latest outcome: {outcome:?}"),
                    }
                }

                assert_eq!(superseded, 1);
            }
        });
}

#[test]
fn reentrant() {
    if std::env::var("AKTOR_CHILD").as_deref() != Ok("latest key") {
        let output = support::child("latest::reentrant", "latest key");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    struct Key {
        handle: WeakHandle<Vec<u32>>,
        listener: Option<Arc<Mutex<Listener<Vec<u32>>>>>,
    }
    impl PartialEq for Key {
        fn eq(&self, _: &Self) -> bool {
            let handle = self.handle.upgrade().unwrap();
            let listener = self.listener.clone();
            std::thread::spawn(move || {
                if let Some(listener) = listener {
                    drop(listener.lock().unwrap().try_recv().unwrap());
                }

                call(&handle, |state, value| state.push(value), 2)
                    .try_cast()
                    .unwrap();
            })
            .join()
            .unwrap();

            true
        }
    }
    impl Eq for Key {}

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for drain in [false, true] {
                let (handle, listener) = channel::<Vec<u32>>(2).unwrap();
                let listener = Arc::new(Mutex::new(listener));
                let key = || Key {
                    handle: handle.downgrade(),
                    listener: drain.then(|| listener.clone()),
                };

                let old = call(&handle, |state, value| state.push(value), 1)
                    .latest(key())
                    .try_send()
                    .unwrap()
                    .checked();

                let new = call(&handle, |state, value| state.push(value), 3)
                    .latest(key())
                    .try_send()
                    .unwrap();

                assert_eq!(
                    old.await,
                    Err(if drain {
                        CallError::Discarded
                    } else {
                        CallError::Superseded
                    })
                );

                let mut state = Vec::new();
                for _ in 0..2 {
                    let message = listener.lock().unwrap().try_recv().unwrap();
                    message.run(&mut state).await;
                }
                new.await;

                assert_eq!(state, [2, 3]);
            }
        });
}

#[tokio::test]
async fn reservation() {
    let (handle, mut listener) = channel::<Vec<u32>>(2).unwrap();

    fn append(state: &mut Vec<u32>, value: u32) {
        state.push(value);
    }

    let write = call(&handle, append, 0).try_send().unwrap();
    let old = call(&handle, append, 1)
        .latest("search")
        .try_send()
        .unwrap()
        .checked();

    let mut waiting = call(&handle, append, 2).latest("other search");
    assert!(poll(&mut waiting).is_pending());

    let mut state = Vec::new();
    listener.recv().await.unwrap().run(&mut state).await;
    write.await;

    let latest = waiting.latest("search").checked_send().await.unwrap();
    assert_eq!(old.await, Err(CallError::Superseded));
    assert_eq!(handle.capacity(), 1);

    listener.recv().await.unwrap().run(&mut state).await;
    latest.await.unwrap();
    assert_eq!(handle.capacity(), 2);
    assert_eq!(state, [0, 2]);
}

#[tokio::test]
async fn queue() {
    let (handle, mut listener) = channel::<Vec<u32>>(3).unwrap();

    let old = call(&handle, |state, value| state.push(value), 1)
        .latest("search")
        .checked_send()
        .await
        .unwrap();

    let write = call(&handle, |state, value| state.push(value), 2)
        .send()
        .await;

    let panel = call(&handle, |state, value| state.push(value), 3)
        .latest("other panel")
        .send()
        .await;

    let mut latest = call(&handle, |state, value| state.push(value), 4)
        .latest("search")
        .try_send()
        .unwrap()
        .checked();

    assert_eq!(old.await, Err(CallError::Superseded));

    for value in 5..8 {
        let next = call(&handle, |state, value| state.push(value), value)
            .latest("search")
            .try_send()
            .unwrap()
            .checked();

        assert_eq!(latest.await, Err(CallError::Superseded));
        latest = next;
    }

    let rejected = call(&handle, |state, value| state.push(value), 5)
        .latest("third panel")
        .try_send();
    assert!(matches!(rejected, Err(TrySendError::Full(_))));

    let mut state = Vec::new();
    for _ in 0..3 {
        listener.recv().await.unwrap().run(&mut state).await;
    }

    write.await;
    panel.await;
    latest.await.unwrap();

    assert_eq!(state, [2, 3, 7]);

    let running = call(&handle, |state, value| state.push(value), 6)
        .latest("search")
        .send()
        .await;
    let active = listener.recv().await.unwrap();
    let queued = call(&handle, |state, value| state.push(value), 7)
        .latest("search")
        .send()
        .await;

    active.run(&mut state).await;
    running.await;
    listener.recv().await.unwrap().run(&mut state).await;
    queued.await;

    assert_eq!(state, [2, 3, 7, 6, 7]);

    let old = call(&handle, |state, value| state.push(value), 8)
        .latest("search")
        .send()
        .await;

    let new = call(&handle, |state, value| state.push(value), 9)
        .latest("search")
        .try_send()
        .unwrap();

    listener.recv().await.unwrap().run(&mut state).await;
    new.await;

    let mut waiting = tokio::spawn(old);
    let result = tokio::time::timeout(Duration::from_secs(1), &mut waiting).await;

    if result.is_err() {
        waiting.abort();
        let _result = waiting.await;
    }

    assert!(matches!(result, Ok(Err(error)) if error.is_panic()));
    assert_eq!(state, [2, 3, 7, 6, 7, 9]);
}
