use super::*;
use aktor::message::CallError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Semaphore, oneshot};

#[tokio::test]
async fn saved_error() {
    let (handle, mut listener) = channel::<()>(1).unwrap();
    let mut reply = call(&handle, |_, ()| (), ()).admit().await.unwrap();

    drop(listener.recv().await.unwrap());
    let _output = reply.try_take();

    let outcome = tokio::time::timeout(Duration::from_secs(1), reply.receive_settled())
        .await
        .unwrap();

    assert_eq!(outcome, Err(CallError::Discarded));
}

struct Notice(Arc<Semaphore>, bool);
impl Drop for Notice {
    fn drop(&mut self) {
        self.0.add_permits(1);

        if self.1 {
            panic!("queued destructor");
        }
    }
}

#[tokio::test]
async fn settle_before_cleanup() {
    let dropped = Arc::new(Semaphore::new(0));
    let cleanup = dropped.clone();
    let mut group = AktorGroup::new();

    group.start().unwrap();

    let owner = group
        .spawn_async(AktorArgs::new(
            "settlement",
            async || Ok::<_, AktorSetupError>(()),
            move |_| {
                let cleanup = cleanup.clone();

                async move {
                    let _permit =
                        tokio::time::timeout(Duration::from_secs(1), cleanup.acquire_many(2))
                            .await
                            .map_err(|_| AktorCleanupError::new("queued calls still alive", ()))?;

                    Ok::<_, AktorCleanupError>(())
                }
            },
        ))
        .await
        .unwrap();

    let (entered, entering) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();

    call(
        &owner.handle,
        move |_, ()| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            panic!("operation failed");
        },
        (),
    )
    .cast()
    .await;

    entering.await.unwrap();

    for broken in [true, false] {
        call(
            &owner.handle,
            |_, notice| drop(notice),
            Notice(dropped.clone(), broken),
        )
        .cast()
        .await;
    }

    release.send(()).unwrap();
    let report = group.completion().await;

    assert!(
        !report.to_string().contains("queued calls still alive"),
        "{report}"
    );
}

struct StateDrop {
    dropped: Arc<Semaphore>,
    settled: Arc<AtomicBool>,
}
impl Drop for StateDrop {
    fn drop(&mut self) {
        self.settled
            .store(self.dropped.available_permits() == 2, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn generic_teardown() {
    for mode in 0..3 {
        let (handle, listener) = channel::<StateDrop>(3).unwrap();
        let dropped = Arc::new(Semaphore::new(0));
        let settled = Arc::new(AtomicBool::new(false));
        let state = StateDrop {
            dropped: dropped.clone(),
            settled: settled.clone(),
        };

        call(
            &handle,
            |_, ()| -> () {
                panic!("operation failed");
            },
            (),
        )
        .cast()
        .await;

        for broken in [true, false] {
            call(
                &handle,
                |_, notice| drop(notice),
                Notice(dropped.clone(), broken),
            )
            .cast()
            .await;
        }

        match mode {
            0 => {
                let _result = AssertUnwindSafe(listener.run(state)).catch_unwind().await;
            }
            1 => {
                let _result =
                    std::panic::catch_unwind(AssertUnwindSafe(|| drop(listener.run(state))));
            }
            _ => {
                let _result = std::thread::spawn(move || listener.run_blocking(state)).join();
            }
        }

        assert!(settled.load(Ordering::SeqCst));
    }
}
