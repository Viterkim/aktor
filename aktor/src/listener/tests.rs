use super::*;
use crate::message::call;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Wake, Waker},
};

struct ReenterAdmission {
    handle: Handle<usize>,
    called: AtomicBool,
}
impl Wake for ReenterAdmission {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.called.swap(true, Ordering::SeqCst) {
            call(&self.handle, |state, ()| *state += 1, ())
                .try_cast()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn receiver_waker_can_reenter_admission() {
    let (handle, mut listener) = spawn::channel::<usize>(2).unwrap();
    let wake = Arc::new(ReenterAdmission {
        handle: handle.new_handle(),
        called: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());

    let mut receiver = Box::pin(listener.recv());

    assert!(matches!(
        receiver.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    call(&handle, |state, ()| *state += 1, ())
        .try_cast()
        .unwrap();
    assert!(wake.called.load(Ordering::SeqCst));

    drop(receiver);

    let mut state = 0;

    for _ in 0..2 {
        listener.try_recv().unwrap().run(&mut state).await;
    }

    assert_eq!(state, 2);
}

#[tokio::test]
async fn outstanding_permit_does_not_keep_failed_runner_alive() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (handle, task) =
                spawn::spawn_local_with_policy(&executor, 0usize, 2, FailurePolicy::Unwind)
                    .unwrap();
            let permit = handle.inner.sender.reserve().await.unwrap();

            call(&handle, |_, ()| panic!("operation failed"), ())
                .cast()
                .await;

            assert!(task.await.is_err());
            drop(permit);

            let (handle, thread) =
                spawn::spawn_thread_with_policy(0usize, 2, FailurePolicy::Unwind).unwrap();
            let permit = handle.inner.sender.reserve().await.unwrap();

            call(&handle, |_, ()| panic!("operation failed"), ())
                .cast()
                .await;

            assert!(thread.join_async().await.is_err());
            drop(permit);

            let (handle, _, thread) = lifecycle::spawn::spawn(SpawnArgs {
                name: "permit".into(),
                capacity: 2,
                failure: FailurePolicy::Unwind,
                setup: || Ok::<_, ()>(0usize),
                cleanup: |_| Ok::<_, ()>(()),
            })
            .await
            .unwrap();

            let permit = handle.inner.sender.reserve().await.unwrap();

            call(&handle, |_, ()| panic!("operation failed"), ())
                .cast()
                .await;

            assert!(thread.join_async().await.is_err());
            drop(permit);

            let (handle, thread) = spawn::spawn_runner(
                SpawnArgs {
                    name: "custom permit".into(),
                    capacity: 2,
                    failure: FailurePolicy::Unwind,
                    setup: || Ok::<_, ()>(0usize),
                    cleanup: |_, _: Result<(), RunError<()>>| (),
                },
                |listener, state| {
                    listener.serve_blocking(state);
                    Ok(())
                },
            )
            .await
            .unwrap();

            let permit = handle.inner.sender.reserve().await.unwrap();

            call(&handle, |_, ()| panic!("operation failed"), ())
                .cast()
                .await;

            assert!(thread.join_async().await.is_err());
            drop(permit);
        })
        .await
}
