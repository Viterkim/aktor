use super::*;
use crate::latest::{SendLatest, Session};
use crate::message::{LatestSender, call};
use crate::queue;
use core::num::NonZeroUsize;
use futures_util::FutureExt;
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
                .cast()
                .now_or_never()
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn reentrant_wake() {
    let (handle, mut mailbox) = queue::channel(NonZeroUsize::new(2).unwrap()).unwrap();
    let handle = Handle::<usize>::new(handle);
    let wake = Arc::new(ReenterAdmission {
        handle: handle.clone(),
        called: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());

    let mut receiver = Box::pin(mailbox.receiver.recv());

    assert!(matches!(
        receiver.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    call(&handle, |state, ()| *state += 1, ())
        .cast()
        .await
        .unwrap();

    drop(receiver);

    let mut state = 0;

    for _ in 0..2 {
        mailbox.receiver.try_recv().unwrap().run(&mut state).await;
    }

    assert_eq!(state, 2);
}

#[test]
fn capacity() {
    let max = tokio::sync::Semaphore::MAX_PERMITS;

    assert!(matches!(
        queue::channel::<()>(NonZeroUsize::new(max + 1).unwrap()),
        Err(crate::message::AktorStateError::CapacityTooLarge)
    ));

    for capacity in [1, max] {
        assert!(queue::channel::<()>(NonZeroUsize::new(capacity).unwrap()).is_ok());
    }
}

struct LatestWake {
    sender: LatestSender<usize>,
    entered: AtomicBool,
}
impl Wake for LatestWake {
    fn wake(self: Arc<Self>) {
        if !self.entered.swap(true, Ordering::SeqCst) {
            self.sender.send(2);
        }
    }
}

#[tokio::test]
async fn latest_wake() {
    let (inner, mut mailbox) = queue::channel(NonZeroUsize::MIN).unwrap();
    let handle = Handle::<usize>::new(inner);
    let (sender, mut output) = (&handle).session(
        crate::operation::Operation {
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
        entered: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    let mut receiver = Box::pin(mailbox.receiver.recv());

    assert!(
        receiver
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    sender.send(1);
    drop(receiver);

    let mut state = 0;

    mailbox.receiver.try_recv().unwrap().run(&mut state).await;
    assert_eq!(output.next().await, Some(Ok(2)));
    assert!(mailbox.receiver.try_recv().is_err());
}

#[test]
fn secondary_panic_payload() {
    struct BrokenDrop;
    impl Drop for BrokenDrop {
        fn drop(&mut self) {
            panic!("secondary payload destructor");
        }
    }

    crate::panic::dispose_secondary(Box::new(BrokenDrop));
}
