use super::*;
use core::task::{Context, Poll};
use std::{
    sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
};

type TestPacket = Packet<fn(&mut (), ()), (), ()>;

struct LockCheckingWake {
    packet: Weak<TestPacket>,
    callbacks: Arc<AtomicUsize>,
}
impl LockCheckingWake {
    fn check(&self) {
        let packet = self.packet.upgrade().expect("packet still exists");
        assert!(packet.data.try_lock().is_some());
        self.callbacks.fetch_add(1, Ordering::SeqCst);
    }
}
impl Wake for LockCheckingWake {
    fn wake(self: Arc<Self>) {
        self.check();
    }
}
impl Drop for LockCheckingWake {
    fn drop(&mut self) {
        self.check();
    }
}

fn operation(_: &mut (), _: ()) {}

#[test]
fn wake() {
    let packet = Arc::new(TestPacket::new(operation, ()));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(LockCheckingWake {
        packet: Arc::downgrade(&packet),
        callbacks: Arc::clone(&callbacks),
    }));
    packet.data.lock().completion = Completion::Waiting(Some(waker));

    packet.finish(Ok(()));

    assert_eq!(callbacks.load(Ordering::SeqCst), 2);
}

#[test]
fn replace() {
    let packet = Arc::new(TestPacket::new(operation, ()));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let first_waker = Waker::from(Arc::new(LockCheckingWake {
        packet: Arc::downgrade(&packet),
        callbacks: Arc::clone(&callbacks),
    }));
    assert!(
        packet
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    drop(first_waker);

    assert!(
        packet
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    assert_eq!(callbacks.load(Ordering::SeqCst), 1);

    packet.finish(Ok(()));

    assert_eq!(
        packet.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}
