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

std::thread_local! {
    static SERVICE_GATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub fn before_service() {
    let gate = SERVICE_GATE.with(|gate| gate.borrow_mut().take());
    if let Some(gate) = gate {
        gate();
    }
}

#[tokio::test]
async fn latest_publication() {
    use crate::latest::{SendLatest, Session};
    use std::{sync::mpsc, time::Duration};

    let (handle, mut listener) = crate::listener::channel::<usize>(1).unwrap();
    let (sender, results) = (&handle).session(
        crate::operation::Operation {
            name: "publication",
            caller: std::panic::Location::caller(),
        },
        async |state, input| {
            *state = input;
            input
        },
    );
    let first = sender.clone();
    let (entered, entering) = mpsc::channel();
    let (resume, resuming) = mpsc::channel();
    let sending = std::thread::spawn(move || {
        SERVICE_GATE.with(|gate| {
            *gate.borrow_mut() = Some(Box::new(move || {
                entered.send(()).unwrap();
                resuming.recv_timeout(Duration::from_secs(5)).unwrap();
            }));
        });
        first.send(1);
    });

    let entered = entering.recv_timeout(Duration::from_secs(5)).is_ok();
    if entered {
        sender.send(2);
        listener.close();
    }
    let mut state = 0;
    if entered {
        listener.serve(&mut state).await;
    }
    drop(listener);
    let _resumed = resume.send(());
    sending.join().unwrap();

    assert!(entered, "first sender never reached publication");
    assert_eq!(state, 2, "shutdown lost the completed latest send");
    drop(results);
}

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
