use super::*;
use core::num::NonZeroUsize;
use core::task::{Context, Poll};
use std::{
    sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
};

type TestPacket = Packet<fn(&mut (), ()), (), ()>;

#[tokio::test]
async fn admit_during_shutdown() {
    let (inner, _mailbox) = crate::queue::channel(NonZeroUsize::MIN).unwrap();
    let handle = crate::listener::Handle::<()>::new(inner);
    let request = call(&handle, |_, ()| (), ());
    handle.inner.admission.shutdown();

    assert!(matches!(
        core::pin::pin!(request.send())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(aktor::AktorDeliveryError {
            outcome: AktorDeliveryOutcome::NotAdmitted,
            ..
        }))
    ));
}

use crate::queue::latest::SERVICE_GATE;

#[test]
fn cancelled_sessions() {
    use crate::latest::{SendLatest, Session};

    let (inner, mailbox) = crate::queue::channel(NonZeroUsize::MIN).unwrap();
    let handle = crate::listener::Handle::<()>::new(inner);

    for _ in 0..1000 {
        let (sender, results) = (&handle).session(
            crate::operation::Operation {
                name: "cancelled",
                caller: core::panic::Location::caller(),
            },
            async |_, input: u32| input,
        );
        sender.send(9);
        drop((sender, results));
    }

    assert!(mailbox.receiver.try_recv().is_err());
}

#[tokio::test]
async fn latest_publication() {
    use crate::latest::{SendLatest, Session};
    use std::{sync::mpsc, time::Duration};

    let (inner, mut mailbox) = crate::queue::channel(NonZeroUsize::MIN).unwrap();
    let handle = crate::listener::Handle::<usize>::new(inner);
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
        mailbox.admission.shutdown();
        mailbox.receiver.close();
    }

    let mut state = 0;

    if entered {
        mailbox.receiver.recv().await.unwrap().run(&mut state).await;
    }

    drop(mailbox);

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
    let packet = Arc::new(TestPacket::new(operation, (), None));
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
    let packet = Arc::new(TestPacket::new(operation, (), None));
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

    assert!(matches!(
        packet.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
}
