use super::*;
use alloc::vec::Vec;
use core::task::{Context, Waker};

pub struct Event {
    waiters: Lock<Vec<Weak<Lock<Option<Waker>>>>>,
}
impl Default for Event {
    fn default() -> Self {
        Self {
            waiters: Mutex::new(RefCell::new(Vec::new())),
        }
    }
}
impl Event {
    pub fn listen(event: &Arc<Self>) -> Listener {
        let waker = Arc::new(Mutex::new(RefCell::new(None)));

        event
            .waiters
            .lock(|waiters| waiters.borrow_mut().push(Arc::downgrade(&waker)));
        Listener {
            event: event.clone(),
            waker,
        }
    }

    pub fn notify(&self) {
        let wakers: Vec<_> = self.waiters.lock(|waiters| {
            waiters
                .borrow()
                .iter()
                .filter_map(Weak::upgrade)
                .filter_map(|waker| waker.lock(|waker| waker.borrow_mut().take()))
                .collect()
        });

        for waker in wakers {
            waker.wake();
        }
    }
}

pub struct Listener {
    event: Arc<Event>,
    waker: Arc<Lock<Option<Waker>>>,
}
impl Listener {
    pub fn register(&self, cx: &Context<'_>) {
        let replacement = cx.waker().clone();
        let previous = self
            .waker
            .lock(|waker| waker.borrow_mut().replace(replacement));

        drop(previous);
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        let weak = Arc::downgrade(&self.waker);

        self.event
            .waiters
            .lock(|waiters| waiters.borrow_mut().retain(|waiter| !waiter.ptr_eq(&weak)));
    }
}
