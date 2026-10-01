use alloc::{
    rc::{Rc, Weak},
    vec::Vec,
};
use core::{
    cell::RefCell,
    task::{Context, Waker},
};

#[derive(Default)]
pub struct Event {
    waiters: RefCell<Vec<Weak<RefCell<Option<Waker>>>>>,
}
impl Event {
    pub fn listen(&self) -> Listener<'_> {
        let waker = Rc::new(RefCell::new(None));
        self.waiters.borrow_mut().push(Rc::downgrade(&waker));

        Listener { event: self, waker }
    }

    pub fn notify(&self) {
        let wakers: Vec<_> = self
            .waiters
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|waiter| waiter.borrow_mut().take())
            .collect();

        for waker in wakers {
            waker.wake();
        }
    }
}

pub struct Listener<'a> {
    event: &'a Event,
    waker: Rc<RefCell<Option<Waker>>>,
}
impl Listener<'_> {
    pub fn register(&self, context: &Context<'_>) {
        let old = self.waker.replace(Some(context.waker().clone()));
        drop(old);
    }
}
impl Drop for Listener<'_> {
    fn drop(&mut self) {
        let weak = Rc::downgrade(&self.waker);
        self.event
            .waiters
            .borrow_mut()
            .retain(|waiter| !waiter.ptr_eq(&weak));
    }
}
