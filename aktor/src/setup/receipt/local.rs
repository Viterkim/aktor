use crate::local::event::Event;
use core::{
    cell::{RefCell, RefMut},
    future::poll_fn,
    ops::{Deref, DerefMut},
    task::Poll,
};

pub(super) struct Mutex<T> {
    value: RefCell<T>,
    available: Event,
}

impl<T> Mutex<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            value: RefCell::new(value),
            available: Event::default(),
        }
    }

    pub(super) async fn lock(&self) -> Guard<'_, T> {
        let available = self.available.listen();
        poll_fn(|cx| {
            available.register(cx);
            match self.value.try_borrow_mut() {
                Ok(value) => Poll::Ready(Guard {
                    value,
                    _notify: Notify(&self.available),
                }),
                Err(_) => Poll::Pending,
            }
        })
        .await
    }
}

pub(super) struct Guard<'a, T> {
    // Release the stage before waking waiters, even if the waiter driving startup was cancelled.
    value: RefMut<'a, T>,
    _notify: Notify<'a>,
}

struct Notify<'a>(&'a Event);

impl Drop for Notify<'_> {
    fn drop(&mut self) {
        self.0.notify();
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
