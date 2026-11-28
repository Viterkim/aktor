use super::{AktorStartReason, LaunchError, StartFailure};
use crate::AktorGroupError;
use alloc::{boxed::Box, rc::Rc};
use core::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use er::*;

struct Shared<T> {
    value: RefCell<Option<T>>,
    alive: Cell<bool>,
    wake: RefCell<Option<Waker>>,
}

pub struct Sender<T> {
    shared: Rc<Shared<T>>,
}
pub(crate) struct Startup<E>(Box<dyn FnOnce(Result<(), StartFailure<E>>)>);
impl<E> Startup<E> {
    pub fn send(self, value: Result<(), StartFailure<E>>) {
        (self.0)(value);
    }
}

pub(crate) fn startup<E: 'static>(
    aborted: impl FnOnce() -> bool + 'static,
    publish: impl FnOnce(Result<(), StartFailure<E>>) + 'static,
) -> Startup<E> {
    Startup(Box::new(move |result| {
        if result.is_ok() && aborted() {
            publish(Err(er_new!(
                LaunchError::new(AktorStartReason::Stopping),
                AktorGroupError::Stopping
            )
            .into()));
        } else {
            publish(result);
        }
    }))
}

pub struct Receiver<T> {
    shared: Rc<Shared<T>>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared {
        value: RefCell::new(None),
        alive: Cell::new(true),
        wake: RefCell::new(None),
    });

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}
impl<T> Sender<T> {
    pub fn send(self, value: T) {
        *self.shared.value.borrow_mut() = Some(value);
        self.shared.alive.set(false);

        let wake = self.shared.wake.borrow_mut().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.alive.set(false);

        let wake = self.shared.wake.borrow_mut().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<T> Future for Receiver<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(value) = self.shared.value.borrow_mut().take() {
            return Poll::Ready(Some(value));
        }

        if !self.shared.alive.get() {
            return Poll::Ready(None);
        }

        let previous = self.shared.wake.replace(Some(cx.waker().clone()));
        drop(previous);
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let wake = self.shared.wake.borrow_mut().take();
        drop(wake);
    }
}
