use super::*;
use core::{future::poll_fn, ops::Deref, task::Poll};

pub fn channel<T>(value: T) -> (Sender<T>, Receiver<T>) {
    let sender = Sender {
        shared: Arc::new(Shared {
            state: Mutex::new(State {
                value,
                revision: 0,
                closed: false,
            }),
            changed: Arc::new(Event::default()),
        }),
    };
    let receiver = sender.subscribe();
    (sender, receiver)
}
impl<T> Sender<T> {
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            shared: self.shared.clone(),
            listener: Event::listen(&self.shared.changed),
            revision: self.shared.state.guard().revision,
        }
    }

    pub fn send_if_modified(&self, change: impl FnOnce(&mut T) -> bool) {
        {
            let mut state = self.shared.state.guard();
            if !change(&mut state.value) {
                return;
            }
            state.revision = state.revision.wrapping_add(1);
        }

        self.shared.changed.notify();
    }

    pub fn send_replace(&self, value: T) -> T {
        let previous = {
            let mut state = self.shared.state.guard();
            state.revision = state.revision.wrapping_add(1);
            core::mem::replace(&mut state.value, value)
        };
        self.shared.changed.notify();
        previous
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.state.guard().closed = true;
        self.shared.changed.notify();
    }
}
impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            listener: Event::listen(&self.shared.changed),
            revision: self.revision,
        }
    }
}
impl<T> Receiver<T> {
    pub fn has_changed(&self) -> Result<bool, ()> {
        let state = self.shared.state.guard();
        if state.closed {
            Err(())
        } else {
            Ok(state.revision != self.revision)
        }
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            state: self.shared.state.guard(),
        }
    }

    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let state = self.shared.state.guard();
        self.revision = state.revision;
        Ref { state }
    }

    pub async fn changed(&mut self) -> Result<(), ()> {
        poll_fn(|cx| {
            self.listener.register(cx);
            let state = self.shared.state.guard();

            if self.revision != state.revision {
                self.revision = state.revision;
                Poll::Ready(Ok(()))
            } else if state.closed {
                Poll::Ready(Err(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.state.value
    }
}
