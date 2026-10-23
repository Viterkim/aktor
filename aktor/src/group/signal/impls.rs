use super::*;
use core::{future::poll_fn, task::Poll};

pub fn channel<T: Clone>(value: T) -> (Sender<T>, Receiver<T>) {
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
impl<T: Clone> Sender<T> {
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            shared: self.shared.clone(),
            listener: Event::listen(&self.shared.changed),
            revision: self.shared.state.guard().revision,
        }
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
impl<T: Clone> Receiver<T> {
    pub fn borrow(&self) -> T {
        self.shared.state.guard().value.clone()
    }

    pub fn borrow_and_update(&mut self) -> T {
        let state = self.shared.state.guard();
        self.revision = state.revision;
        state.value.clone()
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
