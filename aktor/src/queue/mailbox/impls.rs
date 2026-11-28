use super::*;
use crate::queue::Service;
use core::{
    future::poll_fn,
    task::{Context, Poll},
};

impl Ready {
    pub fn wake(self) {
        if let Some(waker) = self.0 {
            waker.wake();
        }
    }
}

impl<M> Queue<M> {
    /// Something the owner has to see changed, it gets another look.
    fn wake(&self) {
        let waiting = self.entries.lock().waiting.take();

        Ready(waiting).wake();
    }

    /// Like `wake`, and the owner hears it even while a call runs.
    fn nudge(&self) {
        let (waiting, watching) = {
            let mut entries = self.entries.lock();

            entries.nudged = true;
            (entries.waiting.take(), entries.watching.take())
        };

        Ready(waiting).wake();
        Ready(watching).wake();
    }

    pub fn retire(&self) {
        let waiting = {
            let mut entries = self.entries.lock();

            entries.receiving = false;
            entries.waiting.take()
        };

        self.permits.close();
        Ready(waiting).wake();
    }
}

impl<'a, M> Permit<'a, M> {
    pub fn commit(self, message: M) -> Result<Ready, (Self, M)> {
        let mut entries = self.queue.entries.lock();

        if !entries.receiving || self.is_closed() {
            drop(entries);
            return Err((self, message));
        }

        entries.messages.push_back((message, true));
        self.permit.forget();

        let waiting = entries.waiting.take();

        drop(entries);
        Ok(Ready(waiting))
    }

    pub fn is_closed(&self) -> bool {
        self.queue.permits.is_closed()
    }
}

impl<M> Sender<M> {
    pub(crate) fn retire(&self) {
        self.0.retire();
    }

    pub fn cancel_service(&self, id: *const ())
    where
        M: Service,
    {
        let message = {
            let mut entries = self.0.entries.lock();
            let index = entries
                .messages
                .iter()
                .position(|(message, _)| message.service_id() == Some(id));
            index.and_then(|index| entries.messages.remove(index))
        };

        drop(message);
    }

    /// `draining` is a job rescheduling itself from inside the owner, which still takes work
    /// after admission closed. Anything from outside stops at the closed permits.
    pub fn commit_service(&self, message: M, draining: bool) -> Result<Ready, M> {
        let mut entries = self.0.entries.lock();

        if !entries.receiving || (!draining && self.0.permits.is_closed()) {
            return Err(message);
        }

        entries.messages.push_back((message, false));

        let waiting = entries.waiting.take();

        drop(entries);
        Ok(Ready(waiting))
    }

    pub async fn reserve(&self) -> Result<Permit<'_, M>, tokio::sync::AcquireError> {
        #[cfg(feature = "tokio")]
        tokio::task::coop::consume_budget().await;

        let permit = self.0.permits.acquire().await?;

        Ok(Permit {
            queue: &self.0,
            permit,
        })
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, M>, tokio::sync::TryAcquireError> {
        let permit = self.0.permits.try_acquire()?;

        Ok(Permit {
            queue: &self.0,
            permit,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.0.permits.is_closed()
    }

    #[cfg(any(feature = "tokio", not(target_family = "wasm")))]
    pub fn capacity(&self) -> usize {
        self.0.permits.available_permits()
    }

    /// The owner takes another look at its stop and force, also in the middle of a call.
    pub fn nudge(&self) {
        self.0.nudge();
    }
}
impl<M> Drop for Sender<M> {
    fn drop(&mut self) {
        self.0.senders_closed.store(true, Ordering::Release);
        self.0.wake();
    }
}

impl<M> Receiver<M> {
    pub(crate) fn retirer(&self) -> impl Fn() + 'static
    where
        M: 'static,
    {
        let queue = self.0.clone();
        move || queue.retire()
    }

    #[cfg(test)]
    pub async fn recv(&mut self) -> Option<M> {
        #[cfg(feature = "tokio")]
        tokio::task::coop::consume_budget().await;

        poll_fn(|cx| {
            loop {
                return match self.poll_next(cx) {
                    Poll::Ready(Received::Message(message)) => Poll::Ready(Some(message)),
                    Poll::Ready(Received::Closed) => Poll::Ready(None),
                    Poll::Ready(Received::Nudged) => continue,
                    Poll::Pending => Poll::Pending,
                };
            }
        })
        .await
    }

    /// The next message, or a nudge to look at the stop and force again.
    pub async fn next(&mut self) -> Received<M> {
        #[cfg(feature = "tokio")]
        tokio::task::coop::consume_budget().await;

        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Pops a message or registers the owner, under one lock. A sender that closes after the
    /// check takes the waker under the same lock, so the owner can't miss it.
    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Received<M>> {
        let result = {
            let mut entries = self.0.entries.lock();

            // A nudge goes before the queue. A force that lands while the owner was on its way
            // here gets seen before another call starts, instead of cutting it after one poll.
            if entries.nudged {
                entries.nudged = false;
                drop(entries);

                return Poll::Ready(Received::Nudged);
            }

            match self.pop(&mut entries) {
                Err(mpsc::error::TryRecvError::Empty) => {
                    let previous = match &entries.waiting {
                        Some(waiting) if waiting.will_wake(cx.waker()) => None,
                        Some(_) | None => entries.waiting.replace(cx.waker().clone()),
                    };

                    drop(entries);
                    drop(previous);

                    #[cfg(test)]
                    tests::after_empty();

                    return Poll::Pending;
                }
                result => result,
            }
        };

        Poll::Ready(match result {
            Ok(entry) => Received::Message(self.counted(entry)),
            Err(mpsc::error::TryRecvError::Disconnected) => Received::Closed,
            Err(mpsc::error::TryRecvError::Empty) => Received::Nudged,
        })
    }

    /// Ready on a nudge. Waits next to a running call, so new messages don't wake it.
    pub fn poll_nudge(&self, cx: &mut Context<'_>) -> Poll<()> {
        let previous = {
            let mut entries = self.0.entries.lock();

            if entries.nudged {
                entries.nudged = false;
                return Poll::Ready(());
            }

            match &entries.watching {
                Some(watching) if watching.will_wake(cx.waker()) => None,
                Some(_) | None => entries.watching.replace(cx.waker().clone()),
            }
        };

        drop(previous);
        Poll::Pending
    }

    fn pop(&self, entries: &mut Entries<M>) -> Result<(M, bool), mpsc::error::TryRecvError> {
        if !entries.receiving {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else if let Some(entry) = entries.messages.pop_front() {
            Ok(entry)
        } else if self.0.permits.is_closed() || self.0.senders_closed.load(Ordering::Acquire) {
            // The owner's last look. Nothing may land after it, it would never run.
            entries.receiving = false;
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }

    /// A counted message gives its queue slot back once it's out.
    fn counted(&self, (entry, counted): (M, bool)) -> M {
        if counted {
            self.0.permits.add_permits(1);
        }

        entry
    }

    #[cfg(test)]
    pub fn try_recv(&self) -> Result<M, mpsc::error::TryRecvError> {
        let result = self.pop(&mut self.0.entries.lock());

        #[cfg(test)]
        if result.is_err() {
            tests::after_empty();
        }

        result.map(|entry| self.counted(entry))
    }

    pub(crate) fn take_queued(&mut self) -> VecDeque<M> {
        let messages = {
            let mut entries = self.0.entries.lock();
            entries.receiving = false;
            std::mem::take(&mut entries.messages)
        };

        let counted = messages.iter().filter(|(_, counted)| *counted).count();
        self.0.permits.add_permits(counted);
        messages.into_iter().map(|(message, _)| message).collect()
    }

    pub fn close(&mut self) {
        self.0.permits.close();
        self.0.wake();
    }
}
impl<M> Drop for Receiver<M> {
    fn drop(&mut self) {
        self.close();

        drop(self.take_queued());
    }
}

pub fn channel<M>(permits: Arc<Semaphore>) -> (Sender<M>, Receiver<M>) {
    let queue = Arc::new(Queue {
        entries: Mutex::new(Entries {
            receiving: true,
            messages: VecDeque::new(),
            waiting: None,
            nudged: false,
            watching: None,
        }),
        permits,
        senders_closed: AtomicBool::new(false),
    });

    (Sender(queue.clone()), Receiver(queue))
}

#[cfg(test)]
mod tests {
    std::thread_local! {
        static EMPTY: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    }

    pub fn after_empty() {
        let action = EMPTY.with(|empty| empty.borrow_mut().take());

        if let Some(action) = action {
            action();
        }
    }

    #[tokio::test]
    async fn final_input() {
        let (sender, mut receiver) = super::channel(super::Arc::new(super::Semaphore::new(1)));

        EMPTY.with(|empty| {
            *empty.borrow_mut() = Some(Box::new(move || {
                let permit = sender.try_reserve().unwrap();
                let Ok(ready) = permit.commit(85) else {
                    panic!("queue closed");
                };
                ready.wake();
            }));
        });

        assert_eq!(receiver.recv().await, Some(85));
        assert_eq!(receiver.recv().await, None);
    }
}
