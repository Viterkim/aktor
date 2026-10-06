use super::*;
use crate::latest::{SendLatest, TypedSession};
use core::{
    future::poll_fn,
    ops::AsyncFnOnce,
    task::{Context, Poll, Waker},
};
use pin_project_lite::pin_project;
use std::sync::Mutex as SlotMutex;

struct Slot<I, O> {
    input: Option<I>,
    output: Option<O>,
    revision: u64,
    scheduled: bool,
    running: bool,
    receiver: bool,
    senders: usize,
    wake: Option<Waker>,
}

struct Shared<S, I, O, Fut> {
    slot: Arc<SlotMutex<Slot<I, O>>>,
    factory: fn(AktorTaskState<S>, I) -> Fut,
    operation: Operation,
    services: Arc<service::Services<S>>,
    kill: KillSwitch,
    _alive: mpsc::Sender<Message<S>>,
}
struct SlotOutput<I, O> {
    slot: Arc<SlotMutex<Slot<I, O>>>,
}
impl<I, O> SlotOutput<I, O> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        let replacement = cx.waker().clone();
        let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(output) = slot.output.take() {
            return Poll::Ready(Some(output));
        }

        if slot.senders == 0 && slot.input.is_none() && !slot.scheduled && !slot.running {
            return Poll::Ready(None);
        }

        let previous = slot.wake.replace(replacement);

        drop(slot);
        drop(previous);
        Poll::Pending
    }

    fn close(&self) {
        let (input, output, wake) = {
            let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());
            slot.receiver = false;
            (slot.input.take(), slot.output.take(), slot.wake.take())
        };

        drop((input, output, wake));
    }
}
impl<S, I, O, Fut> Shared<S, I, O, Fut>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn schedule(
        self: &Arc<Self>,
        slot: &mut Slot<I, O>,
        draining: bool,
    ) -> Result<bool, Message<S>> {
        if slot.scheduled || slot.running || !slot.receiver || slot.input.is_none() {
            return Ok(false);
        }

        let message = Message {
            operation: self.operation,
            job: Box::new(LatestJob {
                shared: self.clone(),
                finished: false,
                future: None,
                revision: 0,
                input: None,
            }),
        };

        self.services.push(message, draining)?;
        slot.scheduled = true;
        Ok(true)
    }
}

pub struct AktorTaskLatest<S, I, O, Fut> {
    shared: Arc<Shared<S, I, O, Fut>>,
}
impl<S, I, O, Fut> Clone for AktorTaskLatest<S, I, O, Fut> {
    fn clone(&self) -> Self {
        self.shared
            .slot
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .senders += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<S, I, O, Fut> Drop for AktorTaskLatest<S, I, O, Fut> {
    fn drop(&mut self) {
        let wake = {
            let mut slot = self
                .shared
                .slot
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            slot.senders = slot.senders.saturating_sub(1);
            slot.wake.take()
        };

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<S, I, O, Fut> SendLatest<I> for AktorTaskLatest<S, I, O, Fut>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn send(&self, input: I) {
        let mut slot = self
            .shared
            .slot
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if !slot.receiver || self.shared.kill.is_stopping() {
            return;
        }

        slot.revision = slot.revision.wrapping_add(1);

        let previous = slot.input.replace(input);
        let output = slot.output.take();
        let wake = slot.wake.take();
        let scheduled = self.shared.schedule(&mut slot, false);
        let rejected = if scheduled.is_err() {
            slot.input.take()
        } else {
            None
        };

        drop(slot);

        if matches!(scheduled, Ok(true)) {
            self.shared.services.wake.notify_one();
        }

        drop((previous, output, rejected, scheduled));

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

trait Output<O>: Send + Sync {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>>;
    fn close(&self);
}
impl<I: Send, O: Send> Output<O> for SlotOutput<I, O> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.poll(cx)
    }

    fn close(&self) {
        self.close();
    }
}

pub struct AktorTaskResults<O> {
    output: Arc<dyn Output<O>>,
    terminal: AktorTaskFuture<'static, AktorTaskStatus>,
    ended: Option<AktorTaskStatus>,
}
impl<O> AktorTaskResults<O> {
    pub async fn next(&mut self) -> Option<O> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        let output = self.output.poll(cx);

        if matches!(output, Poll::Ready(Some(_))) {
            return output;
        }

        let terminal = match self.ended {
            Some(ended) => Poll::Ready(ended),
            None => {
                let terminal = self.terminal.as_mut().poll(cx);

                if let Poll::Ready(ended) = terminal {
                    self.ended = Some(ended);
                }

                terminal
            }
        };

        match terminal {
            Poll::Ready(AktorTaskStatus::Finished) => match self.output.poll(cx) {
                Poll::Ready(Some(output)) => Poll::Ready(Some(output)),
                _ => Poll::Ready(None),
            },
            Poll::Ready(_) => Poll::Pending,
            Poll::Pending => output,
        }
    }
}
impl<O> futures_util::Stream for AktorTaskResults<O> {
    type Item = O;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.get_mut().poll_next(cx)
    }
}
impl<O> Drop for AktorTaskResults<O> {
    fn drop(&mut self) {
        self.output.close();
    }
}

pin_project! {
    struct LatestJob<S, I, O, Fut> {
        shared: Arc<Shared<S, I, O, Fut>>,
        finished: bool,
        revision: u64,
        // Keep input owned here if the before hook unwinds.
        input: Option<I>,
        #[pin]
        future: Option<Fut>,
    }

    impl<S, I, O, Fut> PinnedDrop for LatestJob<S, I, O, Fut> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if !*this.finished {
                let wake = {
                    let mut slot = this.shared.slot.lock().unwrap_or_else(|error| error.into_inner());
                    slot.scheduled = false;
                    slot.running = false;
                    slot.wake.take()
                };
                if let Some(wake) = wake {
                    wake.wake();
                }
            }
        }
    }
}
impl<S, I, O, Fut> Job<S> for LatestJob<S, I, O, Fut>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        hooks: &mut crate::operation::hooks::AktorHooks<S>,
        operation: Operation,
    ) -> Poll<()> {
        let mut this = self.project();

        if let Some(mut state) = state {
            let input = {
                let mut slot = this
                    .shared
                    .slot
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());

                slot.scheduled = false;

                if !slot.receiver {
                    *this.finished = true;
                    return Poll::Ready(());
                }

                let Some(input) = slot.input.take() else {
                    *this.finished = true;
                    return Poll::Ready(());
                };

                slot.running = true;
                *this.revision = slot.revision;
                input
            };

            *this.input = Some(input);
            hooks.before(&mut state, operation);

            let Some(input) = this.input.take() else {
                crate::message::consumed();
            };

            this.future.set(Some((this.shared.factory)(state, input)));
        }

        let Some(future) = this.future.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };
        let (mut state, output) = core::task::ready!(future.poll(cx));

        this.future.set(None);
        hooks.after(&mut state, operation);

        let (discarded, wake, scheduled) = {
            let mut slot = this
                .shared
                .slot
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            slot.running = false;

            let discarded = if slot.receiver && slot.revision == *this.revision {
                slot.output.replace(output)
            } else {
                Some(output)
            };

            let wake = slot.wake.take();
            let scheduled = this.shared.schedule(&mut slot, true);

            (discarded, wake, scheduled)
        };

        *this.finished = true;

        if matches!(scheduled, Ok(true)) {
            this.shared.services.wake.notify_one();
        }

        drop((discarded, scheduled));

        if let Some(wake) = wake {
            wake.wake();
        }

        Poll::Ready(())
    }
}

impl<S: Send + 'static, I: Send + 'static, O: Send + 'static, Role, Wire>
    TypedSession<S, I, O, AktorTaskState<S>, Role, Wire> for &AktorTask<S, Role>
{
    type Sender<Fut> = AktorTaskLatest<S, I, O, Fut>;
    type Results = AktorTaskResults<O>;

    fn session<F, Fut>(
        self,
        operation: Operation,
        _: F,
        factory: fn(AktorTaskState<S>, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: Future<Output = (AktorTaskState<S>, O)>,
    {
        let shared = Arc::new(Shared {
            slot: Arc::new(SlotMutex::new(Slot {
                input: None,
                output: None,
                revision: 0,
                scheduled: false,
                running: false,
                receiver: true,
                senders: 1,
                wake: None,
            })),
            factory,
            operation,
            services: self.services.clone(),
            kill: self.kill.clone(),
            _alive: self.sender.clone(),
        });

        let mut finished = self.finished.clone();
        let terminal = Box::pin(async move {
            loop {
                let outcome = *finished.borrow_and_update();

                if outcome != AktorTaskStatus::Running {
                    return outcome;
                }

                if finished.changed().await.is_err() {
                    return AktorTaskStatus::Failed;
                }
            }
        });

        (
            AktorTaskLatest {
                shared: shared.clone(),
            },
            AktorTaskResults {
                output: Arc::new(SlotOutput {
                    slot: shared.slot.clone(),
                }),
                terminal,
                ended: None,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Publish {
        slot: SlotOutput<(), u32>,
        finished: watch::Sender<AktorTaskStatus>,
        first: std::sync::atomic::AtomicBool,
    }
    impl Output<u32> for Publish {
        fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<u32>> {
            let output = self.slot.poll(cx);

            if self.first.swap(false, std::sync::atomic::Ordering::Relaxed) {
                assert!(output.is_pending());
                self.slot.slot.lock().unwrap().output = Some(85);
                self.finished.send_replace(AktorTaskStatus::Finished);
            }

            output
        }

        fn close(&self) {
            self.slot.close();
        }
    }

    #[test]
    fn terminal_output() {
        let (finished, mut terminal) = watch::channel(AktorTaskStatus::Running);
        let mut results = AktorTaskResults {
            output: Arc::new(Publish {
                slot: SlotOutput {
                    slot: Arc::new(SlotMutex::new(Slot {
                        input: None,
                        output: None,
                        revision: 0,
                        scheduled: false,
                        running: false,
                        receiver: true,
                        senders: 1,
                        wake: None,
                    })),
                },
                finished,
                first: std::sync::atomic::AtomicBool::new(true),
            }),
            terminal: Box::pin(async move {
                terminal.changed().await.unwrap();
                *terminal.borrow()
            }),
            ended: None,
        };

        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(results.poll_next(&mut cx), Poll::Ready(Some(85)));
        assert_eq!(results.poll_next(&mut cx), Poll::Ready(None));
        assert_eq!(results.poll_next(&mut cx), Poll::Ready(None));
    }
}
