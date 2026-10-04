use super::*;
use crate::listener::Handle;
use crate::{
    latest::{SendLatest, Session},
    queue::Phase,
};
use core::{future::poll_fn, ops::AsyncFnOnce};
use std::sync::Weak;

pub struct LatestSender<I> {
    input: Arc<dyn Input<I>>,
}
impl<I> Clone for LatestSender<I> {
    fn clone(&self) -> Self {
        self.input.clone_sender();
        Self {
            input: self.input.clone(),
        }
    }
}
impl<I> Drop for LatestSender<I> {
    fn drop(&mut self) {
        self.input.close_sender();
    }
}
impl<I> SendLatest<I> for LatestSender<I> {
    fn send(&self, input: I) {
        self.input.send(input);
    }
}

pub struct LatestResults<O> {
    output: Arc<dyn Output<O>>,
}
impl<O> LatestResults<O> {
    pub async fn next(&mut self) -> Option<O> {
        poll_fn(|cx| self.output.poll(cx)).await
    }
}
impl<O> futures_util::Stream for LatestResults<O> {
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.output.poll(cx)
    }
}
impl<O> Drop for LatestResults<O> {
    fn drop(&mut self) {
        self.output.close_receiver();
    }
}

trait Input<I>: Send + Sync {
    fn send(&self, input: I);
    fn clone_sender(&self);
    fn close_sender(&self);
}

trait Output<O>: Send + Sync {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>>;
    fn close_receiver(&self);
}

struct Shared<S, I, O, F, Role> {
    handle: Handle<S, Role>,
    state: Mutex<Slot<I, O>>,
    function: Mutex<F>,
    operation: crate::operation::Operation,
    this: Weak<Self>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<I>,
    output: Option<O>,
    scheduled: bool,
    running: bool,
    senders: usize,
    receiver: bool,
    failed: bool,
    finished: bool,
    closing: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    phase: Phase,
    wake: Option<Waker>,
}

struct LatestJob<S, I, O, F, Role>(Arc<Shared<S, I, O, F, Role>>);

impl<S: 'static, I: Send + 'static, O: Send + 'static, Role, F> Shared<S, I, O, F, Role>
where
    Role: 'static,
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn schedule(&self) {
        let (publication, wake) = {
            let mut slot = self.state.lock();
            if slot.scheduled
                || slot.running
                || !slot.receiver
                || slot.failed
                || slot.pending.is_none()
                || matches!(slot.phase, Phase::Paused)
            {
                return;
            }
            let Some(this) = self.this.upgrade() else {
                return;
            };
            slot.scheduled = true;
            let publication = self.handle.inner.sender.commit_service(Message {
                operation: self.operation,
                job: Arc::new(LatestJob(this)),
                finished: false,
                counted: false,
            });
            let wake = if publication.is_err() {
                slot.scheduled = false;
                slot.failed = true;
                slot.wake.take()
            } else {
                None
            };
            (publication, wake)
        };

        #[cfg(test)]
        super::tests::before_service();

        match publication {
            Ok(ready) => ready.notify_one(),
            Err(mut message) => {
                message.finished = true;
                drop(message);
            }
        }
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn change(&self, _phase: Phase) {
        let mut slot = self.state.lock();
        let phase = self.handle.inner.admission.phase();
        slot.phase = phase;
        let pending = if let Phase::Closing { paused: true } = phase {
            slot.senders = 0;
            slot.pending.take()
        } else {
            None
        };
        let wake = slot.wake.take();

        drop(slot);
        drop(pending);
        self.schedule();

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static, F> Input<I>
    for Shared<S, I, O, F, Role>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn send(&self, input: I) {
        let mut slot = self.state.lock();

        if !slot.receiver || slot.failed || matches!(slot.phase, Phase::Closing { .. }) {
            return;
        }

        slot.revision = slot.revision.wrapping_add(1);
        let previous = slot.pending.replace(input);
        let unread = slot.output.take();
        let wake = slot.wake.take();

        drop(slot);
        drop(previous);
        drop(unread);
        self.schedule();

        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn clone_sender(&self) {
        self.state.lock().senders += 1;
    }

    fn close_sender(&self) {
        let mut slot = self.state.lock();
        slot.senders = slot.senders.saturating_sub(1);
        let wake = slot.wake.take();

        drop(slot);

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<S, I: Send, O: Send, F: Send, Role> Output<O> for Shared<S, I, O, F, Role> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        let replacement = cx.waker().clone();
        let mut slot = self.state.lock();

        if !slot.finished {
            let mut closing = slot.closing.take().unwrap_or_else(|| {
                let mut finished = self.handle.inner.finished.clone();
                Box::pin(async move {
                    let _result = finished.changed().await;
                })
            });
            drop(slot);
            let result = closing.as_mut().poll(cx);
            slot = self.state.lock();
            if result.is_pending() {
                slot.closing = Some(closing);
            } else {
                slot.finished = true;
                drop(slot);
                drop(closing);
                slot = self.state.lock();
            }
        }

        if !slot.failed
            && let Some(output) = slot.output.take()
        {
            return Poll::Ready(Some(output));
        }

        if slot.failed || (slot.finished && !matches!(slot.phase, Phase::Closing { .. })) {
            let finished = slot.finished;
            drop(slot);
            if !finished {
                return Poll::Pending;
            }

            if self.handle.inner.admission.group().is_some() {
                self.handle.inner.admission.lost();
                return Poll::Pending;
            }
            stopped();
        }

        if (slot.senders == 0 || matches!(slot.phase, Phase::Closing { .. }))
            && slot.pending.is_none()
            && !slot.running
            && !slot.scheduled
        {
            return Poll::Ready(None);
        }

        let previous = slot.wake.replace(replacement);
        drop(slot);
        drop(previous);
        Poll::Pending
    }

    fn close_receiver(&self) {
        let mut slot = self.state.lock();
        slot.receiver = false;
        let pending = slot.pending.take();
        let output = slot.output.take();
        let wake = slot.wake.take();

        drop(slot);
        drop(pending);
        drop(output);
        drop(wake);
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static, F> Job<S>
    for LatestJob<S, I, O, F, Role>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn run<'a>(&'a self, state: &'a mut S) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let request = {
                let mut slot = self.0.state.lock();
                slot.scheduled = false;
                if !slot.receiver || matches!(slot.phase, Phase::Paused) {
                    return;
                }
                let Some(input) = slot.pending.take() else {
                    return;
                };
                slot.running = true;
                (slot.revision, input)
            };

            let function = self.0.function.lock().clone();
            let output = function(state, request.1).await;

            let mut slot = self.0.state.lock();
            slot.running = false;
            let discarded = if slot.receiver && slot.revision == request.0 {
                slot.output.replace(output)
            } else {
                Some(output)
            };
            let wake = slot.wake.take();
            drop(slot);
            drop(discarded);
            self.0.schedule();
            if let Some(wake) = wake {
                wake.wake();
            }
        })
    }

    fn close(&self) {
        let mut slot = self.0.state.lock();
        slot.failed = true;
        let pending = slot.pending.take();
        let output = slot.output.take();
        slot.scheduled = false;
        let wake = slot.wake.take();

        drop(slot);
        drop(pending);
        drop(output);

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static> Session<S, I, O, Role>
    for &Handle<S, Role>
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(
        self,
        operation: crate::operation::Operation,
        function: F,
    ) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        let shared = Arc::new_cyclic(|this| Shared {
            handle: self.new_handle(),
            state: Mutex::new(Slot {
                revision: 0,
                pending: None,
                output: None,
                scheduled: false,
                running: false,
                senders: 1,
                receiver: true,
                failed: false,
                finished: false,
                closing: None,
                phase: Phase::Running,
                wake: None,
            }),
            function: Mutex::new(function),
            operation,
            this: this.clone(),
        });
        let weak = Arc::downgrade(&shared);
        self.inner
            .admission
            .register_session(Box::new(move |phase| {
                if let Some(session) = weak.upgrade() {
                    session.change(phase);
                    true
                } else {
                    false
                }
            }));
        (
            LatestSender {
                input: shared.clone(),
            },
            LatestResults { output: shared },
        )
    }
}
