//! `latest` sessions on the shared mailbox. A session keeps the newest input, puts one message in
//! the queue for it and keeps the newest output. Each kind's message only says how to run it.

use super::{HandleInner, Phase, Service};
use crate::{latest::SendLatest, message::AktorDeliveryOutcome, operation::Operation};
use core::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll, Waker},
};
use parking_lot::Mutex;
use std::sync::{Arc, OnceLock, Weak};

pub struct LatestSender<I> {
    input: Arc<dyn Input<I>>,
}

pub struct LatestResults<O> {
    output: Arc<dyn Output<O>>,
}

trait Input<I>: Send + Sync {
    fn send(&self, input: I);
    fn clone_sender(&self);
    fn close_sender(&self);
}

trait Output<O>: Send + Sync {
    fn poll(
        &self,
        cx: &mut Context<'_>,
        receive: bool,
    ) -> Poll<Option<Result<O, crate::AktorDeliveryError>>>;
    fn close_receiver(&self);
}

pub struct Session<M, I, O> {
    handle: Arc<HandleInner<M>>,
    state: Mutex<Slot<I, O>>,
    pub operation: Operation,
    /// A task session gets this on its first send, where its future's `Send` gets checked.
    message: OnceLock<Maker<M, I, O>>,
    this: Weak<Self>,
    phase_callback: Arc<dyn Fn(Phase) -> bool + Send + Sync>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<I>,
    output: Option<O>,
    scheduled: bool,
    running: bool,
    senders: usize,
    receiver: bool,
    failed: Option<crate::CallLoss>,
    finished: bool,
    closing: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    phase: Phase,
    wake: Option<Waker>,
}

type Maker<M, I, O> = Box<dyn Fn(Arc<Session<M, I, O>>) -> M + Send + Sync>;

#[cfg(test)]
std::thread_local! {
    pub static SERVICE_GATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

impl<M, I, O> Session<M, I, O>
where
    M: Service + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    pub fn open(
        handle: Arc<HandleInner<M>>,
        operation: Operation,
        message: Option<Maker<M, I, O>>,
    ) -> (Arc<Self>, LatestSender<I>, LatestResults<O>) {
        let session = Arc::new_cyclic(|this: &Weak<Self>| {
            let weak = this.clone();

            Self {
                handle,
                state: Mutex::new(Slot {
                    revision: 0,
                    pending: None,
                    output: None,
                    scheduled: false,
                    running: false,
                    senders: 1,
                    receiver: true,
                    failed: None,
                    finished: false,
                    closing: None,
                    phase: Phase::Running,
                    wake: None,
                }),
                operation,
                message: message.map(OnceLock::from).unwrap_or_default(),
                this: this.clone(),
                phase_callback: Arc::new(move |phase| {
                    weak.upgrade().is_some_and(|session| {
                        session.change(phase);
                        true
                    })
                }),
            }
        });

        session
            .handle
            .admission
            .register_session(session.phase_callback.clone());

        (
            session.clone(),
            LatestSender {
                input: session.clone(),
            },
            LatestResults { output: session },
        )
    }

    #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
    pub fn message(&self, message: impl FnOnce() -> Maker<M, I, O>) {
        self.message.get_or_init(message);
    }

    fn schedule(&self, draining: bool) {
        let (publication, wake, dropped) = {
            let mut slot = self.state.lock();

            if slot.scheduled
                || slot.running
                || !slot.receiver
                || slot.failed.is_some()
                || slot.pending.is_none()
            {
                return;
            }

            let (Some(this), Some(message)) = (self.this.upgrade(), self.message.get()) else {
                return;
            };

            slot.scheduled = true;

            let publication = self.handle.sender.commit_service(message(this), draining);

            let mut dropped = None;
            let wake = if publication.is_err() {
                slot.scheduled = false;

                // Shutdown got here before this session heard about it. The input is as late as
                // a send after close, so the stream just ends. A refused reschedule from inside
                // the owner had its input waiting before `retire`, that's lost work like a
                // discarded call.
                if !draining && self.handle.admission.phase().is_closing() {
                    slot.phase = Phase::Closing;
                    dropped = slot.pending.take();
                } else {
                    slot.failed = Some(crate::CallLoss(
                        if slot.running {
                            AktorDeliveryOutcome::OutcomeUnknown
                        } else {
                            AktorDeliveryOutcome::Discarded
                        },
                        self.handle.admission.lost.get().cloned(),
                    ));
                }

                slot.wake.take()
            } else {
                None
            };

            (publication, wake, dropped)
        };

        drop(dropped);

        #[cfg(test)]
        if let Some(gate) = SERVICE_GATE.with(|gate| gate.borrow_mut().take()) {
            gate();
        }

        // A refused message never ran and isn't scheduled anymore, so its drop changes nothing.
        match publication {
            Ok(ready) => ready.wake(),
            Err(message) => drop(message),
        }

        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn change(&self, _phase: Phase) {
        let mut slot = self.state.lock();

        slot.phase = self.handle.admission.phase();

        let wake = slot.wake.take();

        drop(slot);

        // A panic from scheduling must still wake the reader.
        let scheduled =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.schedule(false)));

        if let Some(wake) = wake {
            wake.wake();
        }

        if let Err(payload) = scheduled {
            std::panic::resume_unwind(payload);
        }
    }

    pub fn begin(&self) -> Option<(u64, I)> {
        let mut slot = self.state.lock();

        slot.scheduled = false;

        if !slot.receiver {
            return None;
        }

        let input = slot.pending.take()?;

        slot.running = true;
        Some((slot.revision, input))
    }

    /// The run finished, its output stays unless a newer input came in meanwhile.
    pub fn end(&self, revision: u64, output: O) {
        let mut slot = self.state.lock();

        slot.running = false;

        let discarded = if slot.receiver && slot.revision == revision {
            slot.output.replace(output)
        } else {
            Some(output)
        };

        let wake = slot.wake.take();

        drop(slot);
        drop(discarded);
        self.schedule(true);

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl<M, I, O> Session<M, I, O> {
    /// The message went without finishing, thrown out of the queue or cut while it ran.
    pub fn lost(&self) {
        let mut slot = self.state.lock();

        if !slot.scheduled && !slot.running {
            return;
        }

        slot.failed = Some(crate::CallLoss(
            if slot.running {
                AktorDeliveryOutcome::OutcomeUnknown
            } else {
                AktorDeliveryOutcome::Discarded
            },
            self.handle.admission.lost.get().cloned(),
        ));
        slot.scheduled = false;
        slot.running = false;

        let pending = slot.pending.take();
        let output = slot.output.take();
        let wake = slot.wake.take();

        drop(slot);
        drop(pending);
        drop(output);

        if let Some(wake) = wake {
            wake.wake();
        }
    }

    pub fn id(&self) -> *const () {
        self.this.as_ptr().cast()
    }
}

impl<M, I, O> Input<I> for Session<M, I, O>
where
    M: Service + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    fn send(&self, input: I) {
        let mut slot = self.state.lock();

        if !slot.receiver || slot.failed.is_some() || slot.phase.is_closing() {
            return;
        }

        slot.revision = slot.revision.wrapping_add(1);

        let previous = slot.pending.replace(input);
        let unread = slot.output.take();

        drop(slot);
        self.schedule(false);
        drop(previous);
        drop(unread);
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

impl<M, I, O> Output<O> for Session<M, I, O>
where
    M: Service + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    fn poll(
        &self,
        cx: &mut Context<'_>,
        receive: bool,
    ) -> Poll<Option<Result<O, crate::AktorDeliveryError>>> {
        let replacement = cx.waker().clone();
        let mut slot = self.state.lock();

        if !slot.receiver {
            return Poll::Ready(None);
        }

        if !slot.finished {
            let mut closing = slot.closing.take().unwrap_or_else(|| {
                let mut finished = self.handle.finished.clone();

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

        if slot.failed.is_none()
            && let Some(output) = slot.output.take()
        {
            return Poll::Ready(Some(Ok(output)));
        }

        let lost = slot.failed.take().or_else(|| {
            (slot.finished && (*self.handle.finished.borrow() || !slot.phase.is_closing())).then(
                || {
                    crate::CallLoss(
                        AktorDeliveryOutcome::OutcomeUnknown,
                        self.handle.admission.terminal.get().cloned(),
                    )
                },
            )
        });
        if let Some(outcome) = lost {
            slot.receiver = false;
            drop(slot);
            return Poll::Ready(Some(Err(outcome.error(receive))));
        }

        if (slot.senders == 0 || slot.finished)
            && slot.pending.is_none()
            && !slot.running
            && !slot.scheduled
        {
            slot.receiver = false;
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
        let scheduled = slot.scheduled;

        drop(slot);

        if scheduled {
            self.handle.sender.cancel_service(self.id());
        }

        drop(pending);
        drop(output);
        drop(wake);
    }
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

impl<O> LatestResults<O> {
    #[cfg(feature = "transport")]
    #[doc(hidden)]
    pub async fn forward(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        poll_fn(|cx| self.output.poll(cx, false)).await
    }

    pub async fn next(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        poll_fn(|cx| self.output.poll(cx, true)).await
    }
}
impl<O> futures_util::Stream for LatestResults<O> {
    type Item = Result<O, crate::AktorDeliveryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll(cx, true)
    }
}
impl<O> Drop for LatestResults<O> {
    fn drop(&mut self) {
        self.output.close_receiver();
    }
}
