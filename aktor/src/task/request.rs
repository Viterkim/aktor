use super::*;
use crate::{Timeout, message::consumed, timeout::WaitStatus};
use core::task::{Context, Poll};
use std::time::Duration;

type Admission<'a, S> =
    AktorTaskFuture<'a, Result<mpsc::Permit<'a, Message<S>>, mpsc::error::SendError<()>>>;
type Pending<S, I, Fut> = (I, fn(AktorTaskState<S>, I) -> Fut);

pub struct AktorTaskRequest<'a, S, I, O, Fut, Role = crate::AktorNoRole> {
    handle: &'a AktorTask<S, Role>,
    operation: Operation,
    pending: Option<Pending<S, I, Fut>>,
    admission: Option<Admission<'a, S>>,
    sender: Option<oneshot::Sender<O>>,
    reply: AktorTaskReply<O>,
    parked: bool,
}
// Input moves into the queue. No field exposes it as pinned.
impl<S, I, O, Fut, Role> Unpin for AktorTaskRequest<'_, S, I, O, Fut, Role> {}
impl<'a, S, I, O, Fut, Role> AktorTaskRequest<'a, S, I, O, Fut, Role> {
    pub fn new(
        handle: &'a AktorTask<S, Role>,
        operation: Operation,
        input: I,
        factory: fn(AktorTaskState<S>, I) -> Fut,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();

        Self {
            handle,
            operation,
            pending: Some((input, factory)),
            admission: None,
            sender: Some(sender),
            reply: AktorTaskReply {
                receiver,
                kill: handle.kill.clone(),
                admitted: false,
                parked: false,
                taken: false,
                clock: handle.clock,
            },
            parked: false,
        }
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Self> {
        self.handle
            .clock
            .timeout(self, duration, |request| WaitStatus {
                admitted: request.reply.admitted,
                stopping: request.handle.kill.is_stopping(),
            })
    }
}
impl<'a, S, I, O, Fut, Role> AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn submit(&mut self, send: impl FnOnce(Message<S>)) {
        let (Some(pending), Some(reply)) = (self.pending.take(), self.sender.take()) else {
            crate::message::consumed();
        };
        let (input, factory) = pending;

        send(Message {
            operation: self.operation,
            job: Box::new(Call {
                input: Some(input),
                factory,
                reply: Some(reply),
                future: None,
            }),
        });

        self.reply.admitted = true;
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.reply.taken {
            consumed();
        }

        if self.reply.admitted {
            return Poll::Ready(());
        }

        if self.parked || self.handle.kill.is_stopping() {
            self.admission = None;
            return Poll::Pending;
        }

        if self.admission.is_none()
            && let Ok(permit) = self.handle.sender.try_reserve()
        {
            #[cfg(feature = "tokio")]
            core::task::ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
            self.submit(|message| permit.send(message));
            return Poll::Ready(());
        }

        let admission = self
            .admission
            .get_or_insert_with(|| Box::pin(self.handle.sender.reserve()));
        let result = match admission.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };

        self.admission = None;

        match result {
            Ok(permit) => {
                self.submit(|message| {
                    permit.send(message);
                });
                Poll::Ready(())
            }
            Err(_) => {
                self.parked = true;
                self.handle.kill.stop();
                Poll::Pending
            }
        }
    }

    pub async fn send(mut self) -> AktorTaskReply<O> {
        core::future::poll_fn(|cx| self.poll_submit(cx)).await;
        self.reply
    }
}
impl<S, I, O, Fut, Role> Future for AktorTaskRequest<'_, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.poll_submit(cx).is_pending() {
            return Poll::Pending;
        }

        Pin::new(&mut this.reply).poll(cx)
    }
}

pub struct AktorTaskReply<O> {
    receiver: oneshot::Receiver<O>,
    kill: KillSwitch,
    admitted: bool,
    parked: bool,
    taken: bool,
    clock: TaskClock,
}
impl<O> AktorTaskReply<O> {
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        match self.receiver.try_recv() {
            Ok(output) => {
                self.taken = true;
                Some(output)
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.parked = true;
                self.kill.stop();
                None
            }
        }
    }

    pub fn timeout(&mut self, duration: Duration) -> Timeout<&mut Self> {
        self.clock.timeout(self, duration, |reply| WaitStatus {
            admitted: reply.admitted,
            stopping: reply.kill.is_stopping(),
        })
    }
}
impl<O> Future for AktorTaskReply<O> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        if self.parked {
            return Poll::Pending;
        }

        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Err(_)) => {
                self.parked = true;
                self.kill.stop();
                Poll::Pending
            }
            Poll::Ready(Ok(output)) => {
                self.taken = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(all(test, feature = "tokio", not(target_family = "wasm")))]
mod tests {
    use super::*;
    use crate::{
        AktorClosures, AktorKind, AktorName, AktorNew, AktorNewOptions, AktorNoRole, AktorOptions,
        AktorSetup, aktor_start,
    };
    use std::sync::{
        Mutex as Log,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    struct Input {
        id: usize,
        entered: Arc<Notify>,
        gate: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn work(
        mut state: AktorTaskState<Vec<usize>>,
        input: Input,
    ) -> (AktorTaskState<Vec<usize>>, ()) {
        state.push(input.id);
        input.entered.notify_one();
        input.gate.notified().await;
        (state, ())
    }

    #[tokio::test]
    async fn reserved_shutdown() {
        for full in [false, true] {
            let ended = Arc::new(Log::new(Vec::new()));
            let cleanup = ended.clone();
            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("reserved"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures {
                        start: async || Ok(Vec::<usize>::new()),
                        end: Some(
                            (async move |state| {
                                *cleanup.lock().unwrap() = state;
                                Ok(())
                            })
                            .into(),
                        ),

                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: AktorNewOptions { capacity: 1 },
                },
                shutdown: async |_| Ok::<_, aktor::AktorCleanupError>(()),
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap();

            let dropped = Arc::new(AtomicUsize::new(0));
            let input = |id| Input {
                id,
                entered: Arc::new(Notify::new()),
                gate: Arc::new(Notify::new()),
                dropped: dropped.clone(),
            };

            let operation = Operation {
                name: "reserved",
                caller: std::panic::Location::caller(),
            };
            let first = input(1);
            let running = first.entered.clone();
            let release_first = first.gate.clone();
            let second = input(2);
            let waiting = second.entered.clone();
            let release_second = second.gate.clone();

            if full {
                drop(
                    AktorTaskRequest::new(&actors.handles, operation, first, work)
                        .send()
                        .await,
                );

                running.notified().await;
                drop(
                    AktorTaskRequest::new(&actors.handles, operation, second, work)
                        .send()
                        .await,
                );
            } else {
                drop((first, second));
            }

            let third = input(3);

            third.gate.notify_one();

            let mut request = AktorTaskRequest::new(&actors.handles, operation, third, work);
            let mut reservation = Box::pin(actors.handles.sender.clone().reserve_owned());

            if full {
                assert!(
                    reservation
                        .as_mut()
                        .poll(&mut Context::from_waker(core::task::Waker::noop()))
                        .is_pending()
                );
                release_first.notify_one();
                waiting.notified().await;
            }

            let permit = reservation.await.unwrap();

            assert!(!request.reply.admitted);
            actors.killswitch().stop();
            actors.handles.closed().await;

            // Closing drains outstanding permits too. Input transfers on send.
            request.submit(|message| {
                permit.send(message);
            });
            assert!(request.reply.admitted);
            drop(request);
            release_second.notify_one();

            let report = tokio::time::timeout(Duration::from_secs(1), actors.shutdown())
                .await
                .unwrap();

            assert!(!report.failed(), "{report}");
            assert_eq!(
                *ended.lock().unwrap(),
                if full { vec![1, 2, 3] } else { vec![3] }
            );
            assert_eq!(dropped.load(Ordering::Relaxed), 3);
        }
    }
}
