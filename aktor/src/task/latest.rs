use super::*;
use crate::{
    latest::{SendLatest, TypedSession},
    queue::latest::{self, LatestResults, LatestSender},
};
use core::ops::AsyncFnOnce;

type TaskSession<S, I, O> = latest::Session<Message<S>, I, O>;

pub type AktorTaskResults<O> = LatestResults<O>;

/// Sends to a task actor's `latest` session. The future gets its `Send` check on `send`.
pub struct AktorTaskLatest<S, I, O, Fut> {
    sender: LatestSender<I>,
    session: Arc<TaskSession<S, I, O>>,
    factory: fn(AktorTaskState<S>, I) -> Fut,
}

pin_project! {
    /// Runs a task `latest` session's newest input against the lease.
    struct LatestJob<S, I, O, Fut> {
        session: Arc<TaskSession<S, I, O>>,
        factory: fn(AktorTaskState<S>, I) -> Fut,
        revision: u64,
        finished: bool,
        // Held here while the before hook runs, so a hook that unwinds doesn't drop it mid-panic.
        input: Option<I>,
        #[pin]
        future: Option<Fut>,
    }
    impl<S, I, O, Fut> PinnedDrop for LatestJob<S, I, O, Fut> {
        fn drop(this: Pin<&mut Self>) {
            if !this.finished {
                this.session.lost();
            }
        }
    }
}

impl<S, I, O, Fut> LatestJob<S, I, O, Fut>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn message(
        session: Arc<TaskSession<S, I, O>>,
        factory: fn(AktorTaskState<S>, I) -> Fut,
    ) -> Message<S> {
        Message {
            operation: session.operation,
            job: Box::pin(Self {
                session,
                factory,
                revision: 0,
                finished: false,
                input: None,
                future: None,
            }),
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
    fn service_id(&self) -> Option<*const ()> {
        Some(self.session.id())
    }

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        owner: &Arc<Mutex<Option<S>>>,
        hooks: &mut CallHooks<S>,
        operation: Operation,
    ) -> Poll<Option<AktorTaskState<S>>> {
        let mut this = self.project();

        if let Some(mut state) = state {
            let Some((revision, input)) = this.session.begin() else {
                *this.finished = true;
                return Poll::Ready(Some(state));
            };

            *this.revision = revision;
            *this.input = Some(input);
            hooks.before(&mut state, operation);

            let Some(input) = this.input.take() else {
                crate::message::consumed();
            };

            this.future.set(Some((this.factory)(state, input)));
        }

        let Some(future) = this.future.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        let (mut state, output) = core::task::ready!(future.poll(cx));

        this.future.set(None);

        if Arc::ptr_eq(OwnedMutexGuard::mutex(&state.guard), owner) {
            hooks.after(&mut state, operation);
        }

        *this.finished = true;
        this.session.end(*this.revision, output);

        Poll::Ready(Some(state))
    }
}

impl<S, I, O, Fut> Clone for AktorTaskLatest<S, I, O, Fut> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            session: self.session.clone(),
            factory: self.factory,
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
        let factory = self.factory;

        self.session
            .message(|| Box::new(move |session| LatestJob::message(session, factory)));
        self.sender.send(input);
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
        let (session, sender, results) = latest::Session::open(self.inner.clone(), operation, None);

        (
            AktorTaskLatest {
                sender,
                session,
                factory,
            },
            results,
        )
    }
}
