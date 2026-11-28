use super::*;
use crate::{
    latest::Session,
    listener::Handle,
    operation::Operation,
    queue::latest::{self, LatestResults, LatestSender},
};
use core::ops::AsyncFnOnce;

/// Runs a thread `latest` session's newest input against `&mut S`.
struct LatestJob<S, I, O, F> {
    session: Arc<latest::Session<Message<S>, I, O>>,
    function: Mutex<Option<F>>,
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, F> Job<S> for LatestJob<S, I, O, F>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn service_id(&self) -> Option<*const ()> {
        Some(self.session.id())
    }

    fn run<'a>(
        &'a self,
        state: &'a mut S,
        hooks: &'a mut crate::listener::hooks::CallHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let Some((revision, input)) = self.session.begin() else {
                return;
            };
            let Some(function) = self.function.lock().take() else {
                return;
            };

            hooks.before(state, operation);

            let output = function(state, input).await;

            hooks.after(state, operation);
            self.session.end(revision, output);
        })
    }

    fn close(&self) {
        self.session.lost();
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static> Session<S, I, O, Role>
    for &Handle<S, Role>
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(self, operation: Operation, function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        // `F` is only `Send`, the lock makes the maker shareable.
        let function = Mutex::new(function);
        let (_, sender, results) = latest::Session::open(
            self.inner.clone(),
            operation,
            Some(Box::new(move |session| Message {
                operation: session.operation,
                job: Arc::new(LatestJob {
                    session,
                    function: Mutex::new(Some(function.lock().clone())),
                }),
                finished: false,
            })),
        );

        (sender, results)
    }
}

impl<S, I, O, Lease, Role, Wire> crate::latest::TypedSession<S, I, O, Lease, Role, Wire>
    for &Handle<S, Role>
where
    Self: Session<S, I, O, Role>,
{
    type Sender<Fut> = <Self as Session<S, I, O, Role>>::Sender;
    type Results = <Self as Session<S, I, O, Role>>::Results;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        _: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: core::future::Future<Output = (Lease, O)>,
    {
        Session::session(self, operation, function)
    }
}
