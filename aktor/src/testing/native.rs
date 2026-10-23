use super::*;
use crate::{
    Aktor, AktorGroup,
    dispatch::{OwnedState, ReadCall, ReadState, WriteCall, WriteState},
    message::call_async,
    operation::Operation,
    owner::StartError,
};
use core::{future::Future, ops::AsyncFnOnce};

/// A test actor with its own queue and handlers, without constructing the production resource.
pub struct Mock<S, Role = ()> {
    pub actor: Aktor<Handlers>,
    names: alloc::collections::BTreeSet<&'static str>,
    state: PhantomData<fn() -> (S, Role)>,
}

impl<S, Role> Preset<S, Role> {
    pub async fn start(self, actors: &mut AktorGroup) -> Result<Mock<S, Role>, StartError> {
        let names = self.handlers.entries.keys().copied().collect();
        let actor = actors.spawn_value("test handlers", self.handlers).await?;
        Ok(Mock {
            actor,
            names,
            state: PhantomData,
        })
    }
}

impl<S: 'static, Role> ReadState<S, Role> for &Mock<S, Role> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role> WriteState<S, Role> for &Mock<S, Role> {
    type Lease = OwnedState<S>;
}

impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> ReadCall<S, I, O, Role>
    for &'a Mock<S, Role>
{
    type Request<Fut> = Request<'a, O>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        _: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        assert!(
            self.names.contains(operation.name),
            "missing test handler for {}",
            operation.name
        );
        request(&self.actor.handle, operation, input)
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> WriteCall<S, I, O, Role>
    for &'a Mock<S, Role>
{
    type Request<Fut> = Request<'a, O>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        _: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        assert!(
            self.names.contains(operation.name),
            "missing test handler for {}",
            operation.name
        );
        request(&self.actor.handle, operation, input)
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, Role, C>
    crate::latest::TypedSession<S, I, O, OwnedState<S>, Role, C> for &Mock<S, Role>
{
    type Sender<Fut> = crate::message::LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F, Fut>(
        self,
        operation: Operation,
        _: F,
        _: fn(OwnedState<S>, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: Future<Output = (OwnedState<S>, O)>,
    {
        assert!(
            self.names.contains(operation.name),
            "missing test handler for {}",
            operation.name
        );
        let (sender, results) = crate::latest::Session::session(
            &self.actor.handle,
            operation,
            async move |handlers: &mut Handlers, input: I| invoke(handlers, operation, input).await,
        );
        (
            sender,
            LatestResults::new(results, self.actor.handle.completion()),
        )
    }
}

async fn invoke<I: Send + 'static, O: Send + 'static>(
    handlers: &mut Handlers,
    operation: Operation,
    input: I,
) -> O {
    let output = handlers
        .run(operation.name, Box::new(input))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    *output
        .downcast::<O>()
        .unwrap_or_else(|_| panic!("incorrect test output for {}", operation.name))
}

fn request<I: Send + 'static, O: Send + 'static>(
    handle: &crate::listener::Handle<Handlers>,
    operation: Operation,
    input: I,
) -> Request<'_, O> {
    let standard = handle.inner.admission.is_standard();
    let request = call_async(
        handle,
        async move |handlers: &mut Handlers, input| {
            // A missing mock is a failed test. Local operation outputs have no delivery error layer.
            invoke(handlers, operation, input).await
        },
        input,
    )
    .operation(operation);
    Request::new(request, standard)
}
