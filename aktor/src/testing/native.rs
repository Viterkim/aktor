use super::*;
use crate::{
    Aktor, AktorGroup, AktorKind, AktorNew,
    dispatch::{OwnedState, ReadCall, ReadState, WriteCall, WriteState},
    message::call_async,
    operation::Operation,
    owner::StartError,
    setup::{AktorStart, AktorStartCause, AktorStartContext, AktorThreadFuture},
};
use core::{future::Future, ops::AsyncFnOnce};

/// A test actor with its own queue and handlers, without constructing the production resource.
pub struct Mock<S, Role = ()> {
    pub actor: Aktor<Handlers>,
    names: alloc::collections::BTreeSet<&'static str>,
    state: PhantomData<fn() -> (S, Role)>,
}

impl<S: 'static, Role: 'static> AktorStart for Preset<S, Role> {
    type Error = core::convert::Infallible;
    type Group = AktorGroup;
    type Handles = Mock<S, Role>;
    type Startup = AktorThreadFuture<Result<Mock<S, Role>, crate::AktorStartError>>;

    fn begin(&self, group: &mut AktorGroup) -> Result<(), AktorStartCause> {
        group
            .start_threaded()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<AktorGroup>) -> Self::Startup {
        let names = self.handlers.entries.keys().copied().collect();
        let handlers = self.handlers;
        let setup = AktorNew {
            name: "test handlers".into(),
            kind: AktorKind::TokioThread,
            start: async move || crate::aktor_ready(handlers),
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: crate::AktorStuck::Kill,
        };

        Box::pin(async move {
            let actor = setup.start_in(context).await?;

            Ok(Mock {
                actor,
                names,
                state: PhantomData,
            })
        })
    }
}

impl<S, Role> Preset<S, Role> {
    #[doc(hidden)]
    pub async fn start(
        self,
        actors: &mut AktorGroup,
    ) -> Result<Mock<S, Role>, (StartError, String)> {
        let names = self.handlers.entries.keys().copied().collect();
        let actor = actors.spawn_value("test handlers", self.handlers).await?;
        Ok(Mock {
            actor,
            names,
            state: PhantomData,
        })
    }
}

impl<S, Role> Mock<S, Role> {
    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        self.actor.run(work).await
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
