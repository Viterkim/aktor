use super::*;
use crate::{
    Aktor, AktorGroup, AktorHooks, AktorKind, AktorNew, AktorStuck, LaunchError,
    dispatch::{OwnedState, ReadCall, ReadState, WriteCall, WriteState},
    operation::Operation,
    setup::{AktorLaunch, AktorStart, AktorStartContext, AktorStartReason, AktorThreadFuture},
};
use core::num::NonZeroUsize;
use core::{future::Future, ops::AsyncFnOnce};
use er::*;

/// A test actor with its own queue and handlers, without constructing the production resource.
pub struct Mock<S, Role = ()> {
    actor: Aktor<Handlers>,
    state: PhantomData<fn() -> (S, Role)>,
}

impl<S: 'static, Role: 'static> AktorStart for Preset<S, Role> {
    type Error = core::convert::Infallible;
    type Group = AktorGroup;
    type Handles = Mock<S, Role>;

    fn actor_name(&self) -> &str {
        "test handlers"
    }

    fn begin(&self, group: &mut AktorGroup) -> Result<(), (AktorStartReason, String)> {
        // Without a Tokio runtime around, the standard group is the one that can start.
        #[cfg(all(feature = "tokio", feature = "std_thread"))]
        let started = match tokio::runtime::Handle::try_current() {
            Ok(_) => group.start_threaded(),
            Err(_) => group.start_standard(),
        };
        #[cfg(all(feature = "tokio", not(feature = "std_thread")))]
        let started = group.start_threaded();
        #[cfg(not(feature = "tokio"))]
        let started = group.start_standard();

        started.er_with(|error| error.start_reason())?;
        Ok(())
    }

    fn closed(_: &str, group: &AktorGroup) -> Self::Handles {
        Mock {
            actor: Aktor::closed_handle(group.killswitch()),
            state: PhantomData,
        }
    }

    fn launch(self, context: AktorStartContext<AktorGroup>) -> AktorLaunch<Self> {
        let (actor, ready) = launch_handlers(self.handlers, context)?;

        Ok((
            Mock {
                actor,
                state: PhantomData,
            },
            ready,
        ))
    }
}

type HandlersLaunch = Result<
    (
        Aktor<Handlers>,
        AktorThreadFuture<Result<(), (LaunchError, String)>>,
    ),
    (LaunchError, String),
>;

/// The mock runs on whichever backend its group already uses.
fn launch_handlers(handlers: Handlers, context: AktorStartContext<AktorGroup>) -> HandlersLaunch {
    #[cfg(feature = "std_thread")]
    if context.group.is_standard() {
        let start = crate::AktorInit::Ready(async move || handlers);

        return AktorNew {
            name: "test handlers".into(),
            kind: AktorKind::StdThread,
            start,
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        }
        .launch(context);
    }

    #[cfg(feature = "tokio")]
    let kind = AktorKind::TokioThread;
    #[cfg(not(feature = "tokio"))]
    let kind = AktorKind::StdThread;

    AktorNew {
        name: "test handlers".into(),
        kind,
        start: crate::AktorInit::Ready(async move || handlers),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    }
    .launch(context)
}

impl<S, Role> Mock<S, Role> {
    #[cfg(feature = "transport")]
    #[doc(hidden)]
    pub fn handler_queue(&self) -> crate::listener::Handle<Handlers> {
        self.actor.handle()
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub fn run<F: IntoFuture>(&self, work: F) -> crate::AktorRun<F::IntoFuture> {
        let work = work.into_future();

        self.actor.run(work)
    }

    pub fn completion(&self) -> crate::owner::OwnerCompletion {
        self.actor.completion()
    }

    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.actor.actor_id()
    }

    pub fn group_completion(&self) -> crate::AktorGroupCompletion {
        self.actor.group_completion()
    }

    pub fn retire(&self) {
        self.actor.retire();
    }
}
impl<S, Role> Clone for Mock<S, Role> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            state: PhantomData,
        }
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
    type Request<Fut> = crate::message::Request<'a, Handlers, O>;

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
        request(&self.actor.handle, TypeId::of::<F>(), operation, input)
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> WriteCall<S, I, O, Role>
    for &'a Mock<S, Role>
{
    type Request<Fut> = crate::message::Request<'a, Handlers, O>;

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
        request(&self.actor.handle, TypeId::of::<F>(), operation, input)
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, Role, C>
    crate::latest::TypedSession<S, I, O, OwnedState<S>, Role, C> for &Mock<S, Role>
{
    type Sender<Fut> = crate::message::LatestSender<I>;
    type Results = crate::message::LatestResults<O>;

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
        crate::latest::Session::session(
            &self.actor.handle,
            operation,
            async move |handlers: &mut Handlers, input: I| {
                invoke(handlers, TypeId::of::<F>(), operation, input).await
            },
        )
    }
}

async fn invoke<I: Send + 'static, O: Send + 'static>(
    handlers: &mut Handlers,
    body: TypeId,
    operation: Operation,
    input: I,
) -> O {
    let output = handlers
        .run_local(body, operation.name, Box::new(input))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    *output
        .downcast::<O>()
        .unwrap_or_else(|_| panic!("incorrect test output for {}", operation.name))
}

fn request<I: Send + 'static, O: Send + 'static>(
    handle: &crate::listener::Handle<Handlers>,
    body: TypeId,
    operation: Operation,
    input: I,
) -> crate::message::Request<'_, Handlers, O> {
    crate::message::call_async(
        handle,
        async move |handlers: &mut Handlers, input| invoke(handlers, body, operation, input).await,
        input,
    )
    .operation(operation)
}

/// Run a closure on an actor's state, like an `#[aktor]` function you didn't have to write.
/// For tests that hold the actor busy, look at its state or make it panic. Await it for the
/// answer, or use `.send().await?` to keep the reply for later.
#[track_caller]
pub fn call<S: 'static, Role, O>(
    actor: &Aktor<S, Role>,
    work: impl for<'s> AsyncFnOnce(&'s mut S) -> O + Send + 'static,
) -> crate::message::Request<'_, S, O>
where
    O: Send + 'static,
{
    crate::message::call_async(&actor.handle, async move |state, ()| work(state).await, ())
}
