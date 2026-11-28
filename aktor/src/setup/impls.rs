use super::receipt::Stage;
use super::*;

// `start` can return anything that turns into `(E, String)`, like an ErResult. The launch code
// underneath only knows the pair, so it gets a `start` that converts (see `pair_start!`).
pub(crate) fn pair_start<S, Start, X, E>(
    start: Start,
) -> impl AsyncFnOnce() -> Result<S, (E, String)>
where
    Start: AsyncFnOnce() -> Result<S, X>,
    X: Into<(E, String)>,
{
    async move || match start().await {
        Ok(state) => Ok(state),
        Err(error) => Err(error.into()),
    }
}

/// The same for the kinds that take `FnOnce() -> impl Future`.
#[cfg(any(feature = "bevy", all(feature = "tokio", not(target_family = "wasm"))))]
pub(crate) fn pair_start_fn<Start, Fut, E>(start: Start) -> impl FnOnce() -> PairStart<Fut, E>
where
    Start: FnOnce() -> Fut,
{
    move || PairStart {
        started: Box::pin(start()),
        error: core::marker::PhantomData,
    }
}

/// A `start` future whose error turns into `(E, String)` when it's done. Built once per actor.
#[cfg(any(feature = "bevy", all(feature = "tokio", not(target_family = "wasm"))))]
pub(crate) struct PairStart<Fut, E> {
    started: core::pin::Pin<Box<Fut>>,
    error: core::marker::PhantomData<fn() -> E>,
}
#[cfg(any(feature = "bevy", all(feature = "tokio", not(target_family = "wasm"))))]
impl<Fut, S, X, E> Future for PairStart<Fut, E>
where
    Fut: Future<Output = Result<S, X>>,
    X: Into<(E, String)>,
{
    type Output = Result<S, (E, String)>;

    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.started.as_mut().poll(cx).map(|result| match result {
            Ok(state) => Ok(state),
            Err(error) => Err(error.into()),
        })
    }
}

impl<S: 'static, Kind: AktorMode> AktorInterval<S, Kind> {
    pub fn new(
        every: Duration,
        run: impl Into<AktorClosure<Kind::Interval<S>>>,
    ) -> Result<Self, (crate::AktorIntervalError, String)> {
        if every.is_zero() {
            er_bail!(crate::AktorIntervalError);
        }

        Ok(Self {
            every,
            run: run.into(),
        })
    }
}

impl<S: 'static, Kind: AktorMode> AktorHooks<S, Kind> {
    /// No intervals, nothing before or after each call. Most actors have none.
    pub const NONE: Self = Self {
        intervals: Vec::new(),
        before_each: (),
        after_each: (),
    };
}

impl<S: 'static, Start, Kind: AktorMode, End, Signature, Before, After>
    AktorNew<S, Start, Kind, End, Signature, Before, After>
{
    pub fn execution(&self) -> AktorExecution {
        Kind::EXECUTION
    }
}

impl<Handles, Group: AktorSetupGroup, E: 'static> AktorStarted<Handles, Group, E> {
    /// Move out the handles while keeping the group and its startup result. This lets you
    /// keep local handles on their executor and send the native group owner to another task.
    pub fn into_parts(self) -> (AktorStarted<(), Group, E>, Handles) {
        let started = AktorStarted {
            handles: (),
            group: self.group,
            readiness: self.readiness,
        };

        (started, self.handles)
    }

    pub fn killswitch(&self) -> Group::AktorKillSwitch {
        self.group.killswitch()
    }

    pub fn completion(&self) -> Group::Completion {
        self.group.completion()
    }

    pub fn handles(&self) -> &Handles {
        &self.handles
    }

    pub fn group(&self) -> &Group {
        &self.group
    }

    /// Every waiter borrows the same startup receipt or initializer error.
    pub async fn ready(&self) -> Result<&crate::AktorStartup, (&AktorStartError<E>, String)> {
        self.readiness.wait(&self.group).await
    }
}

impl<Handles, Group: AktorSetupGroup, E: 'static> AktorSpawned<Handles, Group, E> {
    /// Take the handles early after checking that the group accepted registration.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> Result<(AktorSpawned<(), Group, E>, Handles), (AktorStartError<E>, String)> {
        let readiness = self.readiness.split()?;
        Ok((
            AktorSpawned {
                handles: (),
                group: self.group,
                readiness,
            },
            self.handles,
        ))
    }

    /// Wait for startup and take the handles, including handles with a single owned result.
    pub async fn ready(self) -> Result<Handles, (AktorStartError<E>, String)> {
        self.readiness.into_ready(&self.group).await?;
        Ok(self.handles)
    }

    /// Read the initialization evidence for this spawn, including when its handles have moved.
    pub async fn receipt(&self) -> Result<&crate::AktorStartup, (&AktorStartError<E>, String)> {
        self.readiness.wait(&self.group).await
    }
}

impl<Group: AktorSetupGroup> Clone for AktorStartContext<Group> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.registration(),
        }
    }
}

#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
impl<Handles, E: 'static> AktorStarted<Handles, crate::AktorGroup, E> {
    /// Start shutdown now and return the report by the group shutdown deadline.
    pub fn stop(&self) -> impl Future<Output = crate::AktorShutdownReport> + Send + 'static {
        self.group.stop()
    }
    pub fn stop_released(
        &self,
    ) -> impl Future<Output = Result<crate::AktorRetired, (crate::AktorRetirementError, String)>>
    + Send
    + 'static {
        self.group.stop_released()
    }
}

#[cfg(feature = "local")]
impl<Handles, Clock: crate::local::clock::AktorGroupClock, E: 'static>
    AktorStarted<Handles, crate::local::AktorGroup<Clock>, E>
{
    /// Start shutdown now and return the report by the group shutdown deadline.
    pub fn stop(&self) -> impl Future<Output = crate::AktorShutdownReport> + 'static {
        self.group.stop()
    }
    pub fn stop_released(
        &self,
    ) -> impl Future<Output = Result<crate::AktorRetired, (crate::AktorRetirementError, String)>> + 'static
    {
        self.group.stop_released()
    }
}

/// Start actors. Nothing waits here, the handles come back right away and calls queue until
/// `start` is done. Await `.ready()` when you want the typed start error.
///
/// What you get back owns the actors, dropping it stops them all.
pub fn aktor_start<Setups, Shutdown, ShutdownOutput, ShutdownMode>(
    setup: AktorSetup<Setups, Shutdown, ShutdownOutput>,
) -> AktorStarted<Setups::Handles, Setups::Group, Setups::Error>
where
    Setups: AktorStart,
    Setups::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::AktorShutdownReport) -> ShutdownOutput + 'static,
{
    start_group(Setups::Group::new(setup.grace, setup.stuck), setup)
}

fn start_group<Setups, Shutdown, ShutdownOutput, ShutdownMode>(
    mut group: Setups::Group,
    setup: AktorSetup<Setups, Shutdown, ShutdownOutput>,
) -> AktorStarted<Setups::Handles, Setups::Group, Setups::Error>
where
    Setups: AktorStart,
    Setups::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::AktorShutdownReport) -> ShutdownOutput + 'static,
{
    group.track_startup();

    let kill = group.killswitch();
    let name = String::from(setup.actors.actor_name());
    let actors = setup.actors;
    let fallback = actors.fallback();

    let launched = (|| {
        shutdown::AktorShutdown::on_shutdown(&group, setup.shutdown)
            .er_with(|error| LaunchError::new(error.start_reason()))?;
        if let Err((reason, text)) = actors.begin(&mut group) {
            let id = Setups::Group::startup_id(&kill).unwrap_or_else(|| {
                Setups::Group::fail_startup(
                    &kill,
                    crate::AktorFailureInput::new(
                        crate::AktorFailureSource::GroupDriver,
                        text.clone(),
                    ),
                )
            });
            return Err((
                LaunchError {
                    failure_id: Some(id),
                    ..LaunchError::new(reason)
                },
                text,
            ));
        }
        actors.launch(AktorStartContext {
            group: group.registration(),
        })
    })();

    match launched {
        Ok((handles, ready)) => AktorStarted {
            handles,
            group,
            readiness: Stage::Starting(Setups::Group::observe_ready(ready, kill, name)).into(),
        },
        Err(mut error) => {
            group::identify::<Setups::Group, _>(&kill, name, &mut error);
            Setups::Group::stop(&kill);

            AktorStarted {
                handles: fallback(&group),
                group,
                readiness: Stage::Failed(error).into(),
            }
        }
    }
}

pub(crate) fn validation_error<Group: AktorSetupGroup, E>(
    group: &Group,
    actor: &str,
    kind: AktorExecution,
    mut failure: StartFailure<E>,
) -> StartFailure<E> {
    group.validate_attempt(actor, kind);
    failure.0.failure_id = Some(Group::fail_startup(
        &group.killswitch(),
        crate::AktorFailureInput::actor(
            actor.into(),
            kind,
            crate::AktorPhase::Start,
            failure.1.clone(),
        ),
    ));
    failure
}
