use super::*;
use crate::setup::closures::{AktorCleanupCallbackUnit, AktorCleanupSyncUnit, AktorNoCleanup};
use crate::{
    AktorHooks, AktorWorkerContract,
    local::hooks::CallHooks,
    setup::{closures::AktorEndCall, kind::BrowserWebWorker},
};
use core::ops::AsyncFnOnce;
use server::{ServerSetup, WorkerInterval, respond, scope};
use wasm_bindgen::JsCast;

/// The setup that a worker runs. Pass `serve_setup` a function that builds it from the app's config.
pub struct AktorServe<C: AktorWorkerContract, Init, End = (), Before = (), After = ()> {
    pub start: Init,
    /// Return `()` when cleanup always succeeds. A failed cleanup sends its full report to the page.
    pub end: End,
    pub hooks: AktorHooks<C::State, BrowserWebWorker<C>, Before, After>,
}

/// Return the state to serve and the value to send to the page from the same initializer.
pub struct AktorReady<S, P> {
    pub state: S,
    pub ready: P,
}

/// Use `Pending` while a failed callback still owns resources. The page receives its
/// report immediately. Move those resources into `finish` and resolve it after release.
pub enum ServeFailure<T, E> {
    Returned(E, String),
    Pending {
        error: Option<E>,
        report: String,
        finish: LocalFuture<'static, Result<T, (E, String)>>,
    },
}

impl<T, E> From<(E, String)> for ServeFailure<T, E> {
    fn from((error, report): (E, String)) -> Self {
        Self::Returned(error, report)
    }
}

trait WorkerFailure<T, E, Mode> {
    async fn resolve(self, setup: bool) -> Result<T, (E, String)>;
}

pub struct Returned;
impl<T, E, X: Into<(E, String)>> WorkerFailure<T, E, Returned> for X {
    async fn resolve(self, _: bool) -> Result<T, (E, String)> {
        Err(self.into())
    }
}

pub struct Pending;
impl<T, E: AktorData> WorkerFailure<T, E, Pending> for ServeFailure<T, E> {
    async fn resolve(self, setup: bool) -> Result<T, (E, String)> {
        let (error, report, finish) = match self {
            Self::Returned(error, report) => return Err((error, report)),
            Self::Pending {
                error,
                report,
                finish,
            } => (error, report, finish),
        };
        let outcome = AktorDeliveryOutcome::OutcomeUnknown;
        let mut failure = if setup {
            WireFailure::setup(outcome, report)
        } else {
            WireFailure::cleanup(outcome, report)
        };
        if let Some(data) = &error {
            failure = server::lifecycle_error(failure, data);
        }
        server::pending(if setup {
            Outgoing::SetupPending(failure)
        } else {
            Outgoing::Unconfirmed(failure)
        });

        finish.await
    }
}

trait Receipt<C: AktorWorkerContract, Mode> {
    fn receipt(self) -> AktorReady<C::State, C::Ready>;
}
pub struct UnitReceipt;
impl<C: AktorWorkerContract<Ready = ()>> Receipt<C, UnitReceipt> for C::State {
    fn receipt(self) -> AktorReady<C::State, ()> {
        AktorReady {
            state: self,
            ready: (),
        }
    }
}
pub struct Payload;
impl<C: AktorWorkerContract> Receipt<C, Payload> for AktorReady<C::State, C::Ready> {
    fn receipt(self) -> Self {
        self
    }
}

struct Initialize {
    scope: web_sys::DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
}
impl Drop for Initialize {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

#[derive(Er)]
struct ConfigureErr {
    cause: WorkerCause,
}
async fn configuration<C: AktorWorkerContract>(
    options: Options,
) -> ErResult<(C::Config, ServerSetup), ConfigureErr> {
    let setup = ServerSetup::new::<C::State, C::Role>(options)
        .er(|| ConfigureErr::new(WorkerCause::Setup))?;
    let scope = &setup.scope;

    let (sender, config) = oneshot::channel();
    let sender = Rc::new(RefCell::new(Some(sender)));
    let callback = Closure::new(move |event: MessageEvent| {
        let Some(sender) = sender.borrow_mut().take() else {
            return;
        };
        let result = (|| {
            let (input, _) = read_frame(event).er_with(|err| ConfigureErr::new(err.cause()))?;
            match input {
                Incoming::Initialize { config } => {
                    decode(&config).er(|| ConfigureErr::new(WorkerCause::Codec))
                }
                Incoming::Shutdown => er_bail!(ConfigureErr::new(WorkerCause::Closed)),
                Incoming::Call { .. } => er_bail!(ConfigureErr::new(WorkerCause::Protocol)),
            }
        })();

        let _sent = sender.send(result);
    });

    scope.set_onmessage(Some(callback.as_ref().unchecked_ref()));

    let _initialize = Initialize {
        scope: scope.clone(),
        _callback: callback,
    };

    respond(
        scope,
        Outgoing::Configure {
            config_type: core::any::type_name::<C::Config>().into(),
            payload_type: core::any::type_name::<C::Ready>().into(),
            version: VERSION,
            options: setup.options.clone(),
            operations: setup.operations.clone(),
        },
    )
    .er_with(|err| ConfigureErr::new(err.cause.clone()))?;
    let configured = config.await.er(|| ConfigureErr::new(WorkerCause::Closed))?;
    let config = configured?;

    Ok((config, setup))
}

fn init_not_run(error: WireFailure, release: impl FnOnce()) -> (WorkerError, String) {
    report_start_failure(error, release, |error| Outgoing::InitNotRun {
        version: VERSION,
        error,
    })
}

#[derive(Er)]
struct StartNoticeErr;
fn report_start_failure(
    failure: WireFailure,
    release: impl FnOnce(),
    notice: impl FnOnce(WireFailure) -> Outgoing,
) -> (WorkerError, String) {
    let Ok(scope) = scope() else {
        return failure.without_data();
    };
    let sent = server::send_start_failure(&scope, notice(failure.clone()), release);

    let (error, mut report) = failure.without_data();
    if let Err(post) = sent {
        report = er_new!(StartNoticeErr.er_save("worker startup", report), post).er_report();
        server::log_error(&report);
    }

    (error, report)
}

#[doc(hidden)]
pub trait SetupFn<Config>: FnOnce(Config) -> Self::Init {
    type Init;
}
impl<Config, F, Init> SetupFn<Config> for F
where
    F: FnOnce(Config) -> Init,
{
    type Init = Init;
}

#[doc(hidden)]
pub trait WorkerInit<C: AktorWorkerContract, Signature> {
    fn start(self, setup: ServerSetup) -> impl Future<Output = Result<(), (WorkerError, String)>>;
}

/// Build an [`AktorServe`] from the page's config and serve until cleanup finishes.
/// The bootstrap gets the final result. Startup and cleanup errors travel back to the
/// page with the contract's error data.
pub async fn serve_setup<C: AktorWorkerContract>(
    options: Options,
    logic: impl SetupFn<C::Config, Init: WorkerInit<C, impl Sized>>,
) -> Result<(), (WorkerError, String)> {
    let (config, setup) = match configuration::<C>(options)
        .await
        .er_with(|err| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, err.cause.clone()))
    {
        Ok(config) => config,
        Err(error) => {
            return Err(init_not_run(error.into(), || drop(logic)));
        }
    };

    logic(config).start(setup).await
}

trait InitializeWorker<T, E, Mode> {
    async fn initialize(self) -> Result<T, (E, String)>;
}
impl<F: Future, E> InitializeWorker<F::Output, E, ()> for crate::setup::ReadyState<F> {
    async fn initialize(self) -> Result<F::Output, (E, String)> {
        match self.await {
            Ok(output) => Ok(output),
            Err((never, _)) => match never {},
        }
    }
}
impl<F, T, E, X, M> InitializeWorker<T, E, M> for crate::setup::TryState<F>
where
    F: Future<Output = Result<T, X>>,
    X: WorkerFailure<T, E, M>,
{
    async fn initialize(self) -> Result<T, (E, String)> {
        match self.await {
            Ok(output) => Ok(output),
            Err(error) => error.resolve(true).await,
        }
    }
}

impl<C, Start, Fut, End, Output, EndMode, Before, After, R, M>
    WorkerInit<C, (EndMode, Output, R, M)>
    for AktorServe<C, crate::AktorInit<Start>, End, Before, After>
where
    C: AktorWorkerContract,
    Before: crate::setup::closures::EachHook<dyn FnMut(&mut C::State, Operation)>,
    After: crate::setup::closures::EachHook<dyn FnMut(&mut C::State, Operation)>,
    Start: FnOnce() -> Fut,
    Fut: InitializeWorker<Output, C::Error, M>,
    Output: Receipt<C, R>,
    End: WorkerEnd<C, EndMode> + 'static,
{
    async fn start(self, setup: ServerSetup) -> Result<(), (WorkerError, String)> {
        let AktorServe { start, end, hooks } = self;
        let AktorHooks {
            intervals,
            before_each,
            after_each,
        } = hooks;

        let AktorReady { state, ready } = match (start.0)().initialize().await {
            Ok(output) => output.receipt(),
            Err(error) => {
                let (data, report) = error;
                let failure = server::lifecycle_error(
                    WireFailure::setup(AktorDeliveryOutcome::NotAdmitted, report),
                    &data,
                );
                return Err(report_start_failure(
                    failure,
                    || drop((data, end, intervals, before_each, after_each)),
                    Outgoing::SetupFailed,
                ));
            }
        };

        let payload = encode(&ready)
            .er(|| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Codec))
            .er_split();

        server::serve_logic(
            state,
            (core::any::type_name::<C::Ready>(), payload),
            async move |state| match end.finish(state).await {
                Ok(()) => Ok(()),
                Err((data, report)) => {
                    let failure = server::EndFailure::post(server::lifecycle_error(
                        WireFailure::cleanup(AktorDeliveryOutcome::OutcomeUnknown, report),
                        &data,
                    ));
                    drop(data);
                    Err(failure)
                }
            },
            setup,
            CallHooks {
                before_each: before_each.boxed(),
                after_each: after_each.boxed(),
                intervals: Vec::new(),
            },
            intervals
                .into_iter()
                .map(|interval| WorkerInterval {
                    every: interval.every,
                    run: interval.run,
                })
                .collect(),
        )
        .await
    }
}

trait WorkerEnd<C: AktorWorkerContract, Mode> {
    async fn finish(self, state: C::State) -> Result<(), (C::Error, String)>;
}

trait InfallibleEnd {}
impl InfallibleEnd for AktorNoCleanup {}
impl InfallibleEnd for AktorCleanupCallbackUnit {}
impl InfallibleEnd for AktorCleanupSyncUnit {}

impl<C, F, M: InfallibleEnd> WorkerEnd<C, (M,)> for F
where
    C: AktorWorkerContract,
    F: AktorEndCall<C::State, M, Error = core::convert::Infallible>,
{
    async fn finish(self, state: C::State) -> Result<(), (C::Error, String)> {
        match self.run_typed(state).await {
            Ok(()) => Ok(()),
            Err((never, _)) => match never {},
        }
    }
}

pub struct AsyncEnd;
impl<C, F, X, M> WorkerEnd<C, (AsyncEnd, M)> for F
where
    C: AktorWorkerContract,
    F: AsyncFnOnce(C::State) -> Result<(), X>,
    X: WorkerFailure<(), C::Error, M>,
{
    async fn finish(self, state: C::State) -> Result<(), (C::Error, String)> {
        match self(state).await {
            Ok(()) => Ok(()),
            Err(error) => error.resolve(false).await,
        }
    }
}

pub struct SyncEnd;
impl<C, F, X, M> WorkerEnd<C, (SyncEnd, M)> for F
where
    C: AktorWorkerContract,
    F: FnOnce(C::State) -> Result<(), X>,
    X: WorkerFailure<(), C::Error, M>,
{
    async fn finish(self, state: C::State) -> Result<(), (C::Error, String)> {
        match self(state) {
            Ok(()) => Ok(()),
            Err(error) => error.resolve(false).await,
        }
    }
}
