use super::*;
use crate::{
    AktorHooks,
    local::hooks::CallHooks,
    setup::{closures::AktorEndCall, initial, kind::BrowserWebWorker},
};
use core::ops::AsyncFnOnce;
use server::{WorkerInterval, respond, scope};
use wasm_bindgen::JsCast;

/// The setup that a worker runs. Pass `serve_setup` a function that builds it from the app's config.
pub struct AktorServe<S: 'static, Start, End = ()> {
    pub start: Start,
    /// Return `()` when cleanup always succeeds. A failed cleanup sends its full report to the page.
    pub end: End,
    pub hooks: AktorHooks<S, BrowserWebWorker<S>>,
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

async fn configuration<S: 'static, Role: 'static, Config: DeserializeOwned + 'static>(
    options: Options,
) -> Result<Config, WireError> {
    options
        .validate_limits()
        .map_err(WorkerFailure::without_data)?;
    let operations = registry::Registry::for_actor::<S, Role>()
        .map_err(WorkerFailure::without_data)?
        .operations();

    let scope = scope()?;
    let (sender, config) = oneshot::channel();
    let sender = Rc::new(RefCell::new(Some(sender)));
    let callback = Closure::new(move |event: MessageEvent| {
        let Some(sender) = sender.borrow_mut().take() else {
            return;
        };
        let result = event
            .data()
            .dyn_into::<js_sys::ArrayBuffer>()
            .map_err(|_| WireError::new(CallError::NotAdmitted, WorkerCause::Protocol))
            .and_then(|buffer| {
                postcard::from_bytes::<Incoming>(&js_sys::Uint8Array::new(&buffer).to_vec())
                    .map_err(|error| {
                        WireError::new(
                            CallError::NotAdmitted,
                            WorkerCause::Codec(error.to_string()),
                        )
                    })
            })
            .and_then(|input| match input {
                Incoming::Initialize { config } => decode(&config).map_err(WireError::from),
                Incoming::Shutdown => {
                    Err(WireError::new(CallError::NotAdmitted, WorkerCause::Closed))
                }
                Incoming::Call { .. } => Err(WireError::new(
                    CallError::NotAdmitted,
                    WorkerCause::Protocol,
                )),
            });

        let _sent = sender.send(result);
    });

    scope.set_onmessage(Some(callback.as_ref().unchecked_ref()));

    let _initialize = Initialize {
        scope: scope.clone(),
        _callback: callback,
    };

    respond(
        &scope,
        Outgoing::Configure {
            version: VERSION,
            options,
            operations,
        },
    )?;
    config
        .await
        .unwrap_or_else(|_| Err(WireError::new(CallError::NotAdmitted, WorkerCause::Closed)))
}

fn init_not_run(error: WireError) -> (WorkerError, String) {
    report_start_failure(error, |error| Outgoing::InitNotRun {
        version: VERSION,
        error,
    })
}

fn report_start_failure(
    failure: WireError,
    notice: impl FnOnce(WireError) -> Outgoing,
) -> (WorkerError, String) {
    let Ok(scope) = scope() else {
        return failure.report_without_data();
    };
    let sent = server::send_start_failure(&scope, notice(failure.clone()));

    let (error, mut report) = failure.report_without_data();
    if let Err((notice, post)) = sent {
        server::report_lost_notice(&mut report, notice, post);
    }

    (error, report)
}

#[doc(hidden)]
pub trait WorkerInitError<Mode> {
    fn failure(&self, outcome: CallError) -> WorkerFailure<Vec<u8>>;
}

fn setup_failure<E: Serialize>(
    outcome: CallError,
    report: impl Into<String>,
    data: &E,
) -> WireError {
    server::lifecycle_error(WireError::setup(outcome, report), data)
}

impl<E: core::error::Error + Serialize + 'static> WorkerInitError<initial::Plain> for E {
    fn failure(&self, outcome: CallError) -> WireError {
        setup_failure(outcome, crate::setup::diagnostics(self), self)
    }
}
impl WorkerInitError<initial::Text> for String {
    fn failure(&self, outcome: CallError) -> WireError {
        setup_failure(outcome, self.as_str(), &())
    }
}
impl<E: Serialize> WorkerInitError<initial::Pair> for (E, String) {
    fn failure(&self, outcome: CallError) -> WireError {
        setup_failure(outcome, self.1.as_str(), &self.0)
    }
}
impl<E: core::error::Error + Serialize + 'static> WorkerInitError<initial::Tree> for er::ErTree<E> {
    fn failure(&self, outcome: CallError) -> WireError {
        setup_failure(outcome, crate::report::tree_report(self), &self.top)
    }
}
impl WorkerInitError<core::convert::Infallible> for core::convert::Infallible {
    fn failure(&self, _: CallError) -> WireError {
        match *self {}
    }
}

/// Run the initializer with the config from the page. This sends its own startup notices.
pub async fn serve_setup<
    Role: 'static,
    S: 'static,
    Config: DeserializeOwned + 'static,
    Start,
    End,
    CM,
    E,
    Mode,
>(
    _: Role,
    options: Options,
    logic: impl FnOnce(Config) -> AktorServe<S, Start, End>,
) -> Result<Server, (WorkerError, String)>
where
    Start: AsyncFnOnce() -> Result<S, E>,
    E: WorkerInitError<Mode>,
    End: AktorEndCall<S, CM> + 'static,
{
    let config = match configuration::<S, Role, Config>(options.clone()).await {
        Ok(config) => config,
        Err(error) => {
            drop(logic);
            return Err(init_not_run(error));
        }
    };

    let AktorServe { start, end, hooks } = logic(config);
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = hooks;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        let error = WireError::setup(CallError::NotAdmitted, "interval duration must be positive");

        drop((start, end, intervals, before_each, after_each));
        return Err(init_not_run(error));
    }

    let state = match start().await {
        Ok(state) => state,
        Err(error) => {
            let failure = error.failure(CallError::NotAdmitted);
            drop((error, end, intervals, before_each, after_each));
            return Err(report_start_failure(failure, Outgoing::SetupFailed));
        }
    };

    server::serve_logic::<Role, _, _, _>(
        state,
        async move |state| match end.run(state).await {
            Ok(()) => Ok(()),
            Err(error) => Err(WireError::cleanup(
                CallError::OutcomeUnknown,
                error.diagnostics,
            )),
        },
        options,
        CallHooks {
            before_each: before_each.map(|callback| callback.0),
            after_each: after_each.map(|callback| callback.0),
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
    .map_err(WireError::report_without_data)
}
