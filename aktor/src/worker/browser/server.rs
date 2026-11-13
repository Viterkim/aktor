use super::*;
use crate::{
    AktorClosure,
    local::hooks::CallHooks,
    operation::Operation,
    setup::{AktorIntervalLogic, initial::InitError},
};
use core::{convert::Infallible, future::pending, time::Duration};
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use web_sys::DedicatedWorkerGlobalScope;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = reportError)]
    fn report_error(error: &js_sys::Error);
}

pub struct Server {
    scope: DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
    finished: oneshot::Receiver<Result<(), WireError>>,
}
impl Server {
    /// Keep serving until shutdown and cleanup finish.
    pub async fn wait(mut self) -> Result<(), (WorkerError, String)> {
        let result = (&mut self.finished).await.unwrap_or_else(|_| {
            Err(WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Closed,
            ))
        });

        drop(self);
        result.map_err(WireError::report_without_data)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

pub fn respond(scope: &DedicatedWorkerGlobalScope, output: Outgoing) -> Result<(), WireError> {
    respond_with(scope, output, None)
}

fn respond_with(
    scope: &DedicatedWorkerGlobalScope,
    output: Outgoing,
    buffers: Option<&Buffers>,
) -> Result<(), WireError> {
    let output = postcard::to_allocvec(&output).map_err(|error| {
        WireError::new(
            CallError::OutcomeUnknown,
            WorkerCause::Codec(error.to_string()),
        )
    })?;

    let buffer = if let Some(buffers) = buffers {
        buffers.write(&output).map_err(|error| {
            WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Transport(format!("{error:?}")),
            )
        })?
    } else {
        js_sys::Uint8Array::from(output.as_slice()).buffer()
    };
    let sent = if output.len() < super::buffer::MIN_REUSE {
        scope.post_message(&buffer)
    } else {
        let transfers = js_sys::Array::new();

        transfers.push(&buffer);
        scope.post_message_with_transfer(&buffer, &transfers)
    };

    sent.map_err(|error| {
        WireError::new(
            CallError::OutcomeUnknown,
            WorkerCause::Transport(format!("{error:?}")),
        )
    })
}

pub fn scope() -> Result<DedicatedWorkerGlobalScope, WireError> {
    JsValue::from(js_sys::global())
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map_err(|_| WireError::setup(CallError::NotAdmitted, "serve needs a Dedicated Worker"))
}

/// Report a typed initializer failure after releasing its resources. [`serve_setup`] reports its own.
pub fn setup_failed<T: Serialize>(
    error: crate::AktorError<T>,
) -> Result<(), (WorkerError, String)> {
    let failure = lifecycle_error(
        WireError::setup(CallError::NotAdmitted, error.diagnostics),
        &error.data,
    );
    let mut report = format!("{failure:#}");
    let scope = scope().map_err(WireError::report_without_data)?;

    send_start_failure(&scope, Outgoing::SetupFailed(failure)).map_err(|(notice, post)| {
        let (post, reason) = post.report_without_data();
        append_notice_error(&mut report, notice, &reason);
        (post, report)
    })
}

pub(super) fn send_start_failure(
    scope: &DedicatedWorkerGlobalScope,
    failure: Outgoing,
) -> Result<(), (&'static str, WireError)> {
    respond(scope, failure).map_err(|error| ("startup failure", error))?;
    respond(scope, Outgoing::SetupRetired).map_err(|error| ("retirement", error))
}

fn append_notice_error(report: &mut String, notice: &str, reason: &str) {
    if !report.is_empty() {
        report.push('\n');
    }
    report.push_str("Could not send the ");
    report.push_str(notice);
    report.push_str(" notice to the page: ");
    report.push_str(reason);
}

pub(super) fn report_lost_notice(report: &mut String, notice: &str, post: WireError) {
    let (_, reason) = post.report_without_data::<()>();
    append_notice_error(report, notice, &reason);
    report_error(&js_sys::Error::new(report));
}

/// Tell the page that startup failed and the initializer is still releasing its resources.
/// The result tells you whether the browser accepted the notice. Keep releasing the resources
/// either way, then return the initializer's error.
pub fn report_setup_pending(report: impl Into<String>) -> Result<(), (WorkerError, String)> {
    report_to_page(Outgoing::SetupPending(report.into()))
}

/// Report the initializer's error and keep its resources alive. This future stays pending.
/// With `return retain_setup(error).await`, Rust checks the error type sent to the page
/// against the initializer's return type. Use [`report_setup_pending`] if rollback can keep trying.
pub async fn retain_setup<T, E, Mode>(error: E) -> Result<T, E>
where
    E: super::setup::WorkerInitError<Mode>,
{
    report_retained(error.failure(CallError::OutcomeUnknown), "pending startup");
    let _retained = error;

    match pending::<Infallible>().await {}
}

/// Report a cleanup failure while `end` is still working.
/// `Ok(())` means `postMessage` returned successfully. The page may still be waiting for it.
/// Return from `end` once you have released the resources so the page can confirm retirement.
pub fn report_cleanup_pending(report: impl Into<String>) -> Result<(), (WorkerError, String)> {
    report_to_page(Outgoing::Unconfirmed(WireError::cleanup(
        CallError::OutcomeUnknown,
        report,
    )))
}

/// Report a cleanup failure and stay pending while the worker still owns its resources.
/// Use [`report_cleanup_pending`] when cleanup can keep trying.
/// An `end` closure that always parks needs an explicit return type such as `-> ()`.
pub async fn retain_cleanup<E: InitError<Mode>, Mode>(error: E) -> ! {
    let (error, report) = error.retain_with_report();
    report_retained(
        WireError::cleanup(CallError::OutcomeUnknown, report),
        "pending cleanup",
    );
    let _retained = error;

    match pending::<Infallible>().await {}
}

fn report_retained(mut failure: WireError, notice: &str) {
    let Err((_, reason)) = report_to_page(Outgoing::Unconfirmed(failure.clone())) else {
        return;
    };

    let mut report = failure
        .diagnostics
        .take()
        .unwrap_or_else(|| failure.to_string());
    append_notice_error(&mut report, notice, &reason);
    failure.diagnostics = Some(report.clone());

    if let Err((_, reason)) = report_to_page(Outgoing::Unconfirmed(failure)) {
        append_notice_error(&mut report, "retry", &reason);
        report_error(&js_sys::Error::new(&report));
    }
}

fn report_to_page(message: Outgoing) -> Result<(), (WorkerError, String)> {
    let scope = scope().map_err(WireError::report_without_data)?;

    respond(&scope, message).map_err(WireError::report_without_data)
}

pub fn serve<S: 'static>(state: S) -> Result<Server, (WorkerError, String)> {
    serve_with(
        state,
        async |state| {
            drop(state);
            Ok::<_, crate::AktorError>(())
        },
        Options::default(),
    )
}

pub fn serve_with<S: 'static, C, CF, E: Serialize + 'static>(
    state: S,
    cleanup: C,
    options: Options,
) -> Result<Server, (WorkerError, String)>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), crate::AktorError<E>>> + 'static,
{
    serve_for::<(), _, _, _, E>(state, cleanup, options)
}

/// Pick the marker used by #[aktor(role = ...)] when actors share a state type.
pub fn serve_for<Role: 'static, S: 'static, C, CF, E: Serialize + 'static>(
    state: S,
    cleanup: C,
    options: Options,
) -> Result<Server, (WorkerError, String)>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), crate::AktorError<E>>> + 'static,
{
    serve_logic::<Role, _, _, _>(
        state,
        move |state| async move {
            cleanup(state).await.map_err(|error| {
                lifecycle_error(
                    WireError::cleanup(CallError::OutcomeUnknown, error.diagnostics),
                    &error.data,
                )
            })
        },
        options,
        CallHooks::default(),
        Vec::new(),
    )
    .map_err(WireError::report_without_data)
}

pub struct WorkerInterval<S> {
    pub every: Duration,
    pub run: AktorClosure<dyn AktorIntervalLogic<S>>,
}

pub fn serve_logic<Role: 'static, S: 'static, C, CF>(
    mut state: S,
    cleanup: C,
    options: Options,
    mut hooks: CallHooks<S>,
    mut intervals: Vec<WorkerInterval<S>>,
) -> Result<Server, WireError>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), WireError>> + 'static,
{
    let scope = scope()?;

    options
        .validate_limits()
        .map_err(WorkerFailure::without_data)?;

    let registry =
        registry::Registry::for_actor::<S, Role>().map_err(|error| error.without_data())?;
    let operations = registry.operations();

    let (sender, mut receiver) = mpsc::channel::<Incoming>(options.capacity);
    let (stopping, mut stopped) = watch::channel(false);
    let closing = Rc::new(Cell::new(false));
    let replies = scope.clone();
    let buffers = Rc::new(Buffers::new(options.max_outstanding_bytes));
    let received = buffers.clone();

    let callback = Closure::new(move |event: MessageEvent| {
        let input = event
            .data()
            .dyn_into::<js_sys::ArrayBuffer>()
            .map_err(|_| WireError::new(CallError::Discarded, WorkerCause::Protocol))
            .and_then(|buffer| {
                let input =
                    postcard::from_bytes::<Incoming>(&js_sys::Uint8Array::new(&buffer).to_vec());

                if matches!(input, Ok(Incoming::Call { .. })) {
                    received.retain(buffer);
                }

                input.map_err(|error| {
                    WireError::new(CallError::Discarded, WorkerCause::Codec(error.to_string()))
                })
            });

        match input {
            Ok(Incoming::Shutdown) => {
                closing.set(true);
                stopping.send_replace(true);
            }

            Ok(input @ Incoming::Call { .. }) => {
                let id = match &input {
                    Incoming::Call { id, .. } => *id,
                    _ => return,
                };

                let cause = if closing.get() {
                    Some(WorkerCause::Closed)
                } else {
                    None
                };

                let result = if let Some(cause) = cause {
                    Err(WireError::new(CallError::Discarded, cause))
                } else {
                    sender.try_send(input).map_err(|error| {
                        WireError::new(
                            CallError::Discarded,
                            if matches!(error, mpsc::error::TrySendError::Full(_)) {
                                WorkerCause::Full
                            } else {
                                WorkerCause::Closed
                            },
                        )
                    })
                };

                if let Err(error) = result
                    && let Err(error) = respond_with(
                        &replies,
                        Outgoing::Answer {
                            id,
                            output: Err(error),
                        },
                        Some(&received),
                    )
                {
                    fatal(error);
                }
            }
            Ok(Incoming::Initialize { .. }) => {
                fatal(WireError::new(CallError::Discarded, WorkerCause::Protocol))
            }
            Err(error) => fatal(error),
        }
    });

    scope.set_onmessage(Some(callback.as_ref().unchecked_ref()));
    let announcement = respond(
        &scope,
        Outgoing::Ready {
            version: VERSION,
            options,
            operations,
        },
    );
    let ready_failed = announcement.is_err();
    let replies = scope.clone();
    let (finished, completion) = oneshot::channel();

    wasm_bindgen_futures::spawn_local(async move {
        if ready_failed {
            receiver.close();
            let _shutdown = stopped.wait_for(|stopping| *stopping).await;
        }

        let mut closing = false;
        let mut next: Vec<_> = intervals
            .iter()
            .map(|interval| {
                crate::group::shutdown_deadline(crate::group::Instant::now(), interval.every)
            })
            .collect();

        loop {
            if !closing && *stopped.borrow_and_update() {
                closing = true;
                receiver.close();
            }

            let due = next
                .iter()
                .copied()
                .enumerate()
                .min_by_key(|(_, deadline)| *deadline);

            let input = tokio::select! {
                changed = stopped.changed(), if !closing => {
                    let _changed = changed;
                    closing = true;
                    receiver.close();
                    continue;
                },
                input = receiver.recv() => input,
                _ = wait_interval(due), if !closing && due.is_some() => {
                    if let Some((index, _)) = due {
                        let operation = Operation { name: "interval", caller: core::panic::Location::caller() };
                        hooks.before(&mut state, operation);
                        intervals[index].run.0.run(&mut state).await;
                        hooks.after(&mut state, operation);
                        next[index] = crate::group::shutdown_deadline(crate::group::Instant::now(), intervals[index].every);
                    }
                    continue;
                },
            };

            let Some(Incoming::Call {
                id,
                operation,
                input,
            }) = input
            else {
                break;
            };

            if let Err(error) = respond(&replies, Outgoing::Started { id }) {
                fatal(error);
            }

            let registered = registry.resolve(&operation);
            let metadata = registered.map(|operation| Operation {
                name: operation.name(),
                caller: core::panic::Location::caller(),
            });

            if let Some(operation) = metadata {
                hooks.before(&mut state, operation);
            }

            let output = match registered {
                Some(operation) => operation
                    .execute(&mut state, &input)
                    .await
                    .map_err(|error| error.without_data()),
                None => Err(WireError::new(
                    CallError::Discarded,
                    WorkerCause::UnknownOperation(operation),
                )),
            };
            drop(input);

            if let Some(operation) = metadata {
                hooks.after(&mut state, operation);
            }

            if let Err(error) =
                respond_with(&replies, Outgoing::Answer { id, output }, Some(&buffers))
            {
                fatal(error);
            }
        }

        let result = cleanup(state).await;

        drop((hooks, intervals));
        buffers.clear();

        if let Err(post) = respond(&replies, Outgoing::Finished(result.clone())) {
            let mut report = result
                .as_ref()
                .err()
                .map_or_else(String::new, |error| format!("{error:#}"));
            report_lost_notice(&mut report, "cleanup result", post);
        }

        let _sent = finished.send(result);
    });

    let server = Server {
        scope,
        _callback: callback,
        finished: completion,
    };

    if let Err(mut post) = announcement {
        let report = format!("Could not send the ready notice to the page: {post}");
        report_error(&js_sys::Error::new(&report));
        post.diagnostics = Some(report);

        // The initializer already handed us its state. Keep the shutdown handler alive for end.
        wasm_bindgen_futures::spawn_local(async move {
            let _result = server.wait().await;
        });
        return Err(post);
    }

    Ok(server)
}

pub(super) fn lifecycle_error<E: Serialize>(mut error: WireError, data: &E) -> WireError {
    match encode(data) {
        Ok(data) => error.data = Some(data),
        Err(codec) => error = error.data_error(codec),
    }

    error
}

async fn wait_interval(due: Option<(usize, crate::group::Instant)>) {
    if let Some((_, deadline)) = due {
        let _elapsed =
            crate::group::shutdown::bounded(deadline, core::future::pending::<()>()).await;
    } else {
        core::future::pending::<()>().await;
    }
}
