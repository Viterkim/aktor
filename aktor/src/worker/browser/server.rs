use super::*;
use crate::{
    AktorClosure, local::hooks::CallHooks, operation::Operation, setup::AktorIntervalLogic,
};
use core::time::Duration;
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use web_sys::DedicatedWorkerGlobalScope;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = reportError)]
    fn report_error(error: &js_sys::Error);
}

struct Handler {
    scope: DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
}
impl Drop for Handler {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

#[derive(Er)]
pub(super) struct RespondErr {
    pub(super) cause: WorkerCause,
}
impl From<ErTree<RespondErr>> for WireFailure {
    fn from(tree: ErTree<RespondErr>) -> Self {
        let top = WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, tree.top.cause.clone());
        er_new!(top, tree).into()
    }
}
pub(super) fn respond(
    scope: &DedicatedWorkerGlobalScope,
    output: Outgoing,
) -> ErResult<(), RespondErr> {
    respond_with(scope, output, None)
}

fn respond_with(
    scope: &DedicatedWorkerGlobalScope,
    output: Outgoing,
    buffers: Option<&Buffers>,
) -> ErResult<(), RespondErr> {
    let buffer = buffer::prepare(&output, buffers).er_with(|err| RespondErr::new(err.cause()))?;
    let sent = if (buffer.byte_length() as usize) < buffer::MIN_REUSE {
        scope.post_message(&buffer)
    } else {
        let transfers = js_sys::Array::new();

        transfers.push(&buffer);
        scope.post_message_with_transfer(&buffer, &transfers)
    };

    browser_result(sent).er(|| RespondErr::new(WorkerCause::Transport))
}

#[derive(Er)]
#[er(format = "serve needs a Dedicated Worker")]
pub(super) struct ScopeErr;
pub(super) fn scope() -> ErResult<DedicatedWorkerGlobalScope, ScopeErr> {
    JsValue::from(js_sys::global())
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .ok()
        .er(())
}

#[derive(Er)]
pub(super) struct SendNoticeErr {
    notice: &'static str,
    cause: WorkerCause,
}
pub(super) fn send_start_failure(
    scope: &DedicatedWorkerGlobalScope,
    failure: Outgoing,
    release: impl FnOnce(),
) -> ErResult<(), SendNoticeErr> {
    respond(scope, failure)
        .er_with(|error| SendNoticeErr::new("startup failure", error.cause.clone()))?;
    release();
    respond(scope, Outgoing::SetupRetired)
        .er_with(|error| SendNoticeErr::new("retirement", error.cause.clone()))?;
    close_after_bootstrap(scope.clone());
    Ok(())
}

fn close_after_bootstrap(scope: DedicatedWorkerGlobalScope) {
    // Let the bootstrap receive the serve result before closing the worker's event loop.
    wasm_bindgen_futures::spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(0).await;
        scope.close();
    });
}

#[derive(Er)]
#[er(format = "Could not send the {notice} notice to the page")]
struct NoticeErr {
    notice: &'static str,
}
pub(super) fn log_error(report: &str) {
    report_error(&js_sys::Error::new(report));
}

pub(super) fn pending(notice: Outgoing) {
    if let Err(post) = report_to_page(notice.clone())
        && let Err(retry) = report_to_page(notice)
    {
        log_error(&er_new!(NoticeErr::new("pending"), [post, retry]).er_report());
    }
}

#[derive(Er)]
pub(super) enum ReportErr {
    Scope,
    Send(WorkerCause),
}
pub(super) fn report_to_page(message: Outgoing) -> ErResult<(), ReportErr> {
    let scope = scope().er(|| ReportErr::Scope)?;
    respond(&scope, message).er_with(|err| ReportErr::send(err.cause.clone()))
}
pub struct WorkerInterval<S> {
    pub every: Duration,
    pub run: AktorClosure<dyn AktorIntervalLogic<S>>,
}

pub struct ServerSetup {
    pub(super) scope: DedicatedWorkerGlobalScope,
    pub(super) options: Options,
    pub(super) operations: Operations,
    registry: registry::Registry,
}
#[derive(Er)]
pub(super) enum PrepareServerErr {
    Scope,
    Options,
    Registry,
}
impl ServerSetup {
    pub(super) fn new<S: 'static, Role: 'static>(
        options: Options,
    ) -> ErResult<Self, PrepareServerErr> {
        options.validate().er(|| PrepareServerErr::Options)?;
        let scope = scope().er(|| PrepareServerErr::Scope)?;
        let registry =
            registry::Registry::for_actor::<S, Role>().er(|| PrepareServerErr::Registry)?;
        registry
            .check_handlers()
            .er(|| PrepareServerErr::Registry)?;
        let operations = registry.operations();

        Ok(Self {
            scope,
            options,
            operations,
            registry,
        })
    }
}

pub(super) struct EndFailure {
    error: WireFailure,
    sent: bool,
}

impl EndFailure {
    pub(super) fn post(error: WireFailure) -> Self {
        let sent = match report_to_page(Outgoing::EndReturned(error.clone())) {
            Ok(()) => true,
            Err(post) => {
                log_error(&post.er_report());
                false
            }
        };
        Self { error, sent }
    }
}

pub(super) async fn serve_logic<S: 'static, C, CF>(
    mut state: S,
    ready: (&'static str, Result<Vec<u8>, (WorkerError, String)>),
    cleanup: C,
    setup: ServerSetup,
    mut hooks: CallHooks<S>,
    mut intervals: Vec<WorkerInterval<S>>,
) -> Result<(), (WorkerError, String)>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), EndFailure>> + 'static,
{
    let ServerSetup {
        scope,
        options,
        registry,
        operations,
    } = setup;
    let (sender, mut receiver) = mpsc::channel::<Incoming>(options.capacity.get());
    let (stopping, mut stopped) = watch::channel(false);
    let closing = Rc::new(Cell::new(false));
    let replies = scope.clone();
    let buffers = Rc::new(Buffers::new(options.max_outstanding_bytes.get()));
    let received = buffers.clone();

    let callback = Closure::new(move |event: MessageEvent| {
        let input = read_frame(event)
            .er_with(|err| WorkerError::new(AktorDeliveryOutcome::Discarded, err.cause()));
        let input = input.map(|(input, buffer)| {
            if matches!(input, Incoming::Call { .. }) {
                received.retain(buffer);
            }

            input
        });

        match input {
            Ok(Incoming::Shutdown) => {
                closing.set(true);
                stopping.send_replace(true);
            }

            Ok(input @ Incoming::Call { id, .. }) => {
                let result = if closing.get() {
                    Err(WireFailure::new(
                        AktorDeliveryOutcome::Discarded,
                        WorkerCause::Closed,
                    ))
                } else {
                    match sender.try_send(input) {
                        Ok(()) => Ok(()),
                        Err(mpsc::error::TrySendError::Full(_)) => Err(WireFailure::new(
                            AktorDeliveryOutcome::Discarded,
                            WorkerCause::Full,
                        )),
                        Err(mpsc::error::TrySendError::Closed(_)) => Err(WireFailure::new(
                            AktorDeliveryOutcome::Discarded,
                            WorkerCause::Closed,
                        )),
                    }
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
                    fatal(error.into());
                }
            }
            Ok(Incoming::Initialize { .. }) => fatal(WireFailure::new(
                AktorDeliveryOutcome::Discarded,
                WorkerCause::Protocol,
            )),
            Err(error) => fatal(error.into()),
        }
    });

    scope.set_onmessage(Some(callback.as_ref().unchecked_ref()));
    let (payload_type, payload) = ready;
    let (announcement, mut failure) = match payload {
        Ok(payload) => (
            Outgoing::Ready {
                payload_type: payload_type.into(),
                payload,
                version: VERSION,
                options,
                operations,
            },
            None,
        ),
        Err(error) => {
            let failure = WireFailure::from(error);
            (Outgoing::ReadyFailed(failure.clone()), Some(failure))
        }
    };
    if let Err(error) = respond(&scope, announcement) {
        let error = WireFailure::from(error);
        log_error(&error.report);
        failure = Some(error);
    }

    let replies = scope.clone();
    let _handler = Handler {
        scope,
        _callback: callback,
    };

    let mut closing = false;
    let mut next: Vec<_> = intervals
        .iter()
        .map(|interval| {
            crate::group::shutdown_deadline(crate::group::Instant::now(), interval.every)
        })
        .collect();

    while failure.is_none() {
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
                    let operation = Operation::INTERVAL;
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
            let error = WireFailure::from(error);
            log_error(&error.report);
            failure = Some(error);
            break;
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
            Some(operation) => match operation.execute(&mut state, &input).await {
                Ok(output) => Ok(output),
                Err(error) => Err(error.into()),
            },
            None => Err(WireFailure::new(
                AktorDeliveryOutcome::Discarded,
                WorkerCause::UnknownOperation(operation),
            )),
        };
        drop(input);

        if let Some(operation) = metadata {
            hooks.after(&mut state, operation);
        }

        if let Err(error) = respond_with(&replies, Outgoing::Answer { id, output }, Some(&buffers))
        {
            let error = WireFailure::from(error);
            log_error(&error.report);
            failure = Some(error);
            break;
        }
    }

    if failure.is_some() {
        receiver.close();
        let _shutdown = stopped.wait_for(|stopping| *stopping).await;
    }
    let (result, sent_end) = match cleanup(state).await {
        Ok(()) => (Ok(()), false),
        Err(failure) => (Err(failure.error), failure.sent),
    };

    drop((hooks, intervals));
    buffers.clear();

    let retired = if sent_end {
        Outgoing::Retired
    } else {
        Outgoing::Finished(result.clone())
    };

    let result = match (failure, result) {
        (None, result) => result,
        (Some(failure), Ok(())) => Err(failure),
        (Some(failure), Err(mut cleanup)) => {
            cleanup.report = er_new!(
                LifecycleErr,
                [
                    failure.without_data::<()>(),
                    cleanup.clone().without_data::<()>()
                ]
            )
            .er_report();
            Err(cleanup)
        }
    };

    if let Err(post) = respond(&replies, retired) {
        let error = WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, post.top.cause.clone());
        match result {
            Ok(()) => er_bail!(error, post),
            Err(failure) => er_bail!(error.er_save("worker lifecycle", failure.report), post),
        }
    }

    close_after_bootstrap(replies);
    Ok(result?)
}

#[derive(Er)]
#[er(format = "worker operation and cleanup both failed")]
struct LifecycleErr;

#[derive(Er)]
struct EncodeLifecycleErr;
pub fn lifecycle_error<E: AktorData>(mut error: WireFailure, data: &E) -> WireFailure {
    match encode(data) {
        Ok(bytes) => error.error.data = Some(bytes),
        Err((_, codec)) => {
            error.report = er_new!(
                EncodeLifecycleErr
                    .er_save("worker failure", error.report)
                    .er_save("codec", codec)
            )
            .er_report()
        }
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
