use super::*;
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::DedicatedWorkerGlobalScope;

pub struct Server {
    scope: DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

fn respond(scope: &DedicatedWorkerGlobalScope, output: Outgoing) -> Result<(), WorkerError> {
    let output = encode(&output)?;

    scope
        .post_message(&JsValue::from_str(&output))
        .map_err(|error| {
            WorkerError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Codec(format!("{error:?}")),
            )
        })
}

fn scope() -> Result<DedicatedWorkerGlobalScope, WorkerError> {
    JsValue::from(js_sys::global())
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map_err(|_| {
            WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup("serve needs a Dedicated Worker".into()),
            )
        })
}

pub fn setup_failed(error: impl std::fmt::Display) -> Result<(), WorkerError> {
    respond(
        &scope()?,
        Outgoing::SetupFailed(WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Setup(error.to_string()),
        )),
    )
}

pub fn serve<S: 'static, F>(state: S, dispatch: F) -> Result<Server, WorkerError>
where
    F: for<'a> FnMut(&'a mut S, String, String) -> LocalFuture<'a, Result<String, WorkerError>>
        + 'static,
{
    serve_with(
        state,
        dispatch,
        async |state| {
            drop(state);
            Ok(())
        },
        Options::default(),
    )
}

pub fn serve_with<S: 'static, F, C, CF>(
    mut state: S,
    mut dispatch: F,
    cleanup: C,
    options: Options,
) -> Result<Server, WorkerError>
where
    F: for<'a> FnMut(&'a mut S, String, String) -> LocalFuture<'a, Result<String, WorkerError>>
        + 'static,
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), WorkerError>> + 'static,
{
    let scope = scope()?;
    options.validate()?;

    let (sender, mut receiver) = mpsc::channel::<Incoming>(options.capacity);
    let (stopping, mut stopped) = watch::channel(false);
    let closing = Rc::new(Cell::new(false));
    let replies = scope.clone();
    let limit = options.max_payload_bytes;

    let callback = Closure::new(move |event: MessageEvent| {
        let input = event
            .data()
            .as_string()
            .ok_or_else(|| WorkerError::new(CallError::Discarded, WorkerCause::Protocol))
            .and_then(|data| decode::<Incoming>(&data));

        match input {
            Ok(Incoming::Shutdown) => {
                closing.set(true);
                stopping.send_replace(true);
            }

            Ok(input @ Incoming::Call { .. }) => {
                let (id, size) = match &input {
                    Incoming::Call {
                        id,
                        operation,
                        input,
                    } => (*id, operation.len().saturating_add(input.len())),
                    _ => return,
                };

                let cause = if closing.get() {
                    Some(WorkerCause::Closed)
                } else if size > limit {
                    Some(WorkerCause::PayloadTooLarge)
                } else {
                    None
                };

                let result = if let Some(cause) = cause {
                    Err(WorkerError::new(CallError::Discarded, cause))
                } else {
                    sender.try_send(input).map_err(|error| {
                        WorkerError::new(
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
                    && let Err(error) = respond(
                        &replies,
                        Outgoing::Answer {
                            id,
                            output: Err(error),
                        },
                    )
                {
                    fatal(error);
                }
            }
            Err(error) => fatal(error),
        }
    });
    scope.set_onmessage(Some(callback.as_ref().unchecked_ref()));

    let replies = scope.clone();
    let max_output = options.max_payload_bytes;
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            let input = tokio::select! {
                biased;
                _ = stopped.changed() => { receiver.close(); receiver.recv().await },
                input = receiver.recv() => input,
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

            let output = dispatch(&mut state, operation, input)
                .await
                .and_then(|output| {
                    if output.len() > max_output {
                        Err(WorkerError::new(
                            CallError::OutcomeUnknown,
                            WorkerCause::PayloadTooLarge,
                        ))
                    } else {
                        Ok(output)
                    }
                });

            if let Err(error) = respond(&replies, Outgoing::Answer { id, output }) {
                fatal(error);
            }
        }

        let result = cleanup(state).await;
        if let Err(error) = respond(&replies, Outgoing::Finished(result)) {
            fatal(error);
        }
    });

    respond(
        &scope,
        Outgoing::Ready {
            version: VERSION,
            options,
        },
    )?;

    Ok(Server {
        scope,
        _callback: callback,
    })
}
