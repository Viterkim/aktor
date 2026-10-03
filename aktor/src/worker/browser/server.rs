use super::*;
use tokio::sync::mpsc;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::DedicatedWorkerGlobalScope;

pub struct Server {
    scope: DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
    finished: oneshot::Receiver<Result<(), WireError>>,
}
impl Server {
    /// Keep serving until shutdown and cleanup finish.
    pub async fn wait(mut self) -> Result<(), WorkerError> {
        let result = (&mut self.finished).await.unwrap_or_else(|_| {
            Err(WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Closed,
            ))
        });
        drop(self);
        result.map_err(WireError::without_data)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

fn respond(scope: &DedicatedWorkerGlobalScope, output: Outgoing) -> Result<(), WireError> {
    let output = postcard::to_allocvec(&output).map_err(|error| {
        WireError::new(
            CallError::OutcomeUnknown,
            WorkerCause::Codec(error.to_string()),
        )
    })?;
    let array = js_sys::Uint8Array::from(output.as_slice());
    let buffer = array.buffer();
    let transfers = js_sys::Array::new();
    transfers.push(&buffer);
    scope
        .post_message_with_transfer(&buffer, &transfers)
        .map_err(|error| {
            WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Codec(format!("{error:?}")),
            )
        })
}

fn scope() -> Result<DedicatedWorkerGlobalScope, WireError> {
    JsValue::from(js_sys::global())
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map_err(|_| {
            WireError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup("serve needs a Dedicated Worker".into()),
            )
        })
}

pub fn setup_failed<T: Serialize>(error: crate::AktorSetupError<T>) -> Result<(), WireError> {
    respond(
        &scope()?,
        Outgoing::SetupFailed(lifecycle_error(
            CallError::NotAdmitted,
            WorkerCause::Setup(error.diagnostics),
            &error.data,
        )),
    )
}

pub fn serve<S: 'static>(state: S) -> Result<Server, WireError> {
    serve_with(
        state,
        async |state| {
            drop(state);
            Ok::<_, crate::AktorCleanupError>(())
        },
        Options::default(),
    )
}

pub fn serve_with<S: 'static, C, CF, E: Serialize + 'static>(
    state: S,
    cleanup: C,
    options: Options,
) -> Result<Server, WireError>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), crate::AktorCleanupError<E>>> + 'static,
{
    serve_for::<(), _, _, _, E>(state, cleanup, options)
}

/// Pick the marker used by #[aktor(actor = ...)] when actors share a state type.
pub fn serve_for<Role: 'static, S: 'static, C, CF, E: Serialize + 'static>(
    mut state: S,
    cleanup: C,
    options: Options,
) -> Result<Server, WireError>
where
    C: FnOnce(S) -> CF + 'static,
    CF: Future<Output = Result<(), crate::AktorCleanupError<E>>> + 'static,
{
    let scope = scope()?;
    options.validate().map_err(|error| error.without_data())?;

    let (sender, mut receiver) = mpsc::channel::<Incoming>(options.capacity);
    let (stopping, mut stopped) = watch::channel(false);
    let closing = Rc::new(Cell::new(false));
    let replies = scope.clone();

    let callback = Closure::new(move |event: MessageEvent| {
        let input = event
            .data()
            .dyn_into::<js_sys::ArrayBuffer>()
            .map_err(|_| WireError::new(CallError::Discarded, WorkerCause::Protocol))
            .and_then(|buffer| {
                postcard::from_bytes::<Incoming>(&js_sys::Uint8Array::new(&buffer).to_vec())
                    .map_err(|error| {
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
    let (finished, completion) = oneshot::channel();
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

            let output = registry::dispatch::<S, Role>(&mut state, operation, input)
                .await
                .map_err(|error| error.without_data());

            if let Err(error) = respond(&replies, Outgoing::Answer { id, output }) {
                fatal(error);
            }
        }

        let result = match cleanup(state).await {
            Ok(()) => Ok(()),
            Err(error) => Err(lifecycle_error(
                CallError::OutcomeUnknown,
                WorkerCause::Cleanup(error.diagnostics),
                &error.data,
            )),
        };
        if let Err(error) = respond(&replies, Outgoing::Finished(result.clone())) {
            fatal(error);
        }
        let _sent = finished.send(result);
    });

    respond(
        &scope,
        Outgoing::Ready {
            version: VERSION,
            options,
            operations: Operations::for_actor::<S, Role>(),
        },
    )?;

    Ok(Server {
        scope,
        _callback: callback,
        finished: completion,
    })
}

fn lifecycle_error<E: Serialize>(outcome: CallError, cause: WorkerCause, data: &E) -> WireError {
    let mut error = WireError::new(outcome, cause);
    match encode(data) {
        Ok(data) => error.data = Some(data),
        Err(codec) => error = error.data_error(codec),
    }
    error
}
