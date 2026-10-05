use super::*;
use crate::{AktorClosures, local::hooks::AktorHooks, setup::kind::BrowserWebWorker};
use core::ops::AsyncFnOnce;
use server::{WorkerInterval, respond, scope};
use wasm_bindgen::JsCast;

struct Initialize {
    scope: web_sys::DedicatedWorkerGlobalScope,
    _callback: Closure<dyn FnMut(MessageEvent)>,
}
impl Drop for Initialize {
    fn drop(&mut self) {
        self.scope.set_onmessage(None);
    }
}

async fn configuration<Config: DeserializeOwned + 'static>(
    options: Options,
    operations: Operations,
) -> Result<Config, WireError> {
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
                Incoming::Initialize { config } => {
                    decode(&config).map_err(WorkerError::without_data)
                }
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

pub async fn serve_setup<Role: 'static, S: 'static, Config: DeserializeOwned + 'static, Start>(
    _: Role,
    options: Options,
    logic: impl FnOnce(Config) -> AktorClosures<S, Start, BrowserWebWorker<S>>,
) -> Result<Server, WireError>
where
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError>,
{
    options.validate().map_err(WorkerError::without_data)?;

    let operations = registry::Registry::for_actor::<S, Role>()
        .map_err(WorkerError::without_data)?
        .operations();
    let config = match configuration(options.clone(), operations).await {
        Ok(config) => config,
        Err(error) => {
            respond(&scope()?, Outgoing::SetupFailed(error.clone()))?;
            return Err(error);
        }
    };

    let AktorClosures {
        start,
        end,
        intervals,
        before_each,
        after_each,
    } = logic(config);

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        let error = WireError::new(
            CallError::NotAdmitted,
            WorkerCause::Setup("interval duration must be positive".into()),
        );

        respond(&scope()?, Outgoing::SetupFailed(error.clone()))?;
        return Err(error);
    }

    let state = match start().await {
        Ok(state) => state,
        Err(error) => {
            setup_failed(error.clone())?;
            return Err(WireError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup(error.diagnostics),
            ));
        }
    };

    server::serve_logic::<Role, _, _, _, ()>(
        state,
        async move |state| {
            if let Some(mut end) = end {
                end.0.run(state).await
            } else {
                drop(state);
                Ok(())
            }
        },
        options,
        AktorHooks {
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
}
