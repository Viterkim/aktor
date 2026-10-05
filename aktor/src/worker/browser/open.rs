use super::*;
use wasm_bindgen::JsCast;
use web_sys::{WorkerOptions, WorkerType};

impl<S: 'static, Role: 'static, E> Worker<S, Role, E> {
    pub fn with_config<Config: Serialize>(
        url: &str,
        options: Options,
        config: &Config,
    ) -> Result<Self, WorkerError> {
        let config = encode(config)?;
        let worker = Self::with_options(url, options)?;

        *worker.inner.initialize.borrow_mut() = Some(config);
        Ok(worker)
    }

    pub fn new(url: &str) -> Result<Self, WorkerError> {
        Self::with_options(url, Options::default())
    }

    pub fn with_options(url: &str, options: Options) -> Result<Self, WorkerError> {
        options.validate()?;

        let expected = registry::Registry::for_actor::<S, Role>()?.operations();

        let js_options = WorkerOptions::new();

        js_options.set_type(WorkerType::Module);

        let worker = web_sys::Worker::new_with_options(url, &js_options).map_err(|error| {
            WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup(format!("{error:?}")),
            )
        })?;

        let (ready, _) = watch::channel(None);
        let (finished, _) = watch::channel(None);
        let inner = Rc::new(Inner {
            worker,
            outstanding: RefCell::new(HashMap::new()),
            queue: RefCell::new(VecDeque::new()),
            active: Cell::new(None),
            executing: Cell::new(false),
            pump_scheduled: Cell::new(false),
            shutdown_sent: Cell::new(false),
            next: Cell::new(0),
            handles: Cell::new(1),
            closed: Cell::new(false),
            failed: Cell::new(false),
            count: Arc::new(Semaphore::new(options.capacity)),
            bytes: Arc::new(Semaphore::new(options.max_outstanding_bytes)),
            options,
            initialize: RefCell::new(None),
            group: RefCell::new(None),
            sessions: RefCell::new(Vec::new()),
            prune_at: Cell::new(64),
            ready,
            finished,
            retained: RefCell::new(None),
            message: RefCell::new(None),
            error: RefCell::new(None),
        });

        let weak = Rc::downgrade(&inner);
        let message = Closure::new(move |event: MessageEvent| {
            let Some(inner) = weak.upgrade() else {
                return;
            };

            let output = event
                .data()
                .dyn_into::<js_sys::ArrayBuffer>()
                .map_err(|_| WireError::new(CallError::OutcomeUnknown, WorkerCause::Protocol))
                .and_then(|buffer| {
                    postcard::from_bytes::<Outgoing>(&js_sys::Uint8Array::new(&buffer).to_vec())
                        .map_err(|error| {
                            WireError::new(
                                CallError::OutcomeUnknown,
                                WorkerCause::Codec(error.to_string()),
                            )
                        })
                });

            match output {
                Ok(Outgoing::Configure {
                    version,
                    options,
                    operations,
                }) => {
                    if let Err(cause) =
                        compatible(&inner.options, &expected, version, &options, &operations)
                    {
                        inner.fail(cause);
                    } else if inner.ready.borrow().is_some() {
                        inner.fail(WorkerCause::Protocol);
                    } else if let Some(config) = inner.initialize.borrow_mut().take() {
                        if let Err(error) =
                            impls::inner::post(&inner.worker, &Incoming::Initialize { config })
                        {
                            inner.fail_error(error);
                        }
                    } else {
                        inner.fail(WorkerCause::Setup("worker requires configuration".into()));
                    }
                }
                Ok(Outgoing::Ready {
                    version,
                    options,
                    operations,
                }) => {
                    if let Err(cause) =
                        compatible(&inner.options, &expected, version, &options, &operations)
                    {
                        inner.fail(cause);
                    } else if inner.ready.borrow().is_some() || inner.initialize.borrow().is_some()
                    {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        inner.ready.send_replace(Some(Ok(())));
                        inner.pump();
                    }
                }

                Ok(Outgoing::SetupFailed(error)) => {
                    inner.ready.send_replace(Some(Err(error.clone())));
                    inner.fail_error(error);
                }

                Ok(Outgoing::Started { id }) => {
                    if inner.active.get() == Some(id) {
                        inner.executing.set(true);
                    } else {
                        inner.fail(WorkerCause::Protocol);
                    }
                }

                Ok(Outgoing::Answer { id, output }) => {
                    if inner.active.get() != Some(id) {
                        inner.fail(WorkerCause::Protocol);
                        return;
                    }

                    if let Err(error) = output {
                        inner.fail_error(error);
                        return;
                    }

                    inner.active.set(None);
                    inner.executing.set(false);

                    let work = inner.outstanding.borrow_mut().remove(&id);

                    if let Some(work) = work {
                        match (work.service, work.answer) {
                            (Some(service), None) => service.answer(output),
                            (None, Some(answer)) => {
                                let _sent = answer.send(output);
                            }
                            (None, None) => {}
                            (Some(_), Some(_)) => inner.fail(WorkerCause::Protocol),
                        }
                    } else {
                        inner.fail(WorkerCause::Protocol);
                    }

                    inner.pump();
                }

                Ok(Outgoing::Finished(result)) => {
                    if !inner.shutdown_sent.get() || !inner.outstanding.borrow().is_empty() {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        inner.finish(result);
                    }
                }
                Err(error) => inner.fail(error.cause),
            }
        });

        inner
            .worker
            .set_onmessage(Some(message.as_ref().unchecked_ref()));
        *inner.message.borrow_mut() = Some(message);

        let weak = Rc::downgrade(&inner);
        let error = Closure::new(move |event: ErrorEvent| {
            if let Some(inner) = weak.upgrade() {
                let cause = if matches!(*inner.ready.borrow(), Some(Ok(()))) {
                    WorkerCause::Crashed(event.message())
                } else {
                    WorkerCause::Setup(event.message())
                };

                inner.fail(cause);
            }
        });

        inner
            .worker
            .set_onerror(Some(error.as_ref().unchecked_ref()));
        *inner.error.borrow_mut() = Some(error);

        Ok(Self {
            inner,
            state: PhantomData,
        })
    }
}

fn compatible(
    expected_options: &Options,
    expected_operations: &Operations,
    version: u32,
    options: &Options,
    operations: &Operations,
) -> Result<(), WorkerCause> {
    if version != VERSION
        || options.build != expected_options.build
        || expected_options.capacity > options.capacity
        || expected_options.max_outstanding_bytes > options.max_outstanding_bytes
    {
        Err(WorkerCause::Protocol)
    } else if operations != expected_operations {
        Err(WorkerCause::Operations {
            expected: expected_operations.0.clone(),
            actual: operations.0.clone(),
        })
    } else {
        Ok(())
    }
}
