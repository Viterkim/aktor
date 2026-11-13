use super::*;
use wasm_bindgen::JsCast;
use web_sys::{ErrorEvent, WorkerOptions, WorkerType};

impl<S: 'static, Role: 'static, E> Worker<S, Role, E> {
    #[doc(hidden)]
    pub fn with_config<Config: Serialize>(
        url: &str,
        options: Options,
        config: &Config,
    ) -> Result<Self, (WorkerError, String)> {
        Self::configured(url, options, config, false).map_err(Into::into)
    }

    pub(crate) fn configured<Config: Serialize>(
        url: &str,
        options: Options,
        config: &Config,
        optional: bool,
    ) -> Result<Self, WorkerFailure> {
        let config = encode(config)?;
        let worker = Self::with_policy(url, options, optional)?;

        worker.inner.accepted.set(false);
        *worker.inner.initialize.borrow_mut() = Some(config);
        Ok(worker)
    }

    pub(crate) fn retain_unconfirmed(self, retain: bool) -> Self {
        self.inner.retain_unconfirmed.set(retain);
        self
    }

    #[doc(hidden)]
    pub fn new(url: &str) -> Result<Self, (WorkerError, String)> {
        Self::with_options(url, Options::default())
    }

    #[doc(hidden)]
    pub fn with_options(url: &str, options: Options) -> Result<Self, (WorkerError, String)> {
        Self::with_policy(url, options, false).map_err(Into::into)
    }

    pub(crate) fn with_policy(
        url: &str,
        options: Options,
        optional: bool,
    ) -> Result<Self, WorkerFailure> {
        options.validate_limits()?;

        let expected = registry::Registry::for_actor::<S, Role>()?.operations();

        let js_options = WorkerOptions::new();

        js_options.set_type(WorkerType::Module);

        let worker = web_sys::Worker::new_with_options(url, &js_options).map_err(|error| {
            WorkerFailure::setup(CallError::NotAdmitted, format!("{url}: {error:?}"))
        })?;

        let (ready, _) = watch::channel(None);
        let (finished, _) = watch::channel(None);
        let (retirement, _) = watch::channel(AktorRetireStatus::Unconfirmed);
        let inner = Rc::new(Inner {
            worker,
            outstanding: RefCell::new(HashMap::new()),
            queue: RefCell::new(VecDeque::new()),
            active: Cell::new(None),
            executing: Cell::new(false),
            pump_scheduled: Cell::new(false),
            shutdown_sent: Cell::new(false),
            shutdown_join: RefCell::new(ShutdownJoin::Ready),
            pending_finish: RefCell::new(None),
            next: Cell::new(0),
            handles: Cell::new(1),
            closed: Cell::new(false),
            failed: Cell::new(false),
            waiting_to_terminate: Cell::new(false),
            optional,
            accepted: Cell::new(true),
            retain_unconfirmed: Cell::new(false),
            uncertain: RefCell::new(Vec::new()),
            failure: RefCell::new(None),
            setup_failure_pending: Cell::new(false),
            diagnostics: Rc::new(RefCell::new(Vec::new())),
            count: Arc::new(Semaphore::new(options.capacity)),
            bytes: Arc::new(Semaphore::new(options.max_outstanding_bytes)),
            buffers: Buffers::new(options.max_outstanding_bytes),
            options,
            initialize: RefCell::new(None),
            group: RefCell::new(None),
            sessions: RefCell::new(Vec::new()),
            prune_at: Cell::new(64),
            ready,
            finished,
            retirement,
            end: watch::channel(End::Waiting).0,
            retained: RefCell::new(None),
            message: RefCell::new(None),
            error: RefCell::new(None),
        });

        let weak = Rc::downgrade(&inner);
        let message = Closure::new(move |event: MessageEvent| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            if inner.closed.get() && !inner.accepted.get() {
                return;
            }

            let output = event
                .data()
                .dyn_into::<js_sys::ArrayBuffer>()
                .map_err(|_| WireError::new(CallError::OutcomeUnknown, WorkerCause::Protocol))
                .and_then(|buffer| {
                    let output = postcard::from_bytes::<Outgoing>(
                        &js_sys::Uint8Array::new(&buffer).to_vec(),
                    );

                    if matches!(output, Ok(Outgoing::Answer { .. }))
                        && inner.finished.borrow().is_none()
                    {
                        inner.buffers.retain(buffer);
                    }

                    output.map_err(|error| {
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
                    if inner.closed.get() {
                        return;
                    }

                    if let Err(cause) =
                        compatible(&inner.options, &expected, version, &options, &operations)
                    {
                        if version == VERSION
                            && inner.initialize.borrow().is_some()
                            && inner.ready.borrow().is_none()
                        {
                            inner.fail_start(ReadyFailure::not_run(WireError::new(
                                CallError::NotAdmitted,
                                cause,
                            )));
                        } else {
                            inner.fail(cause);
                        }
                    } else if inner.ready.borrow().is_some() {
                        inner.fail(WorkerCause::Protocol);
                    } else if let Some(config) = inner.initialize.borrow_mut().take() {
                        if let Err(error) = impls::inner::post(
                            &inner.worker,
                            &inner.buffers,
                            &Incoming::Initialize { config },
                        ) {
                            inner.fail_start(ReadyFailure::not_run(error));
                        } else {
                            inner.accepted.set(true);
                        }
                    } else {
                        inner.fail_error(WireError::setup(
                            CallError::OutcomeUnknown,
                            "worker requires configuration",
                        ));
                    }
                }
                Ok(Outgoing::Ready {
                    version,
                    options,
                    operations,
                }) => {
                    inner.accepted.set(true);

                    if inner.failed.get() {
                        inner.setup_failure_pending.set(false);
                        inner.continue_shutdown(false);
                    } else if let Err(cause) =
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

                Ok(Outgoing::SetupRetired) => inner.setup_retired(),

                Ok(Outgoing::InitNotRun {
                    version: VERSION,
                    error,
                }) => inner.setup_failed(ReadyFailure::not_run(error)),
                Ok(Outgoing::InitNotRun { .. }) => inner.fail(WorkerCause::Protocol),
                Ok(Outgoing::SetupFailed(error)) => inner.setup_failed(error.into()),
                Ok(Outgoing::SetupPending(report)) => inner.setup_pending(report),

                Ok(Outgoing::Unconfirmed(error)) => inner.retain_failure(error),

                Ok(Outgoing::Started { id }) => {
                    if inner.active.get() == Some(id) {
                        inner.executing.set(true);
                    } else {
                        inner.fail(WorkerCause::Protocol);
                    }
                }

                Ok(Outgoing::Answer { id, output }) => {
                    if inner.failed.get() {
                        return;
                    }

                    if inner.active.get() != Some(id) {
                        inner.fail(WorkerCause::Protocol);
                        return;
                    }

                    inner.active.set(None);
                    inner.executing.set(false);

                    let wire_failure = output.as_ref().err().cloned();
                    let work = inner.outstanding.borrow_mut().remove(&id);
                    let failure = match work {
                        Some(work) => match (work.service, work.answer) {
                            (Some(service), None) => service.answer(output),
                            (None, Some(answer)) => answer(output),
                            (None, None) => wire_failure,
                            (Some(_), Some(_)) => {
                                inner.fail(WorkerCause::Protocol);
                                return;
                            }
                        },
                        None => {
                            inner.fail(WorkerCause::Protocol);
                            return;
                        }
                    };

                    if let Some(error) = failure {
                        inner.reject(error);
                    }

                    inner.pump();
                }

                Ok(Outgoing::Finished(result)) => {
                    if !inner.shutdown_sent.get() || !inner.outstanding.borrow().is_empty() {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        if *inner.retirement.borrow() == AktorRetireStatus::Unconfirmed {
                            inner.end.send_replace(End::Returned(result.clone()));
                        }

                        inner.finish(result);
                    }
                }
                Err(error) => inner.fail_error(error),
            }
        });

        inner
            .worker
            .set_onmessage(Some(message.as_ref().unchecked_ref()));
        *inner.message.borrow_mut() = Some(message);

        let weak = Rc::downgrade(&inner);
        let url = url.to_owned();
        let error = Closure::new(move |event: Event| {
            if let Some(inner) = weak.upgrade() {
                inner.script_failed(worker_error(&event, &url));
            }
        });

        inner
            .worker
            .set_onerror(Some(error.as_ref().unchecked_ref()));
        *inner.error.borrow_mut() = Some(error);

        *inner.retained.borrow_mut() = Some(inner.clone());

        Ok(Self {
            inner,
            state: PhantomData,
        })
    }
}

fn worker_error(event: &Event, url: &str) -> String {
    let (message, filename, line, column) = event
        .dyn_ref::<ErrorEvent>()
        .map(|error| {
            (
                error.message(),
                error.filename(),
                error.lineno(),
                error.colno(),
            )
        })
        .unwrap_or_default();
    let source = if filename.is_empty() { url } else { &filename };
    let message = if message.is_empty() {
        "worker failed to load or execute"
    } else {
        &message
    };

    match (line, column) {
        (0, 0) => format!("{source}: {message}"),
        (0, column) => format!("{source} (column {column}): {message}"),
        (line, 0) => format!("{source}:{line}: {message}"),
        (line, column) => format!("{source}:{line}:{column}: {message}"),
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
