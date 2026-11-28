use super::*;
use wasm_bindgen::JsCast;
use web_sys::{ErrorEvent, WorkerOptions, WorkerType};

#[derive(Er)]
pub(crate) enum OpenErr {
    Config,
    Options,
    Registry,
    Create,
}
impl OpenErr {
    pub(super) fn public(&self) -> WorkerError {
        let cause = match self {
            Self::Config => WorkerCause::Codec,
            Self::Options | Self::Registry | Self::Create => WorkerCause::Setup,
        };
        WorkerError::new(AktorDeliveryOutcome::NotAdmitted, cause)
    }
}
impl From<ErTree<OpenErr>> for WireFailure {
    fn from(tree: ErTree<OpenErr>) -> Self {
        ErBranch::from(tree.top.public()).er_add(tree).into()
    }
}

impl<W: AktorWorkerContract> Worker<W> {
    /// A worker that never launched, so the group still has a handle to give back. Every call on
    /// it fails, and `failure` is what its start reports.
    pub(crate) fn closed(
        failure: ReadyFailure,
        name: &str,
        group: crate::AktorKillSwitch,
        dependency: Option<JoinReceipt>,
    ) -> Self {
        let inner = Self::new_inner(
            None,
            Options {
                build: String::new(),
                capacity: core::num::NonZeroUsize::MIN,
                max_outstanding_bytes: core::num::NonZeroUsize::MIN,
            },
            false,
            (name, group),
        );

        *inner.pending_finish.borrow_mut() = Some(Ok(()));
        if let Some(mut dependency) = dependency {
            *inner.shutdown_join.borrow_mut() = ShutdownJoin::Waiting(Box::pin(async move {
                let result = dependency
                    .wait_for(Option::is_some)
                    .await
                    .ok()
                    .and_then(|result| result.clone());
                match result {
                    Some(result) => result,
                    None => WorkerJoinOutcome::Unconfirmed(
                        WireFailure::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Closed)
                            .report,
                    ),
                }
            }));
        }

        let error = failure.error.clone();
        inner.ready.send_replace(Some(Err(failure)));
        inner.end.send_replace(End::Unconfirmed(error.clone()));
        inner.close_with_error(error, false);

        Self {
            state: PhantomData,
            receipt: Rc::new(OnceCell::new()),
            inner,
        }
    }

    fn new_inner(
        worker: Option<web_sys::Worker>,
        options: Options,
        optional: bool,
        owner: (&str, crate::AktorKillSwitch),
    ) -> Rc<Inner> {
        let (ready, _) = watch::channel(None);
        let (finished, _) = watch::channel(None);
        let (retirement, _) = watch::channel(None);
        Rc::new(Inner {
            worker,
            outstanding: RefCell::new(HashMap::new()),
            queue: RefCell::new(VecDeque::new()),
            active: Cell::new(None),
            executing: Cell::new(false),
            terminate_requested: Cell::new(false),
            pump_scheduled: Cell::new(false),
            shutdown_sent: Cell::new(false),
            shutdown_join: RefCell::new(ShutdownJoin::Ready),
            pending_finish: RefCell::new(None),
            next: Cell::new(0),
            handles: Cell::new(1),
            closed: Cell::new(false),
            failed: Cell::new(false),
            optional,
            accepted: Cell::new(false),
            retain_unconfirmed: Cell::new(false),
            uncertain: RefCell::new(Vec::new()),
            failure: RefCell::new(None),
            cleanup_pending: Cell::new(None),
            init_entered: Cell::new(false),
            setup_failure_pending: Cell::new(false),
            count: Arc::new(Semaphore::new(options.capacity.get())),
            bytes: Arc::new(Semaphore::new(options.max_outstanding_bytes.get())),
            buffers: Buffers::new(options.max_outstanding_bytes.get()),
            options,
            initialize: RefCell::new(None),
            name: owner.0.into(),
            group: owner.1,
            sessions: RefCell::new(Vec::new()),
            prune_at: Cell::new(64),
            ready,
            finished,
            retirement,
            end: watch::channel(End::Waiting).0,
            retained: RefCell::new(None),
            message: RefCell::new(None),
            error: RefCell::new(None),
        })
    }

    pub(crate) fn retain_unconfirmed(self, retain: bool) -> Self {
        self.inner.retain_unconfirmed.set(retain);
        self
    }

    pub(crate) fn configured(
        url: &str,
        options: Options,
        config: &W::Config,
        optional: bool,
        owner: (&str, crate::AktorKillSwitch),
    ) -> ErResult<Self, OpenErr> {
        let config = encode(config).er(|| OpenErr::Config)?;
        options.validate().er(|| OpenErr::Options)?;

        #[cfg(feature = "wasm_browser_worker_server")]
        warn_server_feature();

        let registry =
            registry::Registry::for_actor::<W::State, W::Role>().er(|| OpenErr::Registry)?;
        let expected = registry.operations();

        let js_options = WorkerOptions::new();
        js_options.set_type(WorkerType::Module);

        let worker = open_worker(url, &js_options).er(|| OpenErr::Create)?;

        let inner = Self::new_inner(Some(worker), options, optional, owner);
        *inner.initialize.borrow_mut() = Some((core::any::type_name::<W::Config>(), config));

        let receipt = Rc::new(OnceCell::new());
        let stored = receipt.clone();
        let weak = Rc::downgrade(&inner);
        let message = Closure::new(move |event: MessageEvent| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            if inner.retirement.borrow().is_some() {
                return;
            }

            let output = read_frame(event)
                .er_with(|err| WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, err.cause()));
            let output = output.map(|(output, buffer)| {
                if matches!(output, Outgoing::Answer { .. }) && inner.finished.borrow().is_none() {
                    inner.buffers.retain(buffer);
                }

                output
            });

            match output {
                Ok(Outgoing::Configure {
                    version,
                    options,
                    operations,
                    config_type,
                    payload_type,
                }) => {
                    if inner.closed.get() {
                        return;
                    }

                    let expected_config = inner.initialize.borrow().as_ref().map(|(name, _)| *name);
                    let handshake =
                        compatible(&inner.options, &expected, version, &options, &operations)
                            .and_then(|()| {
                                check_type(core::any::type_name::<W::Ready>(), &payload_type)
                            })
                            .and_then(|()| match expected_config {
                                Some(name) => check_type(name, &config_type),
                                None => Ok(()),
                            });

                    if let Err(cause) = handshake {
                        if version == VERSION
                            && inner.initialize.borrow().is_some()
                            && inner.ready.borrow().is_none()
                        {
                            inner.fail_start(ReadyFailure::not_run(HandshakeErr::failure(
                                cause,
                                AktorDeliveryOutcome::NotAdmitted,
                            )));
                        } else {
                            inner.fail_error(HandshakeErr::failure(
                                cause,
                                AktorDeliveryOutcome::OutcomeUnknown,
                            ));
                        }
                    } else if inner.ready.borrow().is_some() {
                        inner.fail(WorkerCause::Protocol);
                    } else if let Some((_, config)) = inner.initialize.borrow_mut().take() {
                        if let Err(error) = impls::inner::post(
                            inner.worker.as_ref(),
                            &inner.buffers,
                            &Incoming::Initialize { config },
                        ) {
                            inner.fail_start(ReadyFailure::not_run(
                                impls::inner::PostErr::failure(
                                    error,
                                    AktorDeliveryOutcome::NotAdmitted,
                                ),
                            ));
                        } else {
                            inner.accepted.set(true);
                        }
                    } else {
                        inner.fail_error(WireFailure::setup(
                            AktorDeliveryOutcome::OutcomeUnknown,
                            "worker requires configuration",
                        ));
                    }
                }
                Ok(Outgoing::Ready {
                    payload_type,
                    payload,
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
                            .and_then(|()| {
                                check_type(core::any::type_name::<W::Ready>(), &payload_type)
                            })
                    {
                        inner.fail_error(HandshakeErr::failure(
                            cause,
                            AktorDeliveryOutcome::OutcomeUnknown,
                        ));
                    } else if inner.ready.borrow().is_some() || inner.initialize.borrow().is_some()
                    {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        inner.init_entered.set(true);
                        inner.init_evidence(crate::AktorInitEvidence::Entered);
                        match decode(&payload) {
                            Ok(_) if inner.group.is_startup_aborted() => {
                                inner.close_with_error(
                                    WireFailure::new(
                                        AktorDeliveryOutcome::NotAdmitted,
                                        WorkerCause::Closed,
                                    ),
                                    false,
                                );
                            }
                            Ok(value) => {
                                inner.init_evidence(crate::AktorInitEvidence::Ready);
                                let _ = stored.set(Ok(value));
                                inner.ready.send_replace(Some(Ok(())));
                                inner.pump();
                            }
                            Err(error) => inner.fail_error(
                                er_new!(
                                    WorkerError::new(
                                        AktorDeliveryOutcome::NotAdmitted,
                                        WorkerCause::Codec
                                    ),
                                    error
                                )
                                .er_split()
                                .into(),
                            ),
                        }
                    }
                }

                Ok(Outgoing::SetupRetired) => inner.setup_retired(),

                Ok(Outgoing::InitNotRun {
                    version: VERSION,
                    error,
                }) => inner.setup_failed(ReadyFailure::not_run(error)),
                Ok(Outgoing::InitNotRun { .. }) => inner.fail(WorkerCause::Protocol),
                Ok(Outgoing::SetupFailed(error)) => {
                    inner.setup_failed(ReadyFailure::entered(error))
                }
                Ok(Outgoing::SetupPending(failure)) => inner.setup_pending(failure),
                Ok(Outgoing::ReadyFailed(failure)) => {
                    inner.init_evidence(crate::AktorInitEvidence::Entered);
                    inner.init_entered.set(true);
                    inner.fail_error(failure);
                }

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

                    let work = inner.outstanding.borrow_mut().remove(&id);
                    let Some(work) = work else {
                        inner.fail(WorkerCause::Protocol);
                        return;
                    };
                    let record = |mut error: WireFailure| {
                        let notice = inner.record_at(work.operation.phase(), &error.report);
                        error.error.failure_id = Some(notice);
                        error
                    };
                    let failure = match (work.service, work.answer) {
                        (Some(service), None) => service.answer(output, &record),
                        (None, Some(answer)) => answer(output, &record),
                        (None, None) => output.err().map(record),
                        (Some(_), Some(_)) => {
                            inner.fail(WorkerCause::Protocol);
                            return;
                        }
                    };

                    if let Some(error) = failure {
                        inner.reject_recorded(error);
                    }

                    inner.pump();
                }

                Ok(Outgoing::EndReturned(error)) if inner.shutdown_sent.get() => {
                    let _ = inner.end_returned(Err(error));
                }
                Ok(Outgoing::Retired) => {
                    let result = match &*inner.end.borrow() {
                        End::Returned(result) => Some(result.clone()),
                        _ => None,
                    };
                    if let Some(result) = result {
                        inner.finish(result);
                    } else {
                        inner.fail(WorkerCause::Protocol);
                    }
                }
                Ok(Outgoing::EndReturned(_)) => inner.fail(WorkerCause::Protocol),
                Ok(Outgoing::Finished(result)) => {
                    if !inner.shutdown_sent.get() || !inner.outstanding.borrow().is_empty() {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        let result = inner.end_returned(result);
                        inner.finish(result);
                    }
                }
                Err(error) => inner.fail_error(error.into()),
            }
        });

        if let Some(worker) = &inner.worker {
            worker.set_onmessage(Some(message.as_ref().unchecked_ref()));
        }
        *inner.message.borrow_mut() = Some(message);

        let weak = Rc::downgrade(&inner);
        let url = url.to_owned();
        let error = Closure::new(move |event: Event| {
            if let Some(inner) = weak.upgrade() {
                inner.script_failed(worker_error(&event, &url));
            }
        });

        if let Some(worker) = &inner.worker {
            worker.set_onerror(Some(error.as_ref().unchecked_ref()));
        }
        *inner.error.borrow_mut() = Some(error);

        *inner.retained.borrow_mut() = Some(inner.clone());

        Ok(Self {
            state: PhantomData,
            receipt,
            inner,
        })
    }
}

#[derive(Er)]
struct OpenWorkerErr {
    url: String,
}
fn open_worker(url: &str, options: &WorkerOptions) -> ErResult<web_sys::Worker, OpenWorkerErr> {
    browser_result(web_sys::Worker::new_with_options(url, options)).er(url)
}

#[derive(Er)]
enum ScriptError {
    #[er(format = "{0}: {1}")]
    File(String, String),
    #[er(format = "{0} (column {1}): {2}")]
    Column(String, u32, String),
    #[er(format = "{0}:{1}: {2}")]
    Line(String, u32, String),
    #[er(format = "{0}:{1}:{2}: {3}")]
    Location(String, u32, u32, String),
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

    let error = match (line, column) {
        (0, 0) => ScriptError::File(source.into(), message.into()),
        (0, column) => ScriptError::Column(source.into(), column, message.into()),
        (line, 0) => ScriptError::Line(source.into(), line, message.into()),
        (line, column) => ScriptError::Location(source.into(), line, column, message.into()),
    };
    ErBranch::from(error).er_report()
}

#[derive(Er)]
enum HandshakeErr {
    Type {
        expected: &'static str,
        actual: String,
    },
    Version {
        expected: u32,
        actual: u32,
    },
    Build {
        expected: String,
        actual: String,
    },
    Capacity {
        required: usize,
        available: usize,
    },
    ByteBudget {
        required: usize,
        available: usize,
    },
    Operations {
        expected: Vec<String>,
        actual: Vec<String>,
    },
}
fn check_type(expected: &'static str, actual: &str) -> ErResult<(), HandshakeErr> {
    if actual != expected {
        er_bail!(HandshakeErr::Type {
            expected,
            actual: actual.into()
        });
    }

    Ok(())
}

fn compatible(
    expected_options: &Options,
    expected_operations: &Operations,
    version: u32,
    options: &Options,
    operations: &Operations,
) -> ErResult<(), HandshakeErr> {
    if version != VERSION {
        er_bail!(HandshakeErr::version(VERSION, version));
    }

    if options.build != expected_options.build {
        er_bail!(HandshakeErr::build(&expected_options.build, &options.build));
    }

    if expected_options.capacity > options.capacity {
        er_bail!(HandshakeErr::capacity(
            expected_options.capacity.get(),
            options.capacity.get()
        ));
    }

    if expected_options.max_outstanding_bytes.get() > options.max_outstanding_bytes.get() {
        er_bail!(HandshakeErr::byte_budget(
            expected_options.max_outstanding_bytes.get(),
            options.max_outstanding_bytes.get()
        ));
    }

    if operations != expected_operations {
        er_bail!(HandshakeErr::Operations {
            expected: expected_operations.0.clone(),
            actual: operations.0.clone()
        });
    }

    Ok(())
}
impl HandshakeErr {
    fn failure(tree: ErTree<Self>, outcome: AktorDeliveryOutcome) -> WireFailure {
        let cause = match &tree.top {
            Self::Operations { expected, actual } => WorkerCause::Operations {
                expected: expected.clone(),
                actual: actual.clone(),
            },
            Self::Type { .. }
            | Self::Version { .. }
            | Self::Build { .. }
            | Self::Capacity { .. }
            | Self::ByteBudget { .. } => WorkerCause::Protocol,
        };

        ErBranch::from(WorkerError::new(outcome, cause))
            .er_add(tree)
            .into()
    }
}

#[cfg(feature = "wasm_browser_worker_server")]
#[wasm_bindgen::prelude::wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    fn console_warn(message: &str);
}

/// Cargo can enable the server feature on the page when the page and worker build together.
#[cfg(feature = "wasm_browser_worker_server")]
fn warn_server_feature() {
    use core::sync::atomic::{AtomicBool, Ordering};

    static WARNED: AtomicBool = AtomicBool::new(false);

    if !WARNED.swap(true, Ordering::Relaxed) {
        console_warn(
            "aktor: this module opens browser workers and also links server call bodies. \
             Build the page with wasm_browser_workers in its own Cargo invocation.",
        );
    }
}
