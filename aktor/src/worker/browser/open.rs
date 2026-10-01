use super::*;
use wasm_bindgen::JsCast;
use web_sys::{WorkerOptions, WorkerType};

impl<S, Role> Worker<S, Role> {
    pub fn new(url: &str, timeout_ms: u32) -> Result<Self, WorkerError> {
        Self::with_options(
            url,
            Options {
                timeout_ms,
                ..Options::default()
            },
        )
    }

    pub fn with_options(url: &str, options: Options) -> Result<Self, WorkerError> {
        options.validate()?;

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
            shutdown_sent: Cell::new(false),
            next: Cell::new(0),
            handles: Cell::new(1),
            closed: Cell::new(false),
            count: Arc::new(Semaphore::new(options.capacity)),
            bytes: Arc::new(Semaphore::new(options.max_outstanding_bytes)),
            options,
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
                .as_string()
                .ok_or_else(|| WorkerError::new(CallError::OutcomeUnknown, WorkerCause::Protocol))
                .and_then(|data| decode::<Outgoing>(&data));

            match output {
                Ok(Outgoing::Ready { version, options }) => {
                    if version != VERSION
                        || options.build != inner.options.build
                        || inner.options.capacity > options.capacity
                        || inner.options.max_payload_bytes > options.max_payload_bytes
                        || inner.options.max_outstanding_bytes > options.max_outstanding_bytes
                        || inner.ready.borrow().is_some()
                    {
                        inner.fail(WorkerCause::Protocol);
                    } else {
                        inner.ready.send_replace(Some(Ok(())));
                    }
                }

                Ok(Outgoing::SetupFailed(error)) => {
                    inner.ready.send_replace(Some(Err(error.clone())));
                    inner.fail(error.cause);
                }

                Ok(Outgoing::Started { id }) => {
                    if inner.active.get() == Some(id) {
                        inner.executing.set(true);
                    } else {
                        inner.fail(WorkerCause::Protocol);
                    }
                }

                Ok(Outgoing::Answer { id, mut output }) => {
                    if output
                        .as_ref()
                        .is_ok_and(|output| output.len() > inner.options.max_payload_bytes)
                    {
                        output = Err(WorkerError::new(
                            CallError::OutcomeUnknown,
                            WorkerCause::PayloadTooLarge,
                        ));
                    }

                    if inner.active.get() != Some(id) {
                        inner.fail(WorkerCause::Protocol);
                        return;
                    }

                    inner.active.set(None);
                    inner.executing.set(false);
                    let work = inner.outstanding.borrow_mut().remove(&id);
                    if let Some(work) = work {
                        if let Some(answer) = work.answer {
                            let _sent = answer.send(output);
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

        let weak = Rc::downgrade(&inner);
        let timeout = inner.options.timeout_ms;
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(timeout).await;

            if let Some(inner) = weak.upgrade()
                && inner.ready.borrow().is_none()
            {
                inner.fail(WorkerCause::Timeout);
            }
        });

        Ok(Self {
            inner,
            state: PhantomData,
        })
    }
}
