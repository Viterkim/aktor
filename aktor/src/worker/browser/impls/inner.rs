use super::super::*;

impl Inner {
    pub fn cancel_queued(&self, id: u64) {
        let work = {
            let mut queue = self.queue.borrow_mut();
            let Some(index) = queue.iter().position(|queued| *queued == id) else {
                return;
            };

            queue.remove(index);
            self.outstanding.borrow_mut().remove(&id)
        };

        drop(work);
    }

    fn wake_sessions(&self) {
        let callbacks: Vec<_> = {
            let mut sessions = self.sessions.borrow_mut();
            sessions.retain(|session| session.strong_count() != 0);
            sessions.iter().filter_map(Weak::upgrade).collect()
        };

        for callback in callbacks {
            callback();
        }
    }

    pub fn register_session(&self, callback: &Rc<dyn Fn() -> bool>) {
        let mut sessions = self.sessions.borrow_mut();

        while sessions
            .last()
            .is_some_and(|session| session.strong_count() == 0)
        {
            sessions.pop();
        }

        if sessions.len() >= self.prune_at.get() {
            sessions.retain(|session| session.strong_count() != 0);
            self.prune_at.set(sessions.len().saturating_mul(2).max(64));
        }

        sessions.push(Rc::downgrade(callback));
    }

    pub fn park<O>(&self, error: WireError) -> core::task::Poll<O> {
        if self.group.borrow().is_some() {
            core::task::Poll::Pending
        } else {
            fatal(error)
        }
    }

    pub fn lost<O>(self: &Rc<Self>, error: WireError) -> core::task::Poll<O> {
        if self.group.borrow().is_some() {
            self.reject(error);
            return core::task::Poll::Pending;
        }

        fatal(error)
    }

    pub fn reject(self: &Rc<Self>, error: WireError) {
        if self.finished.borrow().is_some() {
            fatal(error);
        }

        self.record(&error);
        self.failure.borrow_mut().get_or_insert(error);
        self.shutdown();
    }

    fn record(&self, error: &WireError) {
        let primary = self.notify(error);
        let message = match &error.cause {
            WorkerCause::Cleanup => error.diagnostics.clone(),
            _ if !primary => Some(format!("{error:#}")),
            _ => None,
        };

        if let Some(message) = message {
            self.diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(message, ()));
        }
    }

    fn notify(&self, error: &WireError) -> bool {
        if self.optional {
            return false;
        }

        let group = self.group.borrow();
        let Some((name, group)) = &*group else {
            return false;
        };
        let (phase, message) = match &error.cause {
            WorkerCause::Cleanup => ("cleanup", "worker cleanup failed".into()),
            _ => ("worker", format!("{error:#}")),
        };
        let failure = crate::group::AktorFailureReport {
            kind: None,
            actor: name.clone(),
            phase: phase.into(),
            message,
        };

        if matches!(error.cause, WorkerCause::Cleanup)
            || matches!(&*self.ready.borrow(), Some(Ok(())))
        {
            group.fail(failure)
        } else {
            group.fail_startup(failure)
        }
    }

    pub fn fail(self: &Rc<Self>, cause: WorkerCause) {
        self.fail_error(WireError::new(CallError::OutcomeUnknown, cause));
    }

    pub fn retain_failure(self: &Rc<Self>, error: WireError) {
        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        self.accepted.set(true);
        self.end_unconfirmed(error.diagnostics.as_deref());

        // A worker's retention request keeps a standalone owner alive. Its group chooses Kill or Leave.
        if self.group.borrow().is_none() {
            self.retain_unconfirmed.set(true);
        }

        *self.retained.borrow_mut() = Some(self.clone());

        if matches!(error.cause, WorkerCause::Setup)
            && self.ready.borrow().is_some()
            && self.setup_failure_pending.replace(false)
        {
            self.update_setup_failure(error.into());
        } else if self.finished.borrow().is_some() {
            self.record(&error);
        } else {
            self.fail_error(error);
        }
    }

    pub fn setup_pending(self: &Rc<Self>, report: String) {
        if self.ready.borrow().is_some()
            || *self.retirement.borrow() != AktorRetireStatus::Unconfirmed
        {
            return;
        }

        self.setup_failure_pending.set(true);
        self.retain_failure(WireError::setup(CallError::OutcomeUnknown, report));
    }

    pub fn setup_failed(self: &Rc<Self>, failure: ReadyFailure) {
        if matches!(*self.ready.borrow(), Some(Ok(()))) {
            self.fail(WorkerCause::Protocol);
            return;
        }

        self.accepted.set(true);
        self.initialize.borrow_mut().take();

        if !self.setup_failure_pending.replace(false) {
            self.fail_start(failure);
            return;
        }

        self.update_setup_failure(failure);
    }

    pub fn script_failed(self: &Rc<Self>, message: String) {
        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        let error = if matches!(*self.ready.borrow(), Some(Ok(()))) {
            if self.shutdown_sent.get() {
                self.end_unconfirmed(Some(&message));
                if self.finished.borrow().is_some() {
                    self.add_diagnostic(message);
                    return;
                }
            }
            WireError::new(CallError::OutcomeUnknown, WorkerCause::Crashed(message))
        } else {
            if self.ready.borrow().is_none() {
                self.end_unconfirmed(Some(&message));
            }
            WireError::setup(CallError::OutcomeUnknown, message)
        };

        if self.setup_failure_pending.get() {
            self.update_setup_failure(error.into());
        } else {
            self.setup_failure_pending
                .set(self.ready.borrow().is_none());
            self.fail_error(error);
        }
    }

    fn update_setup_failure(&self, mut failure: ReadyFailure) {
        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        self.add_diagnostic(format!("{:#}", failure.error));

        self.end
            .send_replace(End::Unconfirmed(failure.error.diagnostics.clone()));
        self.finished.send_replace(Some(Err(failure.error.clone())));

        failure.error.outcome = CallError::NotAdmitted;
        self.ready.send_replace(Some(Err(failure)));
    }

    fn add_diagnostic(&self, report: String) {
        let mut diagnostics = self.diagnostics.borrow_mut();
        if !diagnostics.iter().any(|entry| entry.diagnostics == report) {
            diagnostics.push(crate::AktorError::new(report, ()));
        }
    }

    pub fn setup_retired(self: &Rc<Self>) {
        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        if matches!(*self.ready.borrow(), Some(Ok(())))
            || !self.failed.get()
            || !self.outstanding.borrow().is_empty()
            || self.initialize.borrow().is_some()
            || self.setup_failure_pending.get()
        {
            self.fail(WorkerCause::Protocol);
            return;
        }

        self.finish(Ok(()));
    }

    pub fn fail_start(self: &Rc<Self>, failure: ReadyFailure) {
        if !matches!(&*self.ready.borrow(), Some(Ok(()))) {
            self.end_unconfirmed(failure.error.diagnostics.as_deref());
        }

        // A JavaScript catch can report the same error again with only its text.
        if self.ready.borrow().is_some() {
            self.fail(WorkerCause::Protocol);
            return;
        }

        let error = failure.error.clone();
        self.ready.send_replace(Some(Err(failure)));
        self.fail_error(error);
    }

    pub fn fail_error(self: &Rc<Self>, error: WireError) {
        self.close_with_error(error, true);
    }

    fn close_with_error(self: &Rc<Self>, error: WireError, report: bool) {
        if self.finished.borrow().is_some() || self.failed.replace(true) {
            return;
        }

        if self.retain_unconfirmed.get() && self.accepted.get() {
            *self.retained.borrow_mut() = Some(self.clone());
        }

        if report {
            self.record(&error);
        }
        self.closed.set(true);
        self.count.close();
        self.bytes.close();

        if self.ready.borrow().is_none() {
            let mut startup = error.clone();
            startup.outcome = CallError::NotAdmitted;
            self.ready.send_replace(Some(Err(startup.into())));
        }

        let active = self.active.get();

        for (id, mut work) in self.outstanding.replace(HashMap::new()) {
            let uncertain = active == Some(id);
            let outcome = if uncertain {
                CallError::OutcomeUnknown
            } else {
                CallError::Discarded
            };

            if let Some(service) = work.service.take() {
                service.answer(Err(WireError {
                    outcome,
                    ..error.clone()
                }));
            }

            if let Some(answer) = work.answer.take() {
                answer(Err(WireError {
                    outcome,
                    ..error.clone()
                }));
            }

            if uncertain {
                self.uncertain.borrow_mut().push(work);
            }
        }

        self.queue.borrow_mut().clear();
        self.active.set(None);
        self.executing.set(false);
        self.publish(Err(error));
        self.wake_sessions();

        self.continue_shutdown(false);
    }

    pub fn finish(self: &Rc<Self>, result: Result<(), WireError>) {
        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        if !self.join_ready() {
            *self.pending_finish.borrow_mut() = Some(result);
            self.continue_shutdown(false);
            return;
        }

        if let Err(error) = &result {
            self.record(error);
        }

        self.kill_worker(AktorRetireStatus::Confirmed);
        self.publish(result);
        self.uncertain.borrow_mut().clear();
        self.wake_sessions();
        self.buffers.clear();
        self.retained.borrow_mut().take();
    }

    fn publish(&self, result: Result<(), WireError>) {
        let first = self.failure.borrow_mut().take();
        if self.finished.borrow().is_none() {
            self.finished.send_replace(Some(first.map_or(result, Err)));
        }
    }

    pub fn pump(self: &Rc<Self>) {
        if self.failed.get()
            || self.pump_scheduled.get()
            || self.active.get().is_some()
            || !matches!(*self.ready.borrow(), Some(Ok(())))
            || self.finished.borrow().is_some()
        {
            return;
        }

        for _ in 0..64 {
            let next = self.queue.borrow_mut().pop_front();
            let Some(id) = next else {
                if self.closed.get()
                    && self.join_ready()
                    && !self.shutdown_sent.replace(true)
                    && let Err(error) = post(&self.worker, &self.buffers, &Incoming::Shutdown)
                {
                    self.fail_error(error);
                }

                return;
            };

            self.active.set(Some(id));

            let work = self.outstanding.borrow_mut().remove(&id);
            let Some(mut work) = work else {
                self.fail(WorkerCause::Protocol);
                return;
            };
            let input = if let Some(service) = &work.service {
                match service.take() {
                    Ok(Some(input)) => Some(Incoming::Call {
                        id,
                        operation: service.operation().into(),
                        input,
                    }),
                    Ok(None) => None,
                    Err(error) => {
                        self.active.set(None);
                        if let Some(error) = service.answer(Err(error)) {
                            self.reject(error);
                        }

                        continue;
                    }
                }
            } else {
                work.input.take()
            };

            if self.failed.get() {
                return;
            }

            if let Some(input) = input {
                self.outstanding.borrow_mut().insert(id, work);

                if let Err(mut error) = post(&self.worker, &self.buffers, &input) {
                    self.active.set(None);
                    error.outcome = CallError::Discarded;
                    self.fail_error(error);
                }

                return;
            }

            self.active.set(None);
            drop(work);
        }

        self.pump_scheduled.set(true);

        let inner = self.clone();

        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(0).await;
            inner.pump_scheduled.set(false);
            inner.pump();
        });
    }

    pub fn shutdown(self: &Rc<Self>) {
        if !self.accepted.get() && self.ready.borrow().is_none() {
            let error = WireError::new(CallError::NotAdmitted, WorkerCause::Closed);
            self.initialize.borrow_mut().take();
            self.end_unconfirmed(None);
            self.ready
                .send_replace(Some(Err(ReadyFailure::not_run(error.clone()))));
            *self.pending_finish.borrow_mut() = Some(Ok(()));
            self.close_with_error(error, false);
            return;
        }

        if self.group.borrow().is_none() && self.ready.borrow().is_none() {
            self.fail_error(WireError::new(CallError::NotAdmitted, WorkerCause::Closed));
            return;
        }

        if self.closed.replace(true) {
            return;
        }

        self.count.close();
        self.bytes.close();
        self.wake_sessions();

        // Keep the worker around until accepted work and cleanup finish.
        *self.retained.borrow_mut() = Some(self.clone());

        self.continue_shutdown(false);
    }

    fn end_unconfirmed(&self, report: Option<&str>) {
        self.end.send_if_modified(|end| match end {
            End::Returned(_) => false,
            End::Unconfirmed(_) if report.is_none() => false,
            End::Unconfirmed(Some(previous)) if let Some(report) = report => {
                if previous.ends_with(report) {
                    return false;
                }
                previous.push('\n');
                previous.push_str(report);
                true
            }
            End::Waiting | End::Unconfirmed(_) => {
                *end = End::Unconfirmed(report.map(str::to_owned));
                true
            }
        });
    }

    fn kill_worker(&self, status: AktorRetireStatus) {
        if *self.retirement.borrow() == AktorRetireStatus::Unconfirmed {
            self.end_unconfirmed(None);
            self.worker.terminate();
            self.retirement.send_replace(status);
        }
    }

    fn join_ready(&self) -> bool {
        match &*self.shutdown_join.borrow() {
            ShutdownJoin::Ready => true,
            ShutdownJoin::Waiting(_) | ShutdownJoin::Joining { .. } => false,
        }
    }

    pub fn continue_shutdown(self: &Rc<Self>, force: bool) {
        let gate = self.shutdown_join.replace(ShutdownJoin::Ready);

        match gate {
            ShutdownJoin::Ready => self.finish_shutdown(force),
            ShutdownJoin::Joining { force: requested } => {
                *self.shutdown_join.borrow_mut() = ShutdownJoin::Joining {
                    force: requested || force,
                };
            }
            ShutdownJoin::Waiting(joining) => {
                *self.shutdown_join.borrow_mut() = ShutdownJoin::Joining { force };
                let inner = self.clone();

                wasm_bindgen_futures::spawn_local(async move {
                    let result = joining.await;
                    let gate = inner.shutdown_join.replace(ShutdownJoin::Ready);
                    let force = match gate {
                        ShutdownJoin::Joining { force } => force,
                        ShutdownJoin::Ready | ShutdownJoin::Waiting(_) => false,
                    };

                    if let Err(error) = result {
                        let error =
                            WireError::cleanup(CallError::OutcomeUnknown, error.diagnostics);
                        inner.record(&error);
                        inner.failure.borrow_mut().get_or_insert(error);
                    }

                    inner.finish_shutdown(force);
                });
            }
        }
    }

    fn finish_shutdown(self: &Rc<Self>, force: bool) {
        let finished = self.pending_finish.borrow_mut().take();

        if let Some(result) = finished {
            self.finish(result);
            return;
        }

        if *self.retirement.borrow() != AktorRetireStatus::Unconfirmed {
            return;
        }

        if force && !self.retain_unconfirmed.get() {
            self.kill_worker(AktorRetireStatus::Terminated);
            self.uncertain.borrow_mut().clear();
            self.buffers.clear();
            self.retained.borrow_mut().take();

            return;
        }

        if self.failed.get() {
            if !self.accepted.get() {
                self.kill_worker(AktorRetireStatus::Terminated);
                self.buffers.clear();
                self.retained.borrow_mut().take();
            } else if !(force && self.shutdown_sent.get()) {
                self.shutdown_sent.set(true);
                let _result = post(&self.worker, &self.buffers, &Incoming::Shutdown);

                if self.group.borrow().is_none()
                    && !self.retain_unconfirmed.get()
                    && !self.waiting_to_terminate.replace(true)
                {
                    let worker = Rc::downgrade(self);
                    let mut retirement = self.retirement.subscribe();

                    wasm_bindgen_futures::spawn_local(async move {
                        let grace = core::time::Duration::from_secs(5);
                        tokio::select! {
                            _ = retirement.wait_for(|status| *status != AktorRetireStatus::Unconfirmed) => {},
                            _ = crate::timeout::browser_sleep(grace) => {
                                if let Some(worker) = worker.upgrade() {
                                    worker.continue_shutdown(true);
                                }
                            }
                        }
                    });
                }
            }

            return;
        }

        let inner = self.clone();

        wasm_bindgen_futures::spawn_local(async move {
            match observe(inner.ready.subscribe()).await {
                Ok(()) => inner.pump(),
                Err(failure) => inner.fail_error(failure.error),
            }
        });
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.worker.set_onmessage(None);
        self.worker.set_onerror(None);
        self.kill_worker(AktorRetireStatus::Terminated);
    }
}

pub fn post(
    worker: &web_sys::Worker,
    buffers: &Buffers,
    input: &Incoming,
) -> Result<(), WireError> {
    let reusable = matches!(input, Incoming::Call { .. });
    let input = postcard::to_allocvec(input).map_err(|error| {
        WireError::new(
            CallError::NotAdmitted,
            WorkerCause::Codec(error.to_string()),
        )
    })?;

    let buffer = if reusable {
        buffers.write(&input).map_err(|error| {
            WireError::new(
                CallError::NotAdmitted,
                WorkerCause::Transport(format!("{error:?}")),
            )
        })?
    } else {
        js_sys::Uint8Array::from(input.as_slice()).buffer()
    };
    let sent = if input.len() < super::super::buffer::MIN_REUSE {
        worker.post_message(&buffer)
    } else {
        let transfers = js_sys::Array::new();

        transfers.push(&buffer);
        worker.post_message_with_transfer(&buffer, &transfers)
    };

    sent.map_err(|error| {
        WireError::new(
            CallError::NotAdmitted,
            WorkerCause::Transport(format!("{error:?}")),
        )
    })
}
