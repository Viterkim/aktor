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

    pub(in crate::worker::browser) fn recorded_failure(
        &self,
        outcome: AktorDeliveryOutcome,
    ) -> Option<WireFailure> {
        self.finished
            .borrow()
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(|error| error.with_outcome(outcome))
            .or_else(|| {
                self.failure
                    .borrow()
                    .as_ref()
                    .map(|error| error.with_outcome(outcome))
            })
    }

    pub(crate) fn reject_recorded(self: &Rc<Self>, error: WireFailure) {
        self.failure.borrow_mut().get_or_insert(error);
        self.shutdown();
    }

    fn record(&self, error: &mut WireFailure) {
        use crate::AktorWorkerPhase::*;
        let phase = match error.error.cause {
            WorkerCause::Setup => Setup,
            WorkerCause::Cleanup => Cleanup,
            _ => Transport,
        };
        let operation = self
            .active
            .get()
            .filter(|_| self.executing.get())
            .and_then(|id| {
                self.outstanding
                    .borrow()
                    .get(&id)
                    .map(|work| work.operation)
            });
        let phase = match (phase, operation) {
            (Transport, Some(operation)) => operation.phase(),
            (phase, _) => crate::AktorPhase::Worker(phase),
        };
        let id = self.record_at(phase, &error.report);
        error.error.failure_id = Some(id);
    }

    pub(in crate::worker::browser) fn record_at(
        &self,
        phase: crate::AktorPhase,
        report: &str,
    ) -> crate::FailureId {
        self.record_occurrence(phase, report, !self.optional)
    }

    pub(in crate::worker::browser) fn record_occurrence(
        &self,
        phase: crate::AktorPhase,
        report: &str,
        stop: bool,
    ) -> crate::FailureId {
        let startup = !matches!(
            phase,
            crate::AktorPhase::Worker(
                crate::AktorWorkerPhase::Cleanup | crate::AktorWorkerPhase::ExternalJoin
            )
        ) && !matches!(&*self.ready.borrow(), Some(Ok(())));
        let failure = crate::AktorFailureInput::actor(
            self.name.clone(),
            crate::AktorExecution::BrowserWebWorker,
            phase,
            report.into(),
        );
        if !stop {
            self.group.observe_failure(failure)
        } else if startup {
            self.group.fail_startup(failure)
        } else {
            self.group.fail(failure)
        }
    }

    pub fn fail(self: &Rc<Self>, cause: WorkerCause) {
        self.fail_error(WireFailure::new(
            AktorDeliveryOutcome::OutcomeUnknown,
            cause,
        ));
    }

    pub(crate) fn init_evidence(&self, evidence: crate::AktorInitEvidence) {
        self.group.init_evidence(evidence);
    }

    pub fn retain_failure(self: &Rc<Self>, mut error: WireFailure) {
        if self.retirement.borrow().is_some() {
            return;
        }

        if !self.accepted.replace(true) {
            self.pending_finish.borrow_mut().take();
        }
        if matches!(error.error.cause, WorkerCause::Setup) {
            self.init_entered.set(true);
            self.init_evidence(crate::AktorInitEvidence::Entered);
        }

        *self.retained.borrow_mut() = Some(self.clone());

        if matches!(error.error.cause, WorkerCause::Cleanup) {
            let id = self.record_at(
                crate::AktorPhase::Worker(crate::AktorWorkerPhase::Cleanup),
                &error.report,
            );
            error.error.failure_id = Some(id);
            self.cleanup_pending.set(Some(id));
            self.end_unconfirmed(&error);
            self.close_with_error(error, false);
            return;
        }

        if matches!(error.error.cause, WorkerCause::Setup)
            && self.ready.borrow().is_some()
            && self.setup_failure_pending.replace(false)
        {
            self.update_setup_failure(ReadyFailure::entered(error));
        } else if self.finished.borrow().is_some() {
            self.record(&mut error);
        } else {
            self.fail_error(error);
        }
    }

    pub fn setup_pending(self: &Rc<Self>, failure: WireFailure) {
        if self.ready.borrow().is_some() || self.retirement.borrow().is_some() {
            return;
        }

        self.init_entered.set(true);
        self.setup_failure_pending.set(true);
        self.retain_failure(failure);
    }

    pub fn setup_failed(self: &Rc<Self>, failure: ReadyFailure) {
        if matches!(*self.ready.borrow(), Some(Ok(()))) {
            self.fail(WorkerCause::Protocol);
            return;
        }

        self.accepted.set(true);
        self.initialize.borrow_mut().take();

        if self.setup_failure_pending.replace(false) {
            self.update_setup_failure(failure);
        } else {
            self.fail_start(failure);
        }
    }

    pub fn script_failed(self: &Rc<Self>, message: String) {
        if self.retirement.borrow().is_some() {
            return;
        }

        if self.ready.borrow().is_none() && !self.accepted.get() {
            self.fail_start(ReadyFailure::not_run(WireFailure::setup(
                AktorDeliveryOutcome::NotAdmitted,
                message,
            )));
            return;
        }

        let mut error = if self.ready.borrow().is_some() {
            WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Crashed)
                .er_save("worker", message)
                .into()
        } else {
            WireFailure::setup(AktorDeliveryOutcome::OutcomeUnknown, message)
        };
        self.record(&mut error);
        self.end_unconfirmed(&error);
        self.close_with_error(error, false);
    }

    fn update_setup_failure(&self, mut failure: ReadyFailure) {
        if self.retirement.borrow().is_some() {
            return;
        }

        failure.error.error.failure_id = Some(self.record_at(
            crate::AktorPhase::Worker(crate::AktorWorkerPhase::Cleanup),
            &failure.error.report,
        ));
        if failure.evidence == crate::AktorInitEvidence::Unknown && self.init_entered.get() {
            failure.evidence = crate::AktorInitEvidence::Entered;
        }
        self.init_evidence(failure.evidence);
        self.end
            .send_replace(End::Unconfirmed(failure.error.clone()));
        self.finished.send_replace(Some(Err(failure.error.clone())));

        failure.error.error.outcome = AktorDeliveryOutcome::NotAdmitted;
        self.ready.send_replace(Some(Err(failure)));
    }

    pub fn setup_retired(self: &Rc<Self>) {
        if self.retirement.borrow().is_some() {
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

    pub fn fail_start(self: &Rc<Self>, mut failure: ReadyFailure) {
        // A JavaScript catch can report the same error again with only its text.
        if self.ready.borrow().is_some() {
            self.fail(WorkerCause::Protocol);
            return;
        }

        self.init_evidence(failure.evidence);
        self.record(&mut failure.error);
        self.end_unconfirmed(&failure.error);
        let error = failure.error.clone();
        self.ready.send_replace(Some(Err(failure)));
        self.close_with_error(error, false);
    }

    pub fn fail_error(self: &Rc<Self>, error: WireFailure) {
        self.close_with_error(error, true);
    }

    pub(in crate::worker::browser) fn close_with_error(
        self: &Rc<Self>,
        mut error: WireFailure,
        report: bool,
    ) {
        if self.finished.borrow().is_some() || self.failed.replace(true) {
            if report {
                self.record(&mut error);
            }
            return;
        }

        if self.retain_unconfirmed.get() && self.accepted.get() {
            *self.retained.borrow_mut() = Some(self.clone());
        }

        if report {
            self.record(&mut error);
        }
        self.closed.set(true);
        self.count.close();
        self.bytes.close();

        if self.ready.borrow().is_none() {
            if matches!(error.error.cause, WorkerCause::Setup) {
                self.end_unconfirmed(&error);
            }
            let mut startup = error.clone();
            startup.error.outcome = AktorDeliveryOutcome::NotAdmitted;
            let failure = if self.init_entered.get() {
                ReadyFailure::entered(startup)
            } else {
                startup.into()
            };
            self.ready.send_replace(Some(Err(failure)));
        }

        let active = self.active.get();

        for (id, mut work) in self.outstanding.replace(HashMap::new()) {
            let uncertain = active == Some(id);
            let outcome = if uncertain {
                AktorDeliveryOutcome::OutcomeUnknown
            } else {
                AktorDeliveryOutcome::Discarded
            };

            if let Some(service) = work.service.take() {
                service.answer(Err(error.with_outcome(outcome)), &core::convert::identity);
            }

            if let Some(answer) = work.answer.take() {
                answer(Err(error.with_outcome(outcome)), &core::convert::identity);
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

    pub fn end_returned(
        self: &Rc<Self>,
        mut result: Result<(), WireFailure>,
    ) -> Result<(), WireFailure> {
        if let Err(error) = &mut result {
            self.record(error);
            if let Some(notice) = self.cleanup_pending.take()
                && self
                    .finished
                    .borrow()
                    .as_ref()
                    .and_then(|result| result.as_ref().err())
                    .and_then(|failure| failure.error.failure_id)
                    == Some(notice)
            {
                self.finished.send_replace(Some(Err(error.clone())));
            }
        }

        self.end.send_replace(End::Returned(result.clone()));
        result
    }

    pub(in crate::worker::browser) fn finish(self: &Rc<Self>, result: Result<(), WireFailure>) {
        if self.retirement.borrow().is_some() {
            return;
        }

        if !self.join_ready() {
            *self.pending_finish.borrow_mut() = Some(result);
            self.continue_shutdown(false);
            return;
        }

        self.retire_worker(AktorRetirement::Released);
        self.publish(result);
        self.uncertain.borrow_mut().clear();
        self.wake_sessions();
        self.buffers.clear();
        self.retained.borrow_mut().take();
    }

    fn publish(&self, result: Result<(), WireFailure>) {
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
                    && let Err(error) =
                        post(self.worker.as_ref(), &self.buffers, &Incoming::Shutdown)
                {
                    self.fail_error(PostErr::failure(error, AktorDeliveryOutcome::NotAdmitted));
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
                match service.take().er_with(|err| {
                    WorkerError::new(AktorDeliveryOutcome::NotAdmitted, err.cause.clone())
                }) {
                    Ok(Some(input)) => Some(Incoming::Call {
                        id,
                        operation: service.operation().into(),
                        input,
                    }),
                    Ok(None) => None,
                    Err(error) => {
                        self.active.set(None);
                        let record = |mut error: WireFailure| {
                            error.error.failure_id =
                                Some(self.record_at(work.operation.phase(), &error.report));
                            error
                        };
                        if let Some(error) = service.answer(Err(error.into()), &record) {
                            self.reject_recorded(error);
                        }

                        continue;
                    }
                }
            } else {
                work.input.take()
            };

            if self.failed.get() {
                // A user codec may close the worker while it has this service out of the queue.
                if let Some(service) = work.service {
                    let error = self
                        .finished
                        .borrow()
                        .as_ref()
                        .and_then(|result| result.as_ref().err().cloned());
                    let cause = error.unwrap_or_else(|| {
                        WireFailure::new(AktorDeliveryOutcome::Discarded, WorkerCause::Closed)
                    });
                    service.answer(
                        Err(cause.with_outcome(AktorDeliveryOutcome::Discarded)),
                        &core::convert::identity,
                    );
                }
                return;
            }

            if let Some(input) = input {
                self.outstanding.borrow_mut().insert(id, work);

                if let Err(error) = post(self.worker.as_ref(), &self.buffers, &input) {
                    self.active.set(None);
                    self.fail_error(PostErr::failure(error, AktorDeliveryOutcome::Discarded));
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
            let error = WireFailure::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Closed);
            self.initialize.borrow_mut().take();
            self.end_unconfirmed(&error);
            self.ready
                .send_replace(Some(Err(ReadyFailure::not_run(error.clone()))));
            *self.pending_finish.borrow_mut() = Some(Ok(()));
            self.close_with_error(error, false);
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

    fn end_unconfirmed(&self, cause: &WireFailure) {
        self.end.send_if_modified(|end| {
            if matches!(end, End::Returned(_)) {
                return false;
            }
            *end = End::Unconfirmed(cause.clone());
            true
        });
    }

    fn retire_worker(&self, status: AktorRetirement) {
        if self.retirement.borrow().is_none() {
            if matches!(*self.end.borrow(), End::Waiting) {
                let cause = self
                    .recorded_failure(AktorDeliveryOutcome::OutcomeUnknown)
                    .unwrap_or_else(|| {
                        WireFailure::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Closed)
                    });
                self.end_unconfirmed(&cause);
            }
            if (status == AktorRetirement::Terminated || !self.accepted.get())
                && let Some(worker) = &self.worker
            {
                worker.terminate();
            }
            self.retirement.send_replace(Some(status));
        }
    }

    fn join_ready(&self) -> bool {
        match &*self.shutdown_join.borrow() {
            ShutdownJoin::Ready => true,
            ShutdownJoin::Unconfirmed | ShutdownJoin::Waiting(_) | ShutdownJoin::Joining { .. } => {
                false
            }
        }
    }

    pub fn continue_shutdown(self: &Rc<Self>, force: bool) {
        let gate = self.shutdown_join.replace(ShutdownJoin::Ready);

        match gate {
            ShutdownJoin::Ready => self.finish_shutdown(force),
            ShutdownJoin::Unconfirmed => {
                *self.shutdown_join.borrow_mut() = ShutdownJoin::Unconfirmed
            }
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
                    let force = matches!(gate, ShutdownJoin::Joining { force: true });
                    let unconfirmed = matches!(result, WorkerJoinOutcome::Unconfirmed(_));
                    let report = match result {
                        WorkerJoinOutcome::Stopped(Ok(())) => None,
                        WorkerJoinOutcome::Stopped(Err((_, report)))
                        | WorkerJoinOutcome::Unconfirmed(report) => Some(report),
                    };

                    if let Some(report) = report {
                        let mut error =
                            WireFailure::cleanup(AktorDeliveryOutcome::OutcomeUnknown, report);
                        error.error.failure_id = Some(inner.record_at(
                            crate::AktorPhase::Worker(crate::AktorWorkerPhase::ExternalJoin),
                            &error.report,
                        ));
                        if unconfirmed {
                            *inner.shutdown_join.borrow_mut() = ShutdownJoin::Unconfirmed;
                            if inner.worker.is_some() {
                                *inner.retained.borrow_mut() = Some(inner.clone());
                            }
                            inner.end_unconfirmed(&error);
                            inner.close_with_error(error, false);
                            inner
                                .retirement
                                .send_replace(Some(AktorRetirement::StillRunning));
                            return;
                        }

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

        if self.retirement.borrow().is_some() {
            return;
        }

        if force && !self.retain_unconfirmed.get() {
            self.retire_worker(AktorRetirement::Terminated);
            self.uncertain.borrow_mut().clear();
            self.buffers.clear();
            self.retained.borrow_mut().take();

            return;
        }

        if self.failed.get() {
            if !self.accepted.get() {
                self.retire_worker(AktorRetirement::Terminated);
                self.buffers.clear();
                self.retained.borrow_mut().take();
            } else if !(force && self.shutdown_sent.get()) {
                self.shutdown_sent.set(true);
                if let Err(error) = post(self.worker.as_ref(), &self.buffers, &Incoming::Shutdown) {
                    self.record(&mut PostErr::failure(
                        error,
                        AktorDeliveryOutcome::NotAdmitted,
                    ));
                }
            }

            return;
        }

        let inner = self.clone();

        wasm_bindgen_futures::spawn_local(async move {
            if observe(inner.ready.subscribe()).await.is_ok() {
                inner.pump();
            }
        });
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.set_onmessage(None);
            worker.set_onerror(None);
        }
        self.retire_worker(AktorRetirement::Terminated);
    }
}

#[derive(Er)]
pub(in crate::worker::browser) struct PostErr {
    cause: WorkerCause,
}
impl PostErr {
    pub(in crate::worker::browser) fn failure(
        tree: ErTree<Self>,
        outcome: AktorDeliveryOutcome,
    ) -> WireFailure {
        let top = WorkerError::new(outcome, tree.top.cause.clone());
        er_new!(top, tree).into()
    }
}
pub(in crate::worker::browser) fn post(
    worker: Option<&web_sys::Worker>,
    buffers: &Buffers,
    input: &Incoming,
) -> ErResult<(), PostErr> {
    let worker = worker.er(|| PostErr::new(WorkerCause::Closed))?;

    let reusable = matches!(input, Incoming::Call { .. }).then_some(buffers);
    let buffer = buffer::prepare(input, reusable).er_with(|err| PostErr::new(err.cause()))?;
    let sent = if (buffer.byte_length() as usize) < buffer::MIN_REUSE {
        worker.post_message(&buffer)
    } else {
        let transfers = js_sys::Array::new();

        transfers.push(&buffer);
        worker.post_message_with_transfer(&buffer, &transfers)
    };

    browser_result(sent).er(|| PostErr::new(WorkerCause::Transport))
}
