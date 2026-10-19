use super::super::*;

impl Inner {
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
        if self.finished.borrow().is_some()
            || self
                .group
                .borrow()
                .as_ref()
                .is_some_and(|(_, group)| group.is_finished())
        {
            fatal(error);
        }

        self.record(&error);
        self.failure.borrow_mut().get_or_insert(error);
        self.shutdown();
    }

    fn record(&self, error: &WireError) {
        let primary = self.notify(error);
        let message = match &error.cause {
            WorkerCause::Cleanup => error
                .diagnostics
                .as_ref()
                .map(|diagnostics| diagnostics.report.clone()),
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
        let group = self.group.borrow();
        let Some((name, group)) = &*group else {
            return false;
        };
        let (phase, message) = match &error.cause {
            WorkerCause::Cleanup => ("cleanup", "worker cleanup failed".into()),
            _ => ("worker", format!("{error:#}")),
        };
        let failure = crate::group::ActorFailure {
            kind: None,
            actor: name.clone(),
            phase: phase.into(),
            message,
        };

        if matches!(&*self.ready.borrow(), Some(Ok(()))) {
            group.fail(failure)
        } else {
            group.fail_startup(failure)
        }
    }

    pub fn fail(&self, cause: WorkerCause) {
        self.fail_error(WireError::new(CallError::OutcomeUnknown, cause));
    }

    pub fn fail_error(&self, error: WireError) {
        if self.finished.borrow().is_some() || self.failed.replace(true) {
            return;
        }

        self.record(&error);
        self.closed.set(true);
        self.count.close();
        self.bytes.close();

        if self.ready.borrow().is_none() {
            let mut startup = error.clone();
            startup.outcome = CallError::NotAdmitted;
            self.ready.send_replace(Some(Err(startup)));
        }

        for (_, work) in self.outstanding.replace(HashMap::new()) {
            let outcome = if work.input.is_some() {
                CallError::Discarded
            } else {
                CallError::OutcomeUnknown
            };

            if let Some(service) = work.service {
                service.answer(Err(WireError {
                    outcome,
                    ..error.clone()
                }));
            }

            if let Some(answer) = work.answer {
                answer(Err(WireError {
                    outcome,
                    ..error.clone()
                }));
            }
        }

        self.queue.borrow_mut().clear();
        self.active.set(None);
        self.executing.set(false);
        self.failure.borrow_mut().take();
        self.complete(Err(error));
    }

    pub fn finish(&self, result: Result<(), WireError>) {
        if let Err(error) = &result {
            self.record(error);
        }

        let failure = self.failure.borrow_mut().take();
        let result = result.and(match failure {
            Some(error) => Err(error),
            None => Ok(()),
        });

        self.complete(result);
    }

    fn complete(&self, result: Result<(), WireError>) {
        if self.finished.borrow().is_none() {
            self.finished.send_replace(Some(result));
        }

        self.wake_sessions();
        self.buffers.clear();
        self.worker.terminate();
        self.retained.borrow_mut().take();
    }

    pub fn pump(self: &Rc<Self>) {
        if self.pump_scheduled.get()
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

                if let Err(error) = post(&self.worker, &self.buffers, &input) {
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

        let inner = self.clone();

        wasm_bindgen_futures::spawn_local(async move {
            match observe(inner.ready.subscribe()).await {
                Ok(()) => inner.pump(),
                Err(error) => inner.fail_error(error),
            }
        });
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.worker.set_onmessage(None);
        self.worker.set_onerror(None);
        self.worker.terminate();
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
                WorkerCause::Codec(format!("{error:?}")),
            )
        })?
    } else {
        js_sys::Uint8Array::from(input.as_slice()).buffer()
    };
    let transfers = js_sys::Array::new();

    transfers.push(&buffer);
    worker
        .post_message_with_transfer(&buffer, &transfers)
        .map_err(|error| {
            WireError::new(
                CallError::NotAdmitted,
                WorkerCause::Codec(format!("{error:?}")),
            )
        })
}
