use super::super::*;

impl<S> Inner<S> {
    pub fn has_work(&self) -> bool {
        !self.queue.borrow().is_empty() || !self.services.borrow().is_empty()
    }

    pub fn cancel_service(&self, id: *const ()) {
        let message = {
            let mut services = self.services.borrow_mut();
            let index = services
                .iter()
                .position(|message| message.job.service_id() == Some(id));
            index.and_then(|index| services.remove(index))
        };

        drop(message);
    }

    pub fn close(&self) {
        if self.open.replace(false) {
            if let Some(cause) = self.completion.lost.get() {
                let _ = self.completion.refused.set(cause.clone());
            }
            let owner = || self.wake_owner();
            let space = || self.space.notify();
            let closed = || self.closed.notify();
            self.notify(&[&owner, &space, &closed]);
        }
    }

    fn notify(&self, actions: &[&dyn Fn()]) {
        let sessions: &dyn Fn() = &|| self.wake_sessions();
        let actions = actions.iter().copied().chain(core::iter::once(sessions));

        #[cfg(feature = "std")]
        {
            let primary = crate::local::panic::capture_each(
                actions,
                |action| action(),
                |payload| self.save_panic(crate::AktorPhase::Drop, payload),
            );

            if let Some(payload) = primary {
                // Drop impls notify too, a second panic while unwinding would abort.
                if std::thread::panicking() {
                    self.save_panic(crate::AktorPhase::Drop, &payload);
                    self.dispose_panic(payload);
                } else {
                    std::panic::resume_unwind(payload);
                }
            }
        }
        #[cfg(not(feature = "std"))]
        for action in actions {
            action();
        }
    }

    fn wake_sessions(&self) {
        let callbacks: alloc::vec::Vec<_> = {
            let mut sessions = self.sessions.borrow_mut();
            sessions.retain(|session| session.strong_count() != 0);
            sessions.iter().filter_map(Weak::upgrade).collect()
        };

        #[cfg(feature = "std")]
        if let Some(payload) = crate::local::panic::capture_each(
            callbacks,
            |callback| {
                callback();
            },
            |payload| self.save_panic(crate::AktorPhase::Drop, payload),
        ) {
            // Drop impls notify too, a second panic while unwinding would abort.
            if std::thread::panicking() {
                self.save_panic(crate::AktorPhase::Drop, &payload);
                self.dispose_panic(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
        #[cfg(not(feature = "std"))]
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

    pub(super) fn init_evidence(&self, init: crate::AktorInitEvidence) {
        self.completion.init.set(init);
        if let Some((_, _, group)) = self.group.borrow().as_ref() {
            group.init_evidence(init);
        }
    }

    pub fn service(&self, message: Message<S>) {
        self.services.borrow_mut().push_back(message);
        self.wake_owner();
    }

    pub fn enqueue(&self, message: Message<S>) -> Result<(), Message<S>> {
        let mut queue = self.queue.borrow_mut();

        if queue.len() == self.capacity {
            return Err(message);
        }

        queue.push_back(message);
        drop(queue);
        self.wake_owner();
        Ok(())
    }

    fn wake_owner(&self) {
        let wake = self.owner_waker.take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    pub fn record(&self, phase: crate::AktorPhase, error: String) -> Option<crate::FailureId> {
        let Some((name, kind, group)) = self.group.borrow().clone() else {
            let id = crate::FailureId::next();
            self.completion
                .returned
                .borrow_mut()
                .push(crate::Delivery::new(id, error));
            return Some(id);
        };

        let startup = self.completion.ready.borrow().is_none();
        let failure = crate::AktorFailureInput::actor(name.as_ref().into(), kind, phase, error);
        Some(if startup {
            group.fail_startup(failure)
        } else {
            group.fail(failure)
        })
    }

    pub(in crate::local) fn remember(&self, id: Option<crate::FailureId>) {
        if let Some(id) = id
            && let Some((_, _, group)) = &*self.group.borrow()
            && !self
                .completion
                .returned
                .borrow()
                .iter()
                .any(|delivery| delivery.0 == id)
            && let Some(delivery) = group.delivery(id)
        {
            self.completion.returned.borrow_mut().push(delivery);
        }
    }

    pub(in crate::local) fn lost(&self, id: Option<crate::FailureId>) {
        self.remember(id);
        if let Some(delivery) = self
            .completion
            .returned
            .borrow()
            .iter()
            .find(|d| Some(d.0) == id)
        {
            let _ = self.completion.lost.set(delivery.clone());
        }
    }

    pub(in crate::local) fn cancelled(&self) {
        if self.completion.lost.get().is_none() {
            let (error, report) = OwnerError::<()>::Cancelled.observed();
            self.lost(self.fail(&error, &report));
        }
    }

    pub fn fail<E>(&self, error: &OwnerError<E>, report: &str) -> Option<crate::FailureId> {
        use crate::AktorPhase;
        let phase = match error {
            OwnerError::SetupDiagnostic { id } => return Some(*id),
            OwnerError::CleanupFailure { id: Some(id) } => return Some(*id),
            OwnerError::SetupPanic => return None,
            OwnerError::Setup(_) => AktorPhase::Start,
            OwnerError::Cleanup(_)
            | OwnerError::CleanupFailure { .. }
            | OwnerError::CleanupUnconfirmed => AktorPhase::End,
            OwnerError::Runner => AktorPhase::Runner,
            OwnerError::Cancelled => {
                if let Some((_, _, group)) = self.group.borrow().as_ref()
                    && group.is_stopping()
                {
                    return group.driver_failure();
                }
                AktorPhase::Runner
            }
        };

        self.record(phase, report.into())
    }

    pub fn discard_queued(&self) {
        self.close();
        let queued = core::mem::take(&mut *self.queue.borrow_mut());
        let services = core::mem::take(&mut *self.services.borrow_mut());
        #[cfg(feature = "std")]
        let primary = crate::local::panic::discard(queued.into_iter().chain(services), |payload| {
            self.save_panic(crate::AktorPhase::Drop, payload);
        });
        #[cfg(not(feature = "std"))]
        for message in queued.into_iter().chain(services) {
            drop(message);
        }
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }
}

impl<S, E> OwnerState<S, E> {
    pub fn finish(&self, result: Result<(), Observed<E>>) {
        #[cfg(feature = "std")]
        let mut primary =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.close())).err();
        #[cfg(not(feature = "std"))]
        self.close();

        if self.completion.result.borrow().is_some() {
            #[cfg(feature = "std")]
            if let Some(payload) = primary {
                std::panic::resume_unwind(payload);
            }

            return;
        }

        let mut completion = CompletionGuard {
            inner: self,
            failure: result.as_ref().err().cloned(),
            armed: true,
        };

        #[cfg(feature = "std")]
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.discard_queued()))
        {
            match primary {
                Some(_) => {
                    self.report_panic(crate::AktorPhase::Drop, &payload, true);
                    self.dispose_panic(payload);
                }
                None => primary = Some(payload),
            }
        }
        #[cfg(not(feature = "std"))]
        self.discard_queued();

        #[cfg(feature = "std")]
        let result = if result.is_ok() && primary.is_some() {
            Err(completion
                .failure
                .clone()
                .unwrap_or_else(|| OwnerError::Runner.observed()))
        } else {
            result
        };

        self.complete(result);
        completion.armed = false;
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }

    fn complete(&self, result: Result<(), Observed<E>>) {
        if self.completion.result.borrow().is_some() {
            return;
        }

        let diagnostic = match &result {
            Ok(()) => Ok(()),
            Err((error, text)) => Err((Rc::new(error.without_data()), text.clone())),
        };
        *self.result.borrow_mut() = Some(result);

        if self.completion.ready.borrow().is_none() {
            *self.completion.ready.borrow_mut() = Some(diagnostic.clone());
        }

        *self.completion.result.borrow_mut() = Some(diagnostic);
        self.notify(&[&|| self.completion.changed.notify()]);
    }
}

impl<S, E> Drop for CompletionGuard<'_, S, E> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.complete(Err(self
                .failure
                .take()
                .unwrap_or_else(|| OwnerError::Runner.observed())));
        }
    }
}
