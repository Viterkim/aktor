use super::super::*;

impl<S, const N: usize, E> Inner<S, N, E> {
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
                |payload| {
                    self.completion
                        .diagnostics
                        .borrow_mut()
                        .push(crate::AktorError::new(
                            crate::panic::panic_message(payload),
                            (),
                        ));
                },
            );

            if let Some(payload) = primary {
                std::panic::resume_unwind(payload);
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
            |payload| {
                self.completion
                    .diagnostics
                    .borrow_mut()
                    .push(crate::AktorError::new(
                        crate::panic::panic_message(payload),
                        (),
                    ));
            },
        ) {
            std::panic::resume_unwind(payload);
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

    pub fn lost(&self) -> bool {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor {
            // During shutdown a lost reply is a call shutdown cancelled, a crash reports itself.
            if !group.is_stopping() {
                group.fail(crate::AktorFailureReport {
                    kind: None,
                    actor: name.as_ref().into(),
                    phase: "call".into(),
                    message: "actor stopped without an output".into(),
                });
            }

            true
        } else {
            false
        }
    }

    pub fn rejected(&self) -> bool {
        if self
            .group
            .borrow()
            .as_ref()
            .is_some_and(|(_, group)| group.is_stopping())
        {
            true
        } else {
            self.lost()
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

    pub fn fail(&self, error: &OwnerError<E>) -> bool {
        if matches!(error, OwnerError::SetupDiagnostic(_, true))
            || matches!(error, OwnerError::CleanupFailure(failure) if failure.primary)
        {
            return true;
        }

        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor
            && !(matches!(error, OwnerError::Cancelled) && group.is_stopping())
            && !matches!(error, OwnerError::CleanupUnconfirmed)
        {
            let failure = crate::AktorFailureReport {
                kind: None,
                actor: name.as_ref().into(),
                phase: match error {
                    OwnerError::Setup(_)
                    | OwnerError::SetupDiagnostic(_, _)
                    | OwnerError::SetupPanic(_) => "setup",
                    OwnerError::Cleanup(_)
                    | OwnerError::CleanupFailure(_)
                    | OwnerError::CleanupUnconfirmed => "cleanup",
                    OwnerError::Runner(_) => "runner",
                    OwnerError::Cancelled => "owner",
                }
                .into(),
                message: alloc::format!("{error:#}"),
            };

            if matches!(
                error,
                OwnerError::Setup(_)
                    | OwnerError::SetupDiagnostic(_, _)
                    | OwnerError::SetupPanic(_)
            ) {
                return group.fail_startup(failure);
            } else {
                return group.fail(failure);
            }
        }

        false
    }

    pub fn discard_queued(&self) {
        self.close();
        let queued = core::mem::take(&mut *self.queue.borrow_mut());
        let services = core::mem::take(&mut *self.services.borrow_mut());
        #[cfg(feature = "std")]
        let primary = crate::local::panic::discard(queued.into_iter().chain(services), |payload| {
            self.completion
                .diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(
                    crate::panic::panic_message(payload),
                    (),
                ));
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

    pub fn finish(&self, result: Result<(), Rc<OwnerError<E>>>) {
        if let Err(error) = &result {
            self.fail(error);
        }

        self.close();

        if self.completion.result.borrow().is_some() {
            return;
        }

        let mut completion = CompletionGuard {
            inner: self,
            failure: result.as_ref().err().cloned().unwrap_or_else(|| {
                Rc::new(OwnerError::Runner(crate::AktorError::new(
                    "actor work destruction failed",
                    (),
                )))
            }),
            armed: true,
        };

        #[cfg(feature = "std")]
        let primary =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.discard_queued())).err();
        #[cfg(not(feature = "std"))]
        self.discard_queued();

        #[cfg(feature = "std")]
        let result = if result.is_ok() && primary.is_some() {
            Err(completion.failure.clone())
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

    fn complete(&self, result: Result<(), Rc<OwnerError<E>>>) {
        if self.completion.result.borrow().is_some() {
            return;
        }

        if self.completion.ready.borrow().is_none() {
            *self.completion.ready.borrow_mut() = Some(result.clone());
        }

        *self.completion.result.borrow_mut() = Some(result);
        self.notify(&[&|| self.completion.changed.notify()]);
    }
}

impl<S, const N: usize, E> Drop for CompletionGuard<'_, S, N, E> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.complete(Err(self.failure.clone()));
        }
    }
}
