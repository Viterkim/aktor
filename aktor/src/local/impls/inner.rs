use super::super::*;

impl<S, const N: usize, E> Inner<S, N, E> {
    pub fn close(&self) {
        if self.open.replace(false) {
            self.closed.notify();
            self.wake_sessions();
        }
    }

    fn wake_sessions(&self) {
        let callbacks: alloc::vec::Vec<_> = {
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

    pub fn lost(&self) -> bool {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor {
            group.fail(crate::ActorFailure {
                kind: None,
                actor: name,
                phase: "call".into(),
                message: "actor stopped without an output".into(),
            });

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
        self.closed.notify();
    }

    pub fn enqueue(&self, message: Message<S>) -> Result<(), Message<S>> {
        let mut queue = self.queue.borrow_mut();

        if queue.len() == self.capacity {
            return Err(message);
        }

        queue.push_back(message);
        drop(queue);
        self.closed.notify();
        Ok(())
    }

    pub fn fail(&self, error: &OwnerError<E>) {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor
            && !(matches!(error, OwnerError::Cancelled) && group.is_stopping())
        {
            let failure = crate::ActorFailure {
                kind: None,
                actor: name,
                phase: match error {
                    OwnerError::Setup(_) | OwnerError::SetupPanic(_) => "setup",
                    OwnerError::Cleanup(_) => "cleanup",
                    OwnerError::Runner(_) => "runner",
                    OwnerError::Cancelled => "owner",
                }
                .into(),
                message: alloc::format!("{error:#}"),
            };

            if matches!(error, OwnerError::Setup(_) | OwnerError::SetupPanic(_)) {
                group.fail_startup(failure);
            } else {
                group.fail(failure);
            }
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
                )))
            }),
            armed: true,
        };

        let queued = core::mem::take(&mut *self.queue.borrow_mut());
        let services = core::mem::take(&mut *self.services.borrow_mut());
        #[cfg(feature = "std")]
        let primary = crate::local::panic::discard(queued.into_iter().chain(services), |payload| {
            self.completion
                .diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(crate::panic::panic_message(payload)));
        });
        #[cfg(not(feature = "std"))]
        for message in queued.into_iter().chain(services) {
            drop(message);
        }
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
        self.completion.changed.notify();
        self.wake_sessions();
    }
}

impl<S, const N: usize, E> Drop for CompletionGuard<'_, S, N, E> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.complete(Err(self.failure.clone()));
        }
    }
}
