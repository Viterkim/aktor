use super::driver::poll_owners;
use super::*;
use crate::timeout::embassy_deadline;

impl AktorGroup {
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

    /// Unrepresentable deadlines clamp to the clock's last supported instant.
    pub fn with_grace(grace: Duration) -> Self {
        Self {
            stop_on_drop: false,
            control: Rc::new(Control {
                stopping: Cell::new(false),
                listening: Cell::new(false),
                grace,
                deadline: Cell::new(None),
                failure: RefCell::new(None),
                completed: RefCell::new(None),
                report: RefCell::new(ShutdownReport::default()),
                changed: Event::default(),
            }),
            owners: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn killswitch(&self) -> KillSwitch {
        KillSwitch {
            control: self.control.clone(),
        }
    }

    pub fn completion(&self) -> GroupCompletion {
        GroupCompletion {
            control: self.control.clone(),
        }
    }

    pub fn shutdown(&self) -> GroupCompletion {
        self.killswitch().stop();
        self.completion()
    }

    pub fn listen(&mut self) -> Result<LocalFuture<'static, ShutdownReport>, AktorError> {
        self.listen_with(async |_| Ok::<_, AktorCleanupError>(()))
    }

    /// Keep an existing value on this executor. Cleanup just drops it.
    pub fn spawn_value<S: 'static, const N: usize>(
        &mut self,
        name: impl Into<String>,
        value: S,
    ) -> Result<Handle<S, N, ()>, ActorError> {
        self.spawn(ActorArgs {
            name: name.into(),
            capacity: N,
            setup: async move || Ok(value),
            cleanup: async |_| Ok(()),
        })
    }

    pub fn spawn<S: 'static, const N: usize, E: 'static>(
        &mut self,
        args: ActorArgs<
            impl AsyncFnOnce() -> Result<S, AktorSetupError<E>> + 'static,
            impl AsyncFnOnce(S) -> Result<(), AktorCleanupError<E>> + 'static,
        >,
    ) -> Result<Handle<S, N, E>, ActorError> {
        if self.killswitch().is_stopping() {
            return Err(ActorError::Closed);
        }
        if !self.control.listening.get() {
            return Err(ActorError::NotStarted);
        }
        if args.capacity != N {
            return Err(ActorError::InvalidCapacity);
        }
        let (handle, owner) = channel::<S, N, E>()?;
        *handle.inner.group.borrow_mut() = Some((args.name.clone(), self.killswitch()));
        let shutdown = handle.new_handle();
        self.owners.borrow_mut().push(Entry {
            name: args.name,
            shutdown: Box::new(move || {
                shutdown.shutdown();
            }),
            owner: Box::pin(async move {
                match owner
                    .run_with(
                        async move || (args.setup)().await,
                        async move |state| (args.cleanup)(state).await,
                    )
                    .await
                {
                    Ok(()) => None,
                    Err(error) => match &*error {
                        OwnerError::Setup(_) => None,
                        OwnerError::Cleanup(error) => Some(error.report()),
                        OwnerError::Cancelled => Some(AktorError::new("actor owner cancelled")),
                    },
                }
            }),
            finished: false,
        });
        self.control.changed.notify();
        Ok(handle)
    }

    /// Move this future onto your local executor to drive the actors.
    /// Dropping it cancels the actors and publishes a failed report.
    pub fn listen_with<'a, Cleanup, CleanupFuture, E>(
        &mut self,
        cleanup: Cleanup,
    ) -> Result<LocalFuture<'a, ShutdownReport>, AktorError>
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture + 'a,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>> + 'a,
        E: 'a,
    {
        self.claim_listener()?;
        self.stop_on_drop = true;
        let driver = Driver {
            control: self.control.clone(),
            owners: self.owners.clone(),
        };
        let group = Self {
            stop_on_drop: false,
            control: self.control.clone(),
            owners: self.owners.clone(),
        };

        Ok(Box::pin(async move {
            let changed = group.control.changed.listen();
            poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&group.owners, cx, &group.control);
                if group.killswitch().is_stopping() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            drop(changed);
            let report = group.finish(cleanup).await;
            drop(driver);
            report
        }))
    }

    pub(super) fn claim_listener(&self) -> Result<(), AktorError> {
        if self.control.stopping.get() {
            return Err(AktorError::new("this group has already stopped"));
        }
        if self.control.listening.replace(true) {
            Err(AktorError::new(
                "this group already has a shutdown listener",
            ))
        } else {
            Ok(())
        }
    }
}
impl Drop for AktorGroup {
    fn drop(&mut self) {
        if self.stop_on_drop || !self.control.listening.get() {
            self.killswitch().stop();
        }
    }
}
impl Default for AktorGroup {
    fn default() -> Self {
        Self::new()
    }
}
impl KillSwitch {
    pub fn stop(&self) {
        if !self.control.stopping.replace(true) {
            self.control.deadline.set(Some(embassy_deadline(
                embassy_time::Instant::now(),
                self.control.grace,
            )));
            if !self.control.listening.get() {
                let mut report = self.control.report.borrow().clone();
                report.failure = self.control.failure.borrow().clone();
                *self.control.completed.borrow_mut() = Some(report);
            }
        }
        self.control.changed.notify();
    }

    pub fn is_stopping(&self) -> bool {
        self.control.stopping.get()
    }

    pub async fn wait_stopping(&self) {
        let changed = self.control.changed.listen();
        poll_fn(|cx| {
            changed.register(cx);
            if self.is_stopping() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    pub fn fail(&self, reason: ActorFailure) {
        if self.control.failure.borrow().is_none() {
            *self.control.failure.borrow_mut() = Some(reason);
        }
        self.stop();
    }
}

impl GroupCompletion {
    pub async fn wait(&self) -> ShutdownReport {
        let changed = self.control.changed.listen();
        poll_fn(|cx| {
            changed.register(cx);
            match self.control.completed.borrow().clone() {
                Some(report) => Poll::Ready(report),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl core::future::IntoFuture for GroupCompletion {
    type Output = ShutdownReport;
    type IntoFuture = LocalFuture<'static, ShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a> core::future::IntoFuture for &'a GroupCompletion {
    type Output = ShutdownReport;
    type IntoFuture = LocalFuture<'a, ShutdownReport>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}
