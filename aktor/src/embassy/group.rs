use super::*;
pub use crate::{ActorArgs, ShutdownReport};
use crate::{ActorFailure, ActorOutcome, AktorError};
use alloc::{string::String, vec::Vec};
use core::{future::poll_fn, ops::AsyncFnOnce, time::Duration};

pub struct AktorGroup {
    stop_on_drop: bool,
    control: Rc<Control>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

pub struct GroupCompletion {
    control: Rc<Control>,
}

#[derive(Clone)]
pub struct KillSwitch {
    control: Rc<Control>,
}

struct Control {
    stopping: Cell<bool>,
    listening: Cell<bool>,
    grace: Duration,
    deadline: Cell<Option<embassy_time::Instant>>,
    failure: RefCell<Option<ActorFailure>>,
    completed: RefCell<Option<ShutdownReport>>,
    changed: Event,
}

struct Entry {
    name: String,
    shutdown: Box<dyn Fn()>,
    owner: LocalFuture<'static, Option<AktorCleanupError>>,
    finished: bool,
}

impl AktorGroup {
    pub fn new() -> Self {
        Self::with_grace(Duration::from_secs(5))
    }

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
        if !self.control.listening.get() {
            return Err(ActorError::NotStarted);
        }
        if self.killswitch().is_stopping() {
            return Err(ActorError::Closed);
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
        let group = Self {
            stop_on_drop: false,
            control: self.control.clone(),
            owners: self.owners.clone(),
        };

        Ok(Box::pin(async move {
            let changed = group.control.changed.listen();
            let mut report = ShutdownReport::default();
            poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&group.owners, cx, &mut report);
                if group.killswitch().is_stopping() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            drop(changed);
            group.finish(cleanup, report).await
        }))
    }

    fn claim_listener(&self) -> Result<(), AktorError> {
        if self.control.listening.replace(true) {
            Err(AktorError::new(
                "this group already has a shutdown listener",
            ))
        } else {
            Ok(())
        }
    }

    pub async fn run<O, A, Application, Cleanup, CleanupFuture, E>(
        mut self,
        application: Application,
        cleanup: Cleanup,
    ) -> Result<Option<O>, Box<ShutdownReport>>
    where
        Application: AsyncFnOnce(&mut AktorGroup) -> Result<O, AktorError<A>>,
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        if let Err(error) = self.claim_listener() {
            return Err(Box::new(ShutdownReport {
                application: alloc::vec![error],
                ..ShutdownReport::default()
            }));
        }
        let owners = self.owners.clone();
        let kill = self.killswitch();
        let changed = kill.control.changed.listen();
        let mut report = ShutdownReport::default();
        let output = {
            let mut app = Box::pin(application(&mut self));
            poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&owners, cx, &mut report);
                if kill.is_stopping() {
                    return Poll::Ready(None);
                }
                match app.as_mut().poll(cx) {
                    Poll::Ready(Ok(value)) => Poll::Ready(Some(value)),
                    Poll::Ready(Err(error)) => {
                        report.application.push(error.report());
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await
        };
        let report = self.finish(cleanup, report).await;
        if report.failed() {
            Err(Box::new(report))
        } else {
            Ok(output)
        }
    }

    async fn finish<Cleanup, CleanupFuture, E>(
        self,
        cleanup: Cleanup,
        mut report: ShutdownReport,
    ) -> ShutdownReport
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        let owners = self.owners.clone();
        let kill = self.killswitch();
        kill.stop();
        for entry in owners.borrow().iter().rev() {
            (entry.shutdown)();
        }
        let mut timer = Box::pin(embassy_time::Timer::at(
            kill.control
                .deadline
                .get()
                .unwrap_or_else(embassy_time::Instant::now),
        ));
        poll_fn(|cx| {
            poll_owners(&owners, cx, &mut report);
            if owners.borrow().iter().all(|entry| entry.finished) {
                return Poll::Ready(());
            }
            if timer.as_mut().poll(cx).is_ready() {
                report.timed_out = true;
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
        for entry in owners.borrow().iter().filter(|entry| !entry.finished) {
            report.actors.push(ActorOutcome {
                actor: entry.name.clone(),
                diagnostics: Vec::new(),
                timed_out: true,
            });
        }
        owners.borrow_mut().clear();
        report.failure = kill.control.failure.borrow().clone();
        let mut hook = Box::pin(cleanup(report.clone()));
        poll_fn(|cx| match hook.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => {
                report.application.push(error.report());
                Poll::Ready(())
            }
            Poll::Pending => {
                if timer.as_mut().poll(cx).is_ready() {
                    report.timed_out = true;
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        })
        .await;
        drop(hook);
        report.failure = kill.control.failure.borrow().clone();
        *kill.control.completed.borrow_mut() = Some(report.clone());
        kill.control.changed.notify();
        report
    }
}
impl Drop for AktorGroup {
    fn drop(&mut self) {
        if self.stop_on_drop {
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
            let micros = self.control.grace.as_micros().min(u128::from(u64::MAX)) as u64;
            self.control.deadline.set(Some(
                embassy_time::Instant::now() + embassy_time::Duration::from_micros(micros),
            ));
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
    pub(crate) fn fail(&self, reason: ActorFailure) {
        if self.control.failure.borrow().is_none() {
            *self.control.failure.borrow_mut() = Some(reason);
        }
        self.stop();
    }
}

fn poll_owners(
    owners: &RefCell<Vec<Entry>>,
    cx: &mut core::task::Context<'_>,
    report: &mut ShutdownReport,
) {
    let mut entries = owners.borrow_mut();
    for entry in entries.iter_mut() {
        if !entry.finished
            && let Poll::Ready(error) = entry.owner.as_mut().poll(cx)
        {
            entry.finished = true;
            report.actors.push(ActorOutcome {
                actor: entry.name.clone(),
                diagnostics: error.into_iter().collect(),
                timed_out: false,
            });
        }
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
