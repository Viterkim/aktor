use super::clock::TaskDeadline;
use super::*;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use crate::setup::kind::TokioTask;
use crate::{AktorClosure, operation::Operation};
use crate::{
    AktorError, AktorFailureReport, AktorGroup, AktorGroupError, AktorHooks, AktorNew,
    AktorOutcome, AktorStartCause, AktorStartError,
    group::shutdown::{contain_drop, panic_message},
    operation::hooks::CallHooks,
    setup::initial::InitError,
    setup::kind::AktorMode,
};
use core::task::{Context, Poll};
use futures_util::{FutureExt, future::Either};
use hooks::{AktorTaskEnd, AktorTaskInterval};
use std::panic::{AssertUnwindSafe, catch_unwind};

struct TaskLogic<S: 'static, Start> {
    start: Option<Start>,
    end: Option<AktorClosure<dyn AktorTaskEnd<S> + Send>>,
    intervals: Vec<TaskInterval<S>>,
    hooks: CallHooks<S>,
    clock: TaskClock,
    kill: AktorKillSwitch,
}
impl<S: 'static, Start> Drop for TaskLogic<S, Start> {
    fn drop(&mut self) {
        contain_drop(self.start.take(), &self.kill, "task start drop");
        contain_drop(self.end.take(), &self.kill, "task end drop");
        contain_drop(self.hooks.before_each.take(), &self.kill, "task hook drop");
        contain_drop(self.hooks.after_each.take(), &self.kill, "task hook drop");

        for interval in std::mem::take(&mut self.intervals) {
            contain_drop(interval, &self.kill, "task interval drop");
        }
    }
}

struct TaskInterval<S: 'static> {
    every: std::time::Duration,
    run: AktorClosure<dyn AktorTaskInterval<S> + Send>,
}

struct Completion {
    cleanup: Arc<parking_lot::Mutex<Vec<Arc<crate::AktorFailure>>>>,
    report: AktorOutcome,
    kill: AktorKillSwitch,
    completed: Option<oneshot::Sender<(AktorOutcome, bool)>>,
    finished: watch::Sender<AktorTaskStatus>,
    failed: bool,
    state_leased: bool,
}
impl Completion {
    fn fail(&mut self, phase: &str, message: String) -> bool {
        self.failed = true;
        self.kill.fail(AktorFailureReport {
            actor: self.report.actor.clone(),
            kind: Some(self.report.kind),
            phase: phase.into(),
            message,
        })
    }

    fn fail_startup(&mut self, message: String) -> bool {
        self.failed = true;
        self.kill.fail_startup(AktorFailureReport {
            actor: self.report.actor.clone(),
            kind: Some(self.report.kind),
            phase: "setup".into(),
            message,
        })
    }

    fn dispose(&mut self, value: impl Sized, phase: &str) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            let message = panic_message(&payload);

            if !self.fail(phase, message.clone()) {
                self.report
                    .diagnostics
                    .push(AktorError::new(format!("{phase}: {message}"), ()));
            }
            contain_drop(payload, &self.kill, "task panic payload drop");
        }
    }

    fn dispose_hooks<S>(&mut self, hooks: CallHooks<S>) {
        self.dispose(hooks.before_each, "task hook drop");
        self.dispose(hooks.after_each, "task hook drop");
    }

    fn dispose_intervals<S: 'static>(&mut self, intervals: Vec<TaskInterval<S>>) {
        for interval in intervals {
            self.dispose(interval, "task interval drop");
        }
    }

    fn dispose_receiver<S>(&mut self, mut driver: service::CallDriver<S>) {
        driver.close();

        while let Ok(message) = driver.receiver.try_recv() {
            self.dispose(message, "task queue drop");
        }

        self.dispose(driver, "task queue drop");
    }

    fn publish(&mut self) {
        self.finished
            .send_replace(if self.failed || self.report.ran_past_grace {
                AktorTaskStatus::Failed
            } else {
                AktorTaskStatus::Finished
            });

        if let Some(completed) = self.completed.take() {
            let _sent = completed.send((self.report.clone(), self.state_leased));
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if self.completed.is_some() {
            self.report.retirement = crate::AktorRetireStatus::Unconfirmed;
            let message = "actor task cancelled before cleanup completed";

            if !self.fail("owner", message.into()) {
                self.report.diagnostics.push(AktorError::new(message, ()));
            }
            self.publish();
        }
    }
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[doc(hidden)]
pub async fn start<S, Start, Fut, End, CM, E, M>(
    setup: AktorNew<S, Start, TokioTask, End, (CM, E, M)>,
    group: &mut AktorGroup,
) -> Result<AktorTask<S>, AktorStartError<E::Error>>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
{
    start_on(setup, group, TaskClock::Tokio, |future| {
        TaskJoin::Tokio(tokio::spawn(future))
    })
    .await
}

#[doc(hidden)]
pub async fn start_on<S, Start, Fut, Kind, End, CM, E, M>(
    setup: AktorNew<S, Start, Kind, End, (CM, E, M)>,
    group: &mut AktorGroup,
    clock: TaskClock,
    spawn: impl FnOnce(TaskOwnerFuture) -> TaskJoin,
) -> Result<AktorTask<S>, AktorStartError<E::Error>>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            Interval<S> = dyn AktorTaskInterval<S> + Send,
        >,
{
    let kind = Kind::EXECUTION;
    let capacity = setup.capacity;

    let invalid = if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
        Some("queue capacity must be positive and fit the semaphore")
    } else if setup
        .hooks
        .intervals
        .iter()
        .any(|interval| interval.every.is_zero())
    {
        Some("interval duration must be positive")
    } else {
        None
    };

    if let Some(reason) = invalid {
        return Err(AktorStartError::not_run(crate::setup::validation_error(
            group,
            &setup.name,
            kind,
            AktorStartCause::Setup(AktorError::new(reason, ())),
        )));
    }

    #[cfg(not(target_family = "wasm"))]
    let stuck = setup.stuck;
    let name = Arc::new(setup.name);
    let kill = group.killswitch();
    let (sender, receiver) = mpsc::unbounded_channel();
    let slots = Arc::new(Semaphore::new(capacity));
    let (closing, closed) = oneshot::channel();
    let (forcing, forced) = oneshot::channel();
    let (completed, outcome) = oneshot::channel();
    let (joining, joined) = oneshot::channel::<TaskJoin>();
    let (finished, observed) = watch::channel(AktorTaskStatus::Running);
    let services = Arc::new(service::Services::new());

    let service_driver = service::ServiceDriver {
        services: services.clone(),
        kill: kill.clone(),
    };
    let label = (*name).clone();
    let observer_kill = kill.clone();

    group
        .register_owner(
            (*name).clone(),
            kind,
            Box::new(move || {
                let _sent = closing.send(());
            }),
            Box::new(move || {
                let _sent = forcing.send(());
            }),
            None,
            None,
            Box::pin(async move {
                let (mut report, state_leased) = outcome.await.unwrap_or_else(|_| {
                    (
                        AktorOutcome {
                            retirement: crate::AktorRetireStatus::Unconfirmed,
                            actor: label,
                            kind,
                            diagnostics: vec![AktorError::new("actor task observer stopped", ())],
                            ran_past_grace: false,
                        },
                        false,
                    )
                });

                if let Ok(join) = joined.await {
                    join.wait(&observer_kill, &mut report).await;
                    if !state_leased && report.retirement == crate::AktorRetireStatus::Unconfirmed {
                        report.retirement = crate::AktorRetireStatus::Terminated;
                    }
                }

                report
            }),
        )
        .map_err(|error| AktorStartError::not_run(AktorStartCause::Group(error)))?;

    let cleanup_errors = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let completion = Completion {
        cleanup: cleanup_errors.clone(),
        report: AktorOutcome {
            retirement: crate::AktorRetireStatus::Unconfirmed,
            actor: (*name).clone(),
            kind,
            diagnostics: Vec::new(),
            ran_past_grace: false,
        },
        kill: kill.clone(),
        completed: Some(completed),
        failed: false,
        state_leased: false,
        finished,
    };

    #[cfg(not(target_family = "wasm"))]
    let life = kill.track_thread((*name).clone(), stuck);
    let (ready, started) = oneshot::channel();
    let start = setup.start;
    let end = setup.end;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = setup.hooks;

    let logic = TaskLogic {
        start: Some(move || async move { start().await.map_err(|error| error.split()) }),
        end: Some(AktorClosure(Box::new(move |state| end.finish(state)))),
        hooks: CallHooks {
            before_each: before_each.map(|hook| hook.0),
            after_each: after_each.map(|hook| hook.0),
        },
        clock,
        kill: kill.clone(),
        intervals: intervals
            .into_iter()
            .map(|interval| TaskInterval {
                every: interval.every,
                run: interval.run,
            })
            .collect(),
    };
    let receiver = service::CallDriver {
        receiver,
        slots: slots.clone(),
        kill: kill.clone(),
    };

    let join = spawn(Box::pin(async move {
        #[cfg(not(target_family = "wasm"))]
        let _life = life;

        drive(
            logic,
            receiver,
            service_driver,
            closed,
            forced,
            ready,
            completion,
        )
        .await;
    }));

    let _sent = joining.send(join);

    let completion = group.completion();
    tokio::select! {
        biased;
        result = started => result.map_err(|_| {
            if kill.is_stopping() {
                AktorStartCause::Group(AktorGroupError::Stopping)
            } else {
                AktorStartCause::Setup(AktorError::new("actor task cancelled during setup", ()))
            }
        })?,
        _ = completion.wait() => Err(AktorGroupError::Stopping.into()),
    }?;

    if kill.is_stopping() {
        return Err(AktorGroupError::Stopping.into());
    }

    Ok(AktorTask {
        cleanup: cleanup_errors,
        name,
        sender,
        slots,
        services,
        kill,
        finished: observed,
        role: PhantomData,
        clock,
    })
}

struct Running<'a, S> {
    job: Pin<Box<dyn Job<S> + Send>>,
    lease: Option<AktorTaskState<S>>,
    state: &'a Arc<Mutex<Option<S>>>,
    waiting: Option<AktorTaskFuture<'static, OwnedMutexGuard<Option<S>>>>,
    hooks: &'a mut CallHooks<S>,
    operation: Operation,
}
impl<'a, S: Send + 'static> Running<'a, S> {
    fn new(
        job: Box<dyn Job<S> + Send>,
        state: &'a Arc<Mutex<Option<S>>>,
        lease: Option<AktorTaskState<S>>,
        hooks: &'a mut CallHooks<S>,
        operation: Operation,
    ) -> Self {
        let (lease, waiting) = if lease.is_some() {
            (lease, None)
        } else {
            match state.clone().try_lock_owned() {
                Ok(guard) => (Some(AktorTaskState { guard }), None),
                Err(_) => (
                    None,
                    Some(Box::pin(state.clone().lock_owned()) as AktorTaskFuture<'static, _>),
                ),
            }
        };

        Self {
            job: Box::into_pin(job),
            lease,
            state,
            waiting,
            hooks,
            operation,
        }
    }
}
impl<S: Send + 'static> Future for Running<'_, S> {
    type Output = Option<AktorTaskState<S>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.lease.is_some() {
            #[cfg(feature = "tokio")]
            core::task::ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
        } else if let Some(waiting) = &mut this.waiting {
            let guard = core::task::ready!(waiting.as_mut().poll(cx));
            this.waiting = None;
            this.lease = Some(AktorTaskState { guard });
        }

        let lease = core::task::ready!(this.job.as_mut().poll(
            cx,
            this.lease.take(),
            this.state,
            this.hooks,
            this.operation
        ));
        // A manually built request can return another actor's state.
        Poll::Ready(
            lease.filter(|lease| Arc::ptr_eq(OwnedMutexGuard::mutex(&lease.guard), this.state)),
        )
    }
}

async fn drive<S, Start, Fut, E>(
    mut closures: TaskLogic<S, Start>,
    mut receiver: service::CallDriver<S>,
    service_driver: service::ServiceDriver<S>,
    closed: oneshot::Receiver<()>,
    mut forced: oneshot::Receiver<()>,
    ready: oneshot::Sender<Result<(), AktorStartError<E>>>,
    mut completion: Completion,
) where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    E: Send + 'static,
    Fut: Future<Output = Result<S, (E, String)>> + Send + 'static,
{
    let clock = closures.clock;

    let mut setup = Box::pin(
        AssertUnwindSafe(async {
            let start = closures.start.take()?;
            Some(start().await)
        })
        .catch_unwind(),
    );
    let result = tokio::select! {
        biased;
        result = &mut setup => Some(result),
        _ = &mut forced => None,
    };

    let primary = match &result {
        Some(Ok(None)) => completion.fail_startup("actor task setup already consumed".into()),
        Some(Err(payload)) => completion.fail_startup(panic_message(payload)),
        _ => false,
    };

    completion.dispose(setup, "task setup drop");

    let state = match result {
        Some(Ok(Some(Ok(state)))) => state,
        Some(Ok(Some(Err((data, diagnostics))))) => {
            completion.failed = true;
            completion.report.retirement = crate::AktorRetireStatus::Confirmed;
            let mut unsent = None;
            let error = completion.kill.startup_error_with(
                completion.report.actor.clone(),
                completion.report.kind,
                AktorError::new(diagnostics, ()),
                |primary, diagnostics| {
                    unsent = ready
                        .send(Err(AktorStartError {
                            init: crate::AktorInitOutcome::Failed(data),
                            cause: AktorStartCause::Init(crate::AktorFailure {
                                diagnostics: diagnostics.into(),
                                primary,
                            }),
                            report: None,
                        }))
                        .err();
                },
            );
            if !error.primary {
                completion
                    .report
                    .diagnostics
                    .push(AktorError::new(error.diagnostics, ()));
            }

            completion.dispose(unsent, "startup reply drop");

            completion.dispose_hooks(std::mem::take(&mut closures.hooks));
            completion.dispose_intervals(std::mem::take(&mut closures.intervals));
            completion.dispose(closures.end.take(), "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
        Some(Ok(None)) => {
            let _sent = ready.send(Err(AktorError::new(
                "actor task setup already consumed",
                (),
            )
            .into()));

            completion.dispose_hooks(std::mem::take(&mut closures.hooks));
            completion.dispose_intervals(std::mem::take(&mut closures.intervals));
            completion.dispose(closures.end.take(), "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
        Some(Err(payload)) => {
            let message = panic_message(&payload);

            if !primary {
                completion
                    .report
                    .diagnostics
                    .push(AktorError::new(message.clone(), ()));
            }
            completion.dispose(payload, "task setup panic drop");

            let _sent = ready.send(Err(AktorError::new(message, ()).into()));

            completion.dispose_hooks(std::mem::take(&mut closures.hooks));
            completion.dispose_intervals(std::mem::take(&mut closures.intervals));
            completion.dispose(closures.end.take(), "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
        None => {
            completion.report.ran_past_grace = true;

            let _sent = ready.send(Err(AktorGroupError::Stopping.into()));

            completion.dispose_hooks(std::mem::take(&mut closures.hooks));
            completion.dispose_intervals(std::mem::take(&mut closures.intervals));
            completion.dispose(closures.end.take(), "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
    };

    let state = Arc::new(Mutex::new(Some(state)));
    let _sent = ready.send(Ok(()));
    // Reuse these waits so calls don't resubscribe to shutdown.
    let closing = async move {
        let _closed = closed.await;
    };
    let forcing = async move {
        let _forced = forced.await;
    };

    tokio::pin!(closing, forcing);

    let mut lease = None;
    let mut stopping = false;
    let mut ordinary_done = false;
    let mut services_done = false;
    let services = service_driver.services.clone();
    let service = services.next();

    tokio::pin!(service);

    let mut next: Vec<_> = closures
        .intervals
        .iter()
        .map(|interval| clock.deadline(interval.every))
        .collect();
    let portable_budget = match clock {
        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        TaskClock::Tokio => false,
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        TaskClock::Std => true,
        #[cfg(target_family = "wasm")]
        TaskClock::Browser => true,
    };
    let mut remaining = 32;
    let mut due = next_interval(&next);
    #[cfg(not(target_family = "wasm"))]
    let timer = wait_interval(due);

    #[cfg(not(target_family = "wasm"))]
    tokio::pin!(timer);

    loop {
        if ordinary_done && services_done {
            break;
        }

        let work = {
            #[cfg(target_family = "wasm")]
            let timer = wait_interval(due);
            #[cfg(target_family = "wasm")]
            tokio::pin!(timer);

            tokio::select! {
                _ = &mut closing, if !stopping => {
                    stopping = true;
                    receiver.close();
                    services.close();
                    continue;
                },
                _ = &mut forcing => break,
                message = receiver.recv(), if !ordinary_done => match message {
                    Some(message) => Some(message),
                    None => { ordinary_done = true; stopping = true; services.close(); continue; }
                },
                message = &mut service, if !services_done => match message {
                    Some(message) => {
                        service.set(services.next());
                        Some(message)
                    },
                    None => { services_done = true; continue; }
                },
                _ = &mut timer, if !stopping && due.is_some() => None,
            }
        };

        let interval = work.is_none();
        let operation = work.as_ref().map_or(
            Operation {
                name: "interval",
                caller: std::panic::Location::caller(),
            },
            |work| work.operation,
        );

        let run = match work {
            Some(message) => Either::Left(Running::new(
                message.job,
                &state,
                lease.take(),
                &mut closures.hooks,
                operation,
            )),
            None => {
                let Some((index, _)) = due else {
                    continue;
                };
                drop(lease.take());
                let state = state.clone();
                let interval = &mut closures.intervals[index];
                let hooks = &mut closures.hooks;
                let next = &mut next[index];

                Either::Right(Box::pin(async move {
                    let guard = state.clone().lock_owned().await;

                    let mut lease = AktorTaskState { guard };

                    hooks.before(&mut lease, operation);
                    interval.run.0.run(lease).await;

                    let mut state = state.lock().await;

                    if let Some(state) = &mut *state {
                        hooks.after(state, operation);
                    }

                    *next = clock.deadline(interval.every);
                    None
                }))
            }
        };

        let mut run = AssertUnwindSafe(run).catch_unwind();
        let outcome = loop {
            tokio::select! {
                biased;
                result = &mut run => break Some(result),
                _ = &mut closing, if !stopping => {
                    stopping = true;
                    receiver.close();
                    services.close();
                },
                _ = &mut forcing => break None,
            }
        };

        if let Some(Err(payload)) = &outcome {
            let message = panic_message(payload);

            if !completion.fail(operation.name, message.clone()) {
                completion.report.diagnostics.push(AktorError::new(
                    format!("{}: {message}", operation.name),
                    (),
                ));
            }
        }

        completion.dispose(run, "task operation drop");

        match outcome {
            Some(Ok(returned)) => lease = returned,
            Some(Err(payload)) => {
                completion.dispose(payload, "task operation panic drop");
                break;
            }
            None => {
                completion.report.ran_past_grace = true;
                break;
            }
        }

        if interval {
            due = next_interval(&next);
            #[cfg(not(target_family = "wasm"))]
            timer.set(wait_interval(due));
        }

        if portable_budget {
            remaining -= 1;

            if remaining == 0 {
                super::clock::yield_owner().await;
                remaining = 32;
            }
        }
    }

    receiver.close();

    for message in service_driver.services.discard() {
        completion.dispose(message, "task latest queue drop");
    }

    completion.dispose(service_driver, "task latest driver drop");
    completion.dispose_receiver(receiver);

    let mut recovered = false;
    let mut cleanup = Box::pin(
        AssertUnwindSafe(async {
            let state = match lease {
                Some(mut lease) => lease.guard.take(),
                None => state.lock().await.take(),
            };
            recovered = true;
            let Some(state) = state else {
                return Ok(());
            };

            if let Some(end) = closures.end.take() {
                end.0.run(state).await
            } else {
                drop(state);
                Ok(())
            }
        })
        .catch_unwind(),
    );

    let result = tokio::select! {
        biased;
        result = clock.end_within(&completion.kill, &mut cleanup) => result,
        _ = clock.wait_for_force(&completion.kill) => None,
    };

    let primary = match &result {
        Some(Ok(Err(error))) => completion.fail("cleanup", error.diagnostics.clone()),
        Some(Err(payload)) => completion.fail("cleanup", panic_message(payload)),
        _ => false,
    };

    completion.dispose(cleanup, "task cleanup drop");

    match result {
        Some(Ok(Ok(()))) => {
            completion.report.retirement = crate::AktorRetireStatus::Confirmed;
        }
        Some(Ok(Err(mut error))) => {
            completion.report.retirement = crate::AktorRetireStatus::Confirmed;
            error.primary = primary;

            if !primary {
                completion
                    .report
                    .diagnostics
                    .push(AktorError::new(error.diagnostics.clone(), ()));
            }
            completion.cleanup.lock().push(Arc::new(error));
        }
        Some(Err(payload)) => {
            if !primary {
                completion
                    .report
                    .diagnostics
                    .push(AktorError::new(panic_message(&payload), ()));
            }
            completion.dispose(payload, "task cleanup panic drop");
        }
        None => {
            completion.report.ran_past_grace = true;
            completion.state_leased = !recovered;
            completion.report.diagnostics.push(AktorError::new(
                if recovered {
                    "end ran out of its grace"
                } else {
                    "actor state still leased during shutdown"
                },
                (),
            ));
        }
    }

    completion.dispose_hooks(std::mem::take(&mut closures.hooks));
    completion.dispose_intervals(std::mem::take(&mut closures.intervals));
    completion.dispose(closures.end.take(), "task end drop");
    completion.publish();
}

fn next_interval(next: &[Option<TaskDeadline>]) -> Option<(usize, TaskDeadline)> {
    next.iter()
        .enumerate()
        .filter_map(|(index, deadline)| deadline.map(|deadline| (index, deadline)))
        .min_by_key(|(_, deadline)| *deadline)
}

async fn wait_interval(due: Option<(usize, TaskDeadline)>) {
    if let Some((_, deadline)) = due {
        deadline.wait().await;
    } else {
        core::future::pending::<()>().await;
    }
}

#[cfg(all(test, feature = "bevy", not(target_family = "wasm")))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct Signal(AtomicUsize);
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct Chain {
        sender: mpsc::UnboundedSender<Message<()>>,
        slots: Arc<Semaphore>,
        services: Option<Arc<service::Services<()>>>,
        count: Arc<AtomicUsize>,
        remaining: usize,
    }
    impl Chain {
        fn enqueue(self) {
            let sender = self.sender.clone();
            let slots = self.slots.clone();
            let services = self.services.clone();
            let message = Message {
                operation: Operation {
                    name: "chain",
                    caller: std::panic::Location::caller(),
                },
                job: Box::new(self),
            };

            if let Some(services) = services {
                assert!(services.push(message, false).is_ok());
                services.wake.notify_one();
            } else {
                slots.try_acquire().unwrap().forget();
                assert!(sender.send(message).is_ok());
            }
        }
    }
    impl Job<()> for Chain {
        fn poll(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            state: Option<AktorTaskState<()>>,
            _: &Arc<Mutex<Option<()>>>,
            _: &mut CallHooks<()>,
            _: Operation,
        ) -> Poll<Option<AktorTaskState<()>>> {
            let this = self.get_mut();
            this.count.fetch_add(1, Ordering::Relaxed);

            if this.remaining > 1 {
                Chain {
                    sender: this.sender.clone(),
                    slots: this.slots.clone(),
                    services: this.services.clone(),
                    count: this.count.clone(),
                    remaining: this.remaining - 1,
                }
                .enqueue();
            }

            Poll::Ready(state)
        }
    }

    #[test]
    fn owner_budget() {
        for latest in [false, true] {
            let group = AktorGroup::new();
            let kill = group.killswitch();
            let (sender, receiver) = mpsc::unbounded_channel();
            let slots = Arc::new(Semaphore::new(1));
            let services = Arc::new(service::Services::new());
            let count = Arc::new(AtomicUsize::new(0));
            let cleaned = Arc::new(AtomicBool::new(false));
            let cleanup = cleaned.clone();
            let (_closing, closed) = oneshot::channel();
            let (_forcing, forced) = oneshot::channel();
            let (ready, _started) = oneshot::channel();
            let (completed, outcome) = oneshot::channel();
            let (finished, _observed) = watch::channel(AktorTaskStatus::Running);

            let completion = Completion {
                cleanup: Arc::new(parking_lot::Mutex::new(Vec::new())),
                report: AktorOutcome {
                    retirement: crate::AktorRetireStatus::Confirmed,
                    actor: "chain".into(),
                    kind: crate::AktorExecution::BevyTask,
                    diagnostics: vec![],
                    ran_past_grace: false,
                },
                kill: kill.clone(),
                completed: Some(completed),
                finished,
                failed: false,
                state_leased: false,
            };

            let logic = TaskLogic {
                start: Some(async || Ok::<_, ((), String)>(())),
                end: Some(AktorClosure(Box::new(async move |_: ()| {
                    cleanup.store(true, Ordering::Relaxed);
                    Ok::<_, crate::AktorFailure>(())
                }))),
                intervals: vec![],
                hooks: CallHooks {
                    before_each: None,
                    after_each: None,
                },
                clock: TaskClock::Std,
                kill: kill.clone(),
            };

            Chain {
                sender,
                slots: slots.clone(),
                services: latest.then(|| services.clone()),
                count: count.clone(),
                remaining: 256,
            }
            .enqueue();

            let mut owner = Box::pin(drive(
                logic,
                service::CallDriver {
                    receiver,
                    slots,
                    kill: kill.clone(),
                },
                service::ServiceDriver {
                    services,
                    kill: kill.clone(),
                },
                closed,
                forced,
                ready,
                completion,
            ));
            let signal = Arc::new(Signal(AtomicUsize::new(0)));
            let waker = Waker::from(signal.clone());
            let first = owner.as_mut().poll(&mut Context::from_waker(&waker));
            let mut polled = first;
            let observed = count.load(Ordering::Relaxed);
            let wakes = signal.0.load(Ordering::Relaxed);

            for _ in 0..256 {
                if polled.is_ready() || count.load(Ordering::Relaxed) == 256 {
                    break;
                }

                polled = owner.as_mut().poll(&mut Context::from_waker(&waker));
            }
            kill.stop();

            if polled.is_pending() {
                crate::executor::block_on(owner);
            }
            let (report, _) = crate::executor::block_on(outcome).unwrap();

            assert!(!report.ran_past_grace && report.diagnostics.is_empty());
            assert!(cleaned.load(Ordering::Relaxed));
            assert_eq!(count.load(Ordering::Relaxed), 256);
            assert!(
                observed > 0 && observed < 256,
                "one poll ran {observed} jobs"
            );
            assert!(first.is_pending());
            assert!(wakes > 0);
        }
    }
}
