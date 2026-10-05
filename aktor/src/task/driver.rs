use super::clock::TaskDeadline;
use super::*;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use crate::setup::kind::TokioTask;
use crate::{
    ActorFailure, ActorOutcome, AktorClosures, AktorError, AktorGroup, AktorSetup, AktorSetupError,
    AktorStartError,
    group::shutdown::{contain_drop, panic_message},
    operation::hooks::AktorHooks,
    setup::kind::AktorMode,
};
use crate::{AktorClosure, operation::Operation};
use core::task::{Context, Poll};
use futures_util::{FutureExt, future::Either};
use hooks::{AktorTaskEnd, AktorTaskInterval};
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::sync::OwnedMutexGuard;

struct TaskLogic<S: 'static, Start> {
    start: Start,
    end: Option<AktorClosure<dyn AktorTaskEnd<S> + Send>>,
    intervals: Vec<TaskInterval<S>>,
    hooks: AktorHooks<S>,
    clock: TaskClock,
}

struct TaskInterval<S: 'static> {
    every: std::time::Duration,
    run: AktorClosure<dyn AktorTaskInterval<S> + Send>,
}

struct Completion {
    report: ActorOutcome,
    kill: KillSwitch,
    completed: Option<oneshot::Sender<ActorOutcome>>,
    finished: watch::Sender<AktorTaskStatus>,
    failed: bool,
}
impl Completion {
    fn fail(&mut self, phase: &str, message: String) {
        self.failed = true;
        self.kill.fail(ActorFailure {
            actor: self.report.actor.clone(),
            kind: self.report.kind,
            phase: phase.into(),
            message,
        });
    }

    fn dispose(&mut self, value: impl Sized, phase: &str) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            let message = panic_message(&payload);

            self.fail(phase, message.clone());
            self.report
                .diagnostics
                .push(AktorError::new(format!("{phase}: {message}")));
            contain_drop(payload, &self.kill, "task panic payload drop");
        }
    }

    fn dispose_hooks<S>(&mut self, hooks: AktorHooks<S>) {
        self.dispose(hooks.before_each, "task hook drop");
        self.dispose(hooks.after_each, "task hook drop");
    }

    fn dispose_intervals<S: 'static>(&mut self, intervals: Vec<TaskInterval<S>>) {
        for interval in intervals {
            self.dispose(interval, "task interval drop");
        }
    }

    fn dispose_receiver<S>(&mut self, mut receiver: mpsc::Receiver<Message<S>>) {
        receiver.close();

        while let Ok(message) = receiver.try_recv() {
            self.dispose(message, "task queue drop");
        }

        self.dispose(receiver, "task queue drop");
    }

    fn publish(&mut self) {
        self.finished
            .send_replace(if self.failed || self.report.timed_out {
                AktorTaskStatus::Failed
            } else {
                AktorTaskStatus::Finished
            });

        if let Some(completed) = self.completed.take() {
            let _sent = completed.send(self.report.clone());
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if self.completed.is_some() {
            self.fail("owner", "actor task cancelled".into());
            self.report.diagnostics.push(AktorError::new(
                "actor task cancelled before cleanup completed",
            ));
            self.publish();
        }
    }
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[doc(hidden)]
pub async fn start<S, Start, Fut>(
    setup: AktorSetup<S, Start, TokioTask>,
    group: &mut AktorGroup,
) -> Result<AktorTask<S>, AktorStartError>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, AktorSetupError>> + Send + 'static,
{
    start_on(setup, group, TaskClock::Tokio, |future| {
        TaskJoin::Tokio(tokio::spawn(future))
    })
    .await
}

#[doc(hidden)]
pub async fn start_on<S, Start, Fut, Kind>(
    setup: AktorSetup<S, Start, Kind>,
    group: &mut AktorGroup,
    clock: TaskClock,
    spawn: impl FnOnce(TaskOwnerFuture) -> TaskJoin,
) -> Result<AktorTask<S>, AktorStartError>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, AktorSetupError>> + Send + 'static,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            End<S> = dyn AktorTaskEnd<S> + Send,
            Interval<S> = dyn AktorTaskInterval<S> + Send,
        >,
{
    let kind = Kind::EXECUTION;
    let options = setup.options.unwrap_or_default();

    if options.capacity == 0 || options.capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(AktorStartError::Setup(AktorError::new(
            "queue capacity must be positive and fit the semaphore",
        )));
    }

    if setup
        .closures
        .intervals
        .iter()
        .any(|interval| interval.every.is_zero())
    {
        return Err(AktorStartError::Setup(AktorError::new(
            "interval duration must be positive",
        )));
    }

    let name = Arc::new(setup.name.name);
    let kill = group.killswitch();
    let (sender, receiver) = mpsc::channel(options.capacity);
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
            Box::pin(async move {
                let mut report = outcome.await.unwrap_or_else(|_| ActorOutcome {
                    actor: label,
                    kind: Some(kind),
                    diagnostics: vec![AktorError::new("actor task observer stopped")],
                    timed_out: false,
                });

                if let Ok(join) = joined.await {
                    join.wait(&observer_kill, &mut report).await;
                }

                report
            }),
        )
        .map_err(AktorStartError::Setup)?;

    let completion = Completion {
        report: ActorOutcome {
            actor: (*name).clone(),
            kind: Some(kind),
            diagnostics: Vec::new(),
            timed_out: false,
        },
        kill: kill.clone(),
        completed: Some(completed),
        failed: false,
        finished,
    };

    #[cfg(not(target_family = "wasm"))]
    let life = kill.track_thread((*name).clone());
    let (ready, started) = oneshot::channel();
    let AktorClosures {
        start,
        end,
        intervals,
        before_each,
        after_each,
    } = setup.closures;

    let logic = TaskLogic {
        start,
        end,
        hooks: AktorHooks {
            before_each: before_each.map(|hook| hook.0),
            after_each: after_each.map(|hook| hook.0),
        },
        clock,
        intervals: intervals
            .into_iter()
            .map(|interval| TaskInterval {
                every: interval.every,
                run: interval.run,
            })
            .collect(),
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

    started
        .await
        .unwrap_or_else(|_| Err(AktorError::new("actor task cancelled during setup")))
        .map_err(AktorStartError::Setup)?;

    if kill.is_stopping() {
        return Err(AktorStartError::Setup(AktorError::new(
            "actor task closed during startup",
        )));
    }

    Ok(AktorTask {
        name,
        sender,
        services,
        kill,
        finished: observed,
        role: PhantomData,
        clock,
    })
}

struct Running<'a, S> {
    job: Pin<Box<dyn Job<S> + Send>>,
    guard: Option<OwnedMutexGuard<Option<S>>>,
    waiting: Option<AktorTaskFuture<'static, OwnedMutexGuard<Option<S>>>>,
    hooks: &'a mut AktorHooks<S>,
    operation: Operation,
}
impl<'a, S: Send + 'static> Running<'a, S> {
    fn new(
        job: Box<dyn Job<S> + Send>,
        state: &Arc<Mutex<Option<S>>>,
        hooks: &'a mut AktorHooks<S>,
        operation: Operation,
    ) -> Self {
        let (guard, waiting) = match state.clone().try_lock_owned() {
            Ok(guard) => (Some(guard), None),
            Err(_) => (
                None,
                Some(Box::pin(state.clone().lock_owned()) as AktorTaskFuture<'static, _>),
            ),
        };

        Self {
            job: Box::into_pin(job),
            guard,
            waiting,
            hooks,
            operation,
        }
    }
}
impl<S: Send + 'static> Future for Running<'_, S> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let guard = if this.guard.is_some() {
            #[cfg(feature = "tokio")]
            core::task::ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
            this.guard.take()
        } else if let Some(waiting) = &mut this.waiting {
            let guard = core::task::ready!(waiting.as_mut().poll(cx));
            this.waiting = None;
            Some(guard)
        } else {
            None
        };

        let state = if let Some(guard) = guard {
            let Ok(guard) = OwnedMutexGuard::try_map(guard, Option::as_mut) else {
                return Poll::Ready(());
            };

            Some(AktorTaskState { guard })
        } else {
            None
        };

        this.job
            .as_mut()
            .poll(cx, state, this.hooks, this.operation)
    }
}

async fn drive<S, Start, Fut>(
    closures: TaskLogic<S, Start>,
    mut receiver: mpsc::Receiver<Message<S>>,
    service_driver: service::ServiceDriver<S>,
    closed: oneshot::Receiver<()>,
    mut forced: oneshot::Receiver<()>,
    ready: oneshot::Sender<Result<(), AktorSetupError>>,
    mut completion: Completion,
) where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, AktorSetupError>> + Send + 'static,
{
    let TaskLogic {
        start,
        end,
        mut intervals,
        mut hooks,
        clock,
    } = closures;

    let mut setup = Box::pin(AssertUnwindSafe(async move { start().await }).catch_unwind());
    let result = tokio::select! {
        biased;
        result = &mut setup => Some(result),
        _ = &mut forced => None,
    };

    match &result {
        Some(Ok(Err(error))) => completion.fail("setup", error.to_string()),
        Some(Err(payload)) => completion.fail("setup", panic_message(payload)),
        _ => {}
    }

    completion.dispose(setup, "task setup drop");

    let state = match result {
        Some(Ok(Ok(state))) => state,
        Some(Ok(Err(error))) => {
            completion.fail("setup", error.to_string());

            let _sent = ready.send(Err(error));

            completion.dispose_hooks(hooks);
            completion.dispose_intervals(intervals);
            completion.dispose(end, "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
        Some(Err(payload)) => {
            let message = panic_message(&payload);

            completion.fail("setup", message.clone());
            completion.dispose(payload, "task setup panic drop");

            let _sent = ready.send(Err(AktorError::new(message)));

            completion.dispose_hooks(hooks);
            completion.dispose_intervals(intervals);
            completion.dispose(end, "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
        None => {
            completion.report.timed_out = true;

            let _sent = ready.send(Err(AktorError::new("actor task setup cancelled")));

            completion.dispose_hooks(hooks);
            completion.dispose_intervals(intervals);
            completion.dispose(end, "task end drop");
            completion.dispose_receiver(receiver);
            completion.publish();
            return;
        }
    };

    let state = Arc::new(Mutex::new(Some(state)));
    let _sent = ready.send(Ok(()));
    // Keep their wake registrations between calls.
    let closing = async move {
        let _closed = closed.await;
    };
    let forcing = async move {
        let _forced = forced.await;
    };

    tokio::pin!(closing, forcing);

    let mut stopping = false;
    let mut ordinary_done = false;
    let mut services_done = false;
    let services = service_driver.services.clone();
    let service = services.next();

    tokio::pin!(service);

    let mut next: Vec<_> = intervals
        .iter()
        .map(|interval| clock.deadline(interval.every))
        .collect();

    loop {
        if ordinary_done && services_done {
            break;
        }

        if completion.kill.is_stopping() && !stopping {
            stopping = true;
            receiver.close();
            services.close();
        }

        let due = next
            .iter()
            .enumerate()
            .filter_map(|(index, deadline)| deadline.map(|deadline| (index, deadline)))
            .min_by_key(|(_, deadline)| *deadline);

        let work = tokio::select! {
            _ = &mut closing, if !stopping => {
                stopping = true;
                receiver.close();
                services.close();
                continue;
            },
            _ = &mut forcing => { completion.report.timed_out = true; break; },
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
            _ = wait_interval(due), if !stopping && due.is_some() => None,
        };

        let operation = work.as_ref().map_or(
            Operation {
                name: "interval",
                caller: std::panic::Location::caller(),
            },
            |work| work.operation,
        );

        let run = match work {
            Some(message) => Either::Left(Running::new(message.job, &state, &mut hooks, operation)),
            None => {
                let Some((index, _)) = due else {
                    continue;
                };
                let state = state.clone();
                let interval = &mut intervals[index];
                let hooks = &mut hooks;
                let next = &mut next[index];

                Either::Right(Box::pin(async move {
                    let guard = state.clone().lock_owned().await;

                    if let Ok(guard) = OwnedMutexGuard::try_map(guard, Option::as_mut) {
                        let mut lease = AktorTaskState { guard };

                        hooks.before(&mut lease, operation);
                        interval.run.0.run(lease).await;

                        let mut state = state.lock().await;

                        if let Some(state) = &mut *state {
                            hooks.after(state, operation);
                        }
                    }

                    *next = clock.deadline(interval.every);
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
            completion.fail(operation.name, panic_message(payload));
        }

        completion.dispose(run, "task operation drop");

        match outcome {
            Some(Ok(())) => {}
            Some(Err(payload)) => {
                completion.fail(operation.name, panic_message(&payload));
                completion.dispose(payload, "task operation panic drop");
                break;
            }
            None => {
                completion.report.timed_out = true;
                break;
            }
        }
    }

    receiver.close();

    for message in service_driver.services.discard() {
        completion.dispose(message, "task latest queue drop");
    }

    completion.dispose(service_driver, "task latest driver drop");
    completion.dispose_receiver(receiver);

    let state = state.lock().await.take();

    if let Some(state) = state {
        let mut cleanup = Box::pin(
            AssertUnwindSafe(async move {
                if let Some(end) = end {
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
            result = &mut cleanup => Some(result),
            _ = clock.wait_for_force(&completion.kill) => None,
        };

        match &result {
            Some(Ok(Err(error))) => completion.fail("cleanup", error.to_string()),
            Some(Err(payload)) => completion.fail("cleanup", panic_message(payload)),
            _ => {}
        }

        completion.dispose(cleanup, "task cleanup drop");

        match result {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(error))) => {
                completion.fail("cleanup", error.to_string());
                completion.report.diagnostics.push(error);
            }
            Some(Err(payload)) => {
                let message = panic_message(&payload);

                completion.fail("cleanup", message.clone());
                completion.report.diagnostics.push(AktorError::new(message));
                completion.dispose(payload, "task cleanup panic drop");
            }
            None => {
                completion.report.timed_out = true;
                completion
                    .report
                    .diagnostics
                    .push(AktorError::new("actor task cleanup cancelled"));
            }
        }
    }

    completion.dispose_hooks(hooks);
    completion.dispose_intervals(intervals);
    completion.publish();
}

async fn wait_interval(due: Option<(usize, TaskDeadline)>) {
    if let Some((_, deadline)) = due {
        deadline.wait().await;
    } else {
        core::future::pending::<()>().await;
    }
}
