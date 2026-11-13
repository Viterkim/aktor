use super::*;
use crate::{
    AktorGroup, AktorOutcome,
    worker::{
        OptionalWorker, Options, Worker, WorkerFailure, WorkerJoin, WorkerStartError,
        browser::ReadyFailure,
    },
};
use alloc::rc::Rc;
use core::cell::Cell;
use serde::{Serialize, de::DeserializeOwned};

pub struct AktorWorkerNew<S, Config, E = ()> {
    pub name: String,
    pub kind: kind::BrowserWebWorker<S, E>,
    pub config: Config,
    pub options: AktorWorkerOptions,
    /// `Leave` keeps accepted work running when shutdown gives up on it, so cleanup can finish.
    pub stuck: crate::AktorStuck,
}

/// The group still owns an optional worker. Its calls return delivery errors to the caller.
pub struct AktorOptionalWorkerNew<S, Config, E = ()>(pub AktorWorkerNew<S, Config, E>);

impl<S, Config, E> AktorWorkerNew<S, Config, E> {
    pub fn optional(self) -> AktorOptionalWorkerNew<S, Config, E> {
        AktorOptionalWorkerNew(self)
    }
}

#[derive(Default)]
pub struct AktorWorkerOptions {
    pub transport: Options,
    /// Use `Some(worker::join(future))` when external work must finish before the worker stops.
    /// A returned error confirms the work stopped and keeps the diagnostic in the group report.
    pub before_shutdown: Option<WorkerJoin>,
}

impl<S: 'static, Config: Serialize + 'static, E: DeserializeOwned + 'static> AktorStart
    for AktorWorkerNew<S, Config, E>
{
    type Group = AktorGroup;
    type Handles = Worker<S, (), E>;
    type Error = E;
    type Startup =
        crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        start_worker(self, context)
    }
}

impl<S: 'static, Config: Serialize + 'static, E: DeserializeOwned + 'static> AktorStart
    for AktorOptionalWorkerNew<S, Config, E>
{
    type Group = AktorGroup;
    type Handles = OptionalWorker<S, (), E>;
    type Error = core::convert::Infallible;
    type Startup =
        crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self.0;
        let joining = setup.options.before_shutdown.unwrap_or_default();

        Box::pin(async move {
            let dependency = joining.completion();
            match Worker::<S, (), E>::configured(
                &setup.kind.program,
                setup.options.transport,
                &setup.config,
                true,
            )
            .map_err(ReadyFailure::not_run)
            .and_then(|worker| {
                joining
                    .attach(worker.retain_unconfirmed(setup.stuck == crate::AktorStuck::Leave))
                    .map_err(ReadyFailure::from)
            }) {
                Ok(worker) => {
                    register_worker(&worker, setup.name, setup.stuck, context)?;
                    Ok(OptionalWorker::from_worker(worker))
                }
                Err(failure) => {
                    let worker = OptionalWorker::unavailable(failure, dependency);

                    register_unavailable(&worker, setup.name, &mut context)?;
                    Ok(worker)
                }
            }
        })
    }
}

fn start_worker<S: 'static, Config: Serialize + 'static, E: DeserializeOwned + 'static>(
    setup: AktorWorkerNew<S, Config, E>,
    mut context: AktorStartContext<AktorGroup>,
) -> crate::message::LocalFuture<'static, Result<Worker<S, (), E>, AktorStartError<E>>> {
    let joining = setup.options.before_shutdown.unwrap_or_default();

    Box::pin(async move {
        let dependency = joining.completion();
        let worker = Worker::<S, (), E>::configured(
            &setup.kind.program,
            setup.options.transport,
            &setup.config,
            false,
        )
        .map_err(ReadyFailure::not_run)
        .and_then(|worker| {
            joining
                .attach(worker.retain_unconfirmed(setup.stuck == crate::AktorStuck::Leave))
                .map_err(ReadyFailure::from)
        });
        let worker = match worker {
            Ok(worker) => worker,
            Err(failure) => {
                let mut error =
                    start_error(failure.clone().report(WorkerFailure::report_without_data));
                let worker = OptionalWorker::<S, (), E>::unavailable(failure, dependency);

                error.cause = match register_unavailable(&worker, setup.name.clone(), &mut context)
                {
                    Ok(()) => validation_error(
                        &context.group,
                        &setup.name,
                        AktorExecution::BrowserWebWorker,
                        error.cause,
                    ),
                    Err(cause) => cause,
                };
                return Err(error);
            }
        };

        register_worker(&worker, setup.name, setup.stuck, context)?;

        worker.ready().await.map_err(start_error)?;
        Ok(worker)
    })
}

fn start_error<E>((error, report): (WorkerStartError<E>, String)) -> AktorStartError<E> {
    let cause = crate::worker::WorkerError {
        outcome: error.outcome,
        cause: error.cause,
        data: None,
    };

    AktorStartError {
        init: error.init,
        cause: AktorStartCause::Worker(cause, report),
        report: None,
    }
}

fn register_unavailable<S: 'static, Role: 'static, E: DeserializeOwned + 'static>(
    worker: &OptionalWorker<S, Role, E>,
    name: String,
    context: &mut AktorStartContext<AktorGroup>,
) -> Result<(), AktorStartCause> {
    worker.manage_unavailable(context.group.killswitch());

    let completion = worker.completion();
    let confirming = completion.clone();
    let label = name.clone();

    context
        .group
        .register_owner(
            name,
            AktorExecution::BrowserWebWorker,
            Box::new(|| {}),
            Box::new(|| {}),
            None,
            Some(Box::pin(retirement(confirming))),
            Box::pin(async move {
                AktorOutcome {
                    retirement: completion.retirement_status().await,
                    actor: label,
                    kind: AktorExecution::BrowserWebWorker,
                    diagnostics: completion.wait_diagnostics().await,
                    ran_past_grace: false,
                }
            }),
        )
        .map_err(AktorStartCause::Group)
}

fn register_worker<S: 'static, Role: 'static, E: DeserializeOwned + 'static>(
    worker: &Worker<S, Role, E>,
    name: String,
    stuck: crate::AktorStuck,
    mut context: AktorStartContext<AktorGroup>,
) -> Result<(), AktorStartCause> {
    let kill = context.group.killswitch();

    let shutdown = worker.clone();
    let cancel = worker.clone();
    let owner = worker.clone();
    let confirming = worker.completion();
    let killing = stuck == crate::AktorStuck::Kill;
    let label = name.clone();
    let timed_out = Rc::new(Cell::new(false));
    let forced = timed_out.clone();

    if let Err(error) = context.group.register_owner(
        name.clone(),
        AktorExecution::BrowserWebWorker,
        Box::new(move || {
            shutdown.shutdown();
        }),
        // A worker's `end` runs inside it, terminating early would cut it off.
        Box::new(|| {}),
        Some(Box::new(move || {
            forced.set(true);
            cancel.give_up(stuck)
        })),
        // A left worker's retained work can still confirm after the report went out.
        (!killing).then(|| Box::pin(retirement(confirming)) as Pin<Box<dyn Future<Output = _>>>),
        Box::pin(async move {
            let (retirement, diagnostics) = owner.group_outcome(stuck).await;

            AktorOutcome {
                retirement,
                actor: label,
                kind: AktorExecution::BrowserWebWorker,
                diagnostics,
                ran_past_grace: timed_out.get(),
            }
        }),
    ) {
        worker.shutdown();
        return Err(AktorStartCause::Group(error));
    }

    worker.manage(name, kill);
    Ok(())
}

async fn retirement<E>(
    completion: crate::worker::Completion<E>,
) -> Option<(crate::AktorRetireStatus, Vec<AktorError>)> {
    use crate::AktorRetireStatus::*;

    let retirement = completion.await_retirement().await;

    match retirement {
        Confirmed | Terminated => Some((retirement, completion.wait_diagnostics().await)),
        Unconfirmed => None,
    }
}
