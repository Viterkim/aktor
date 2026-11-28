use super::*;
use crate::data::AktorData;
use crate::message::AktorDeliveryOutcome;
use crate::{
    AktorGroup, OwnerOutcome,
    worker::{
        OptionalWorker, Options, WireFailure, Worker, WorkerJoin, WorkerStartError,
        browser::ReadyFailure,
    },
};
use alloc::rc::Rc;
use core::cell::Cell;

pub struct Required;
pub struct Optional;

pub struct AktorWorkerNew<C: crate::AktorWorkerContract, A = Required> {
    pub name: String,
    pub kind: kind::BrowserWebWorker<C>,
    pub config: C::Config,
    pub availability: A,
    pub options: Options,
    pub before_shutdown: Option<WorkerJoin>,
    pub stuck: crate::AktorStuck,
}

impl<C: crate::AktorWorkerContract> AktorStart for AktorWorkerNew<C> {
    type Group = AktorGroup;
    type Handles = Worker<C>;
    type Error = C::Error;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.start().er_with(|err| err.start_reason())?;

        Ok(())
    }

    fn closed(name: &str, group: &AktorGroup) -> Self::Handles {
        Worker::closed(never_launched(name), name, group.killswitch(), None)
    }

    fn fallback(&self) -> impl FnOnce(&AktorGroup) -> Self::Handles + use<C> {
        let name = self.name.clone();
        let dependency = self.before_shutdown.as_ref().map(WorkerJoin::completion);
        move |group| Worker::closed(never_launched(&name), &name, group.killswitch(), dependency)
    }

    fn launch(self, mut context: AktorStartContext<AktorGroup>) -> AktorLaunch<Self> {
        let setup = self;
        let external_owner = setup.before_shutdown.is_some();
        let joining = setup.before_shutdown;
        let dependency = joining.as_ref().map(WorkerJoin::completion);

        let worker = open_worker(
            (&setup.name, context.group.killswitch()),
            &setup.kind,
            setup.options,
            &setup.config,
            joining,
            setup.stuck,
            false,
        );

        let worker = match worker {
            Ok(worker) => worker,
            Err(mut failure) => {
                context
                    .group
                    .validate_attempt(&setup.name, AktorExecution::BrowserWebWorker);
                let kill = context.group.killswitch();
                let id = record_open(&kill, &setup.name, &mut failure);
                let error = start_error(failure.clone().report(WireFailure::without_data));
                let worker = Worker::closed(failure, &setup.name, kill.clone(), dependency);
                if external_owner {
                    register_unavailable(worker.completion(), setup.name, &mut context)
                        .er_with(|err| LaunchError::new(err.reason))?;
                }

                kill.stop_startup(id);
                return Ok((worker, Box::pin(async move { Err(error) })));
            }
        };
        register_worker(&worker, setup.name, setup.stuck, context)
            .er_with(|err| LaunchError::new(err.reason))?;

        let waiting = worker.clone();
        let ready: crate::message::LocalFuture<'static, _> = Box::pin(async move {
            match waiting.observe_ready().await {
                Ok(()) => Ok(()),
                Err(failure) => Err(start_error(failure)),
            }
        });

        Ok((worker, ready))
    }
}

impl<C: crate::AktorWorkerContract> AktorStart for AktorWorkerNew<C, Optional> {
    type Group = AktorGroup;
    type Handles = OptionalWorker<C>;
    type Error = core::convert::Infallible;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.start().er_with(|err| err.start_reason())?;

        Ok(())
    }

    fn closed(name: &str, group: &AktorGroup) -> Self::Handles {
        OptionalWorker::from_worker(AktorWorkerNew::<C>::closed(name, group))
    }

    fn fallback(&self) -> impl FnOnce(&AktorGroup) -> Self::Handles + use<C> {
        let name = self.name.clone();
        let dependency = self.before_shutdown.as_ref().map(WorkerJoin::completion);
        move |group| {
            OptionalWorker::from_worker(Worker::closed(
                never_launched(&name),
                &name,
                group.killswitch(),
                dependency,
            ))
        }
    }

    /// An optional worker that can't open is just unavailable, its start doesn't fail.
    fn launch(self, mut context: AktorStartContext<AktorGroup>) -> AktorLaunch<Self> {
        let setup = self;
        let external_owner = setup.before_shutdown.is_some();
        let joining = setup.before_shutdown;
        let dependency = joining.as_ref().map(WorkerJoin::completion);

        let opened = open_worker(
            (&setup.name, context.group.killswitch()),
            &setup.kind,
            setup.options,
            &setup.config,
            joining,
            setup.stuck,
            true,
        );

        let worker = match opened {
            Ok(worker) => {
                register_worker(&worker, setup.name, setup.stuck, context)
                    .er_with(|err| LaunchError::new(err.reason))?;
                OptionalWorker::from_worker(worker)
            }
            Err(mut failure) => {
                context
                    .group
                    .validate_attempt(&setup.name, AktorExecution::BrowserWebWorker);
                record_open(&context.group.killswitch(), &setup.name, &mut failure);
                let worker = OptionalWorker::unavailable(
                    failure,
                    setup.name.clone(),
                    context.group.killswitch(),
                    dependency,
                );

                if external_owner {
                    register_unavailable(worker.completion(), setup.name, &mut context)
                        .er_with(|err| LaunchError::new(err.reason))?;
                }
                worker
            }
        };

        let ready: crate::message::LocalFuture<'static, _> = Box::pin(async { Ok(()) });

        Ok((worker, ready))
    }
}

pub(crate) fn record_open(
    kill: &crate::AktorKillSwitch,
    name: &str,
    failure: &mut ReadyFailure,
) -> crate::FailureId {
    let id = kill.observe_failure(crate::AktorFailureInput::actor(
        name.into(),
        AktorExecution::BrowserWebWorker,
        crate::AktorPhase::Worker(crate::AktorWorkerPhase::Setup),
        failure.error.report.clone(),
    ));
    failure.error.error.failure_id = Some(id);
    id
}

fn open_worker<C: crate::AktorWorkerContract>(
    owner: (&str, crate::AktorKillSwitch),
    kind: &kind::BrowserWebWorker<C>,
    options: Options,
    config: &C::Config,
    joining: Option<WorkerJoin>,
    stuck: crate::AktorStuck,
    optional: bool,
) -> Result<Worker<C>, ReadyFailure> {
    let worker = match Worker::configured(&kind.program, options, config, optional, owner) {
        Ok(worker) => worker.retain_unconfirmed(stuck == crate::AktorStuck::Leave),
        Err(failure) => return Err(ReadyFailure::not_run(failure.into())),
    };

    if let Some(joining) = joining {
        joining.attach(&worker);
    }
    Ok(worker)
}

fn never_launched(name: &str) -> ReadyFailure {
    #[derive(Er)]
    #[er(format = "{name} never launched")]
    struct NeverLaunched {
        name: String,
    }

    ReadyFailure::not_run(WireFailure::setup(
        AktorDeliveryOutcome::NotAdmitted,
        er_new!(NeverLaunched { name: name.into() }).er_report(),
    ))
}

fn start_error<E: 'static>((error, report): (WorkerStartError<E>, String)) -> StartFailure<E> {
    let (init, failure) = match error {
        WorkerStartError::Initializer { error, failure } => (Some(error), failure),
        WorkerStartError::Refused(failure) | WorkerStartError::Failed(failure) => (None, failure),
    };
    let top = LaunchError {
        init,
        failure_id: failure.failure_id,
        ..LaunchError::new(failure.reason)
    };

    crate::report::with_report(top, report)
}

#[derive(Er)]
pub(crate) struct RegisterWorkerErr {
    reason: AktorStartReason,
}
fn register_unavailable<E: AktorData + 'static>(
    completion: crate::worker::Completion<E>,
    name: String,
    context: &mut AktorStartContext<AktorGroup>,
) -> ErResult<(), RegisterWorkerErr> {
    let confirming = completion.clone();

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
                OwnerOutcome {
                    retirement: completion.retirement_status().await,
                    init: crate::AktorInitEvidence::NeverEntered,
                    ran_past_grace: false,
                }
            }),
        )
        .er_with(|err| err.start_reason())
}

fn register_worker<C: crate::AktorWorkerContract>(
    worker: &Worker<C>,
    name: String,
    stuck: crate::AktorStuck,
    mut context: AktorStartContext<AktorGroup>,
) -> ErResult<(), RegisterWorkerErr> {
    let control = worker.control();
    let shutdown = control.clone();
    let cancel = control.clone();
    let owner = control;
    let confirming = worker.completion();
    let timed_out = Rc::new(Cell::new(false));
    let forced = timed_out.clone();

    if let Err(error) = context
        .group
        .register_owner(
            name,
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
            None,
            Box::pin(async move {
                let retirement = confirming.await_retirement().await;

                OwnerOutcome {
                    retirement,
                    init: owner.init(),
                    ran_past_grace: timed_out.get(),
                }
            }),
        )
        .er_with(|error| RegisterWorkerErr::new(error.start_reason()))
    {
        worker.shutdown();
        return Err(error);
    }

    wasm_bindgen_futures::spawn_local(crate::group::keeper(
        worker.clone(),
        context.group.completion(),
    ));
    Ok(())
}

async fn retirement<E>(completion: crate::worker::Completion<E>) -> Option<crate::AktorRetirement> {
    use crate::AktorRetirement::*;

    let retirement = completion.await_retirement().await;

    match retirement {
        Released | Terminated => Some(retirement),
        StillRunning => None,
    }
}
