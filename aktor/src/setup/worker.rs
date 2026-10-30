use super::*;
use crate::{
    AktorGroup, AktorOutcome,
    worker::{OptionalWorker, Options, ShutdownDependency, Worker},
};
use alloc::rc::Rc;
use core::cell::Cell;
use serde::Serialize;

pub struct AktorWorkerNew<S, Config, Role = AktorNoRole> {
    pub name: AktorName,
    pub role: Role,
    pub kind: kind::BrowserWebWorker<S>,
    pub config: Config,
    pub options: AktorWorkerOptions,
}
impl<S, Config, Role> Drop for AktorWorkerNew<S, Config, Role> {
    fn drop(&mut self) {
        if let Some(joining) = self.options.before_shutdown.take() {
            // A rejected or cancelled setup may never reach start_in.
            drop(ShutdownDependency::new(Some(joining)));
        }
    }
}

/// An optional worker remains owned by its group, while calls report failures locally.
pub struct AktorOptionalWorkerNew<S, Config, Role = AktorNoRole>(
    pub AktorWorkerNew<S, Config, Role>,
);

impl<S, Config, Role> AktorWorkerNew<S, Config, Role> {
    pub fn optional(self) -> AktorOptionalWorkerNew<S, Config, Role> {
        AktorOptionalWorkerNew(self)
    }
}

#[derive(Default)]
pub struct AktorWorkerOptions {
    pub transport: Options,
    /// Join external work before normal or forced worker shutdown.
    /// Returning, including Err, confirms that dependency has physically finished.
    /// Keep its resources owned here until its actual retirement receipt arrives.
    pub before_shutdown:
        Option<crate::message::LocalFuture<'static, Result<(), crate::AktorError>>>,
}

impl<S: 'static, Config: Serialize + 'static, Role: 'static> AktorStart
    for AktorWorkerNew<S, Config, Role>
{
    type Group = AktorGroup;
    type Handles = Worker<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ()).map_err(Into::into)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        start_worker(self, context)
    }
}

impl<S: 'static, Config: Serialize + 'static, Role: 'static> AktorStart
    for AktorOptionalWorkerNew<S, Config, Role>
{
    type Group = AktorGroup;
    type Handles = OptionalWorker<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ()).map_err(Into::into)
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let mut setup = self.0;
        let joining = ShutdownDependency::new(setup.options.before_shutdown.take());

        Box::pin(async move {
            let dependency = joining.completion();
            match OptionalWorker::<S, Role>::with_config(
                &setup.kind.program,
                core::mem::take(&mut setup.options.transport),
                &setup.config,
            )
            .and_then(OptionalWorker::into_worker)
            .and_then(|worker| joining.attach(worker))
            {
                Ok(worker) => {
                    register_worker(&worker, core::mem::take(&mut setup.name.name), context)?;
                    Ok(OptionalWorker::from_worker(worker))
                }
                Err(error) => {
                    let worker = OptionalWorker::unavailable(error, dependency);
                    register_unavailable(
                        &worker,
                        core::mem::take(&mut setup.name.name),
                        &mut context,
                    )?;
                    Ok(worker)
                }
            }
        })
    }
}

fn start_worker<S: 'static, Config: Serialize + 'static, Role: 'static>(
    mut setup: AktorWorkerNew<S, Config, Role>,
    mut context: AktorStartContext<AktorGroup>,
) -> crate::message::LocalFuture<'static, Result<Worker<S, Role>, AktorStartError>> {
    let joining = ShutdownDependency::new(setup.options.before_shutdown.take());

    Box::pin(async move {
        let dependency = joining.completion();
        let worker = Worker::<S, Role>::with_config(
            &setup.kind.program,
            core::mem::take(&mut setup.options.transport),
            &setup.config,
        )
        .and_then(|worker| joining.attach(worker));
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                let message = alloc::format!("{error:#}");
                let worker = OptionalWorker::<S, Role>::unavailable(error, dependency);

                register_unavailable(&worker, setup.name.name.clone(), &mut context)?;
                return Err(validation_error(
                    &context.group,
                    &setup.name.name,
                    AktorExecution::BrowserWebWorker,
                    AktorStartError::Setup(AktorSetupError::new(message, ())),
                ));
            }
        };

        register_worker(&worker, core::mem::take(&mut setup.name.name), context)?;

        worker.ready().await.map_err(|error| {
            AktorStartError::Init(crate::AktorFailure::from(AktorSetupError::new(
                alloc::format!("{error:#}"),
                (),
            )))
        })?;
        Ok(worker)
    })
}

fn register_unavailable<S: 'static, Role: 'static>(
    worker: &OptionalWorker<S, Role>,
    name: String,
    context: &mut AktorStartContext<AktorGroup>,
) -> Result<(), AktorStartError> {
    let completion = worker.completion();
    let label = name.clone();

    context
        .group
        .register_owner(
            name,
            AktorExecution::BrowserWebWorker,
            Box::new(|| {}),
            Box::new(|| {}),
            Box::pin(async move {
                let retirement = match completion.wait().await {
                    crate::worker::Retirement::Confirmed(_) => crate::RetirementStatus::Confirmed,
                    crate::worker::Retirement::Unconfirmed(_) => {
                        crate::RetirementStatus::Unconfirmed
                    }
                };

                AktorOutcome {
                    retirement,
                    actor: label,
                    kind: Some(AktorExecution::BrowserWebWorker),
                    diagnostics: completion.wait_diagnostics().await,
                    timed_out: false,
                }
            }),
        )
        .map_err(AktorStartError::Setup)
}

fn register_worker<S: 'static, Role: 'static>(
    worker: &Worker<S, Role>,
    name: String,
    mut context: AktorStartContext<AktorGroup>,
) -> Result<(), AktorStartError> {
    let kill = context.group.killswitch();

    worker.manage(name.clone(), kill);

    let shutdown = worker.new_handle();
    let cancel = worker.new_handle();
    let completion = worker.completion();
    let label = name.clone();
    let timed_out = Rc::new(Cell::new(false));
    let forced = timed_out.clone();

    if let Err(error) = context.group.register_owner(
        name,
        AktorExecution::BrowserWebWorker,
        Box::new(move || {
            shutdown.shutdown();
        }),
        Box::new(move || {
            forced.set(true);
            cancel.terminate();
        }),
        Box::pin(async move {
            let retirement = match completion.wait().await {
                crate::worker::Retirement::Confirmed(_) => crate::RetirementStatus::Confirmed,
                crate::worker::Retirement::Unconfirmed(_) => crate::RetirementStatus::Unconfirmed,
            };
            let diagnostics = completion.wait_diagnostics().await;

            AktorOutcome {
                retirement,
                actor: label,
                kind: Some(AktorExecution::BrowserWebWorker),
                diagnostics,
                timed_out: timed_out.get(),
            }
        }),
    ) {
        worker.terminate();
        return Err(AktorStartError::Setup(error));
    }

    Ok(())
}
