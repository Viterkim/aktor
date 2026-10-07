use super::*;
use crate::{
    ActorOutcome, AktorGroup,
    worker::{Options, Worker},
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

#[derive(Default)]
pub struct AktorWorkerOptions {
    pub transport: Options,
}

impl<S: 'static, Config: Serialize + 'static, Role: 'static> AktorStart
    for AktorWorkerNew<S, Config, Role>
{
    type Error = AktorStartError;
    type Group = AktorGroup;
    type Handles = Worker<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ())
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        Box::pin(async move {
            let worker = Worker::<S, Role>::with_config(
                &self.kind.program,
                self.options.transport,
                &self.config,
            )
            .map_err(|error| AktorStartError::Setup(AktorSetupError::new(error.to_string())))?;

            let kill = context.group.killswitch();

            worker.manage(self.name.name.clone(), kill);

            let shutdown = worker.new_handle();
            let cancel = worker.new_handle();
            let completion = worker.completion();
            let label = self.name.name.clone();
            let timed_out = Rc::new(Cell::new(false));
            let forced = timed_out.clone();

            if let Err(error) = context.group.register_owner(
                self.name.name,
                AktorExecution::BrowserWebWorker,
                Box::new(move || {
                    shutdown.shutdown();
                }),
                Box::new(move || {
                    forced.set(true);
                    cancel.terminate();
                }),
                Box::pin(async move {
                    let mut report = ActorOutcome {
                        actor: label,
                        kind: Some(AktorExecution::BrowserWebWorker),
                        diagnostics: Vec::new(),
                        timed_out: false,
                    };

                    if let Err(error) = completion.wait_report().await {
                        report.timed_out = timed_out.get();
                        report
                            .diagnostics
                            .push(AktorSetupError::new(error.to_string()));
                    }

                    report
                }),
            ) {
                worker.terminate();
                return Err(AktorStartError::Setup(error));
            }

            worker
                .ready()
                .await
                .map_err(|error| AktorStartError::Init(AktorSetupError::new(error.to_string())))?;
            Ok(worker)
        })
    }
}
