use super::*;
use crate::{
    ActorOutcome, AktorGroup,
    worker::{Options, Worker, WorkerCause},
};
use serde::Serialize;

pub struct AktorWorkerSetup<S, Config, Role = AktorNoRole> {
    pub name: AktorName,
    pub role: Role,
    pub kind: kind::BrowserWebWorker<S>,
    pub config: Config,
    pub options: Option<AktorWorkerOptions>,
}
impl<S, Config, Role> AktorWorkerSetup<S, Config, Role> {
    pub async fn start_in(
        self,
        group: &<Self as AktorStart>::Group,
    ) -> Result<<Self as AktorStart>::Handles, AktorStartupError<<Self as AktorStart>::Error>>
    where
        Self: AktorStart,
    {
        start_in(group, self).await
    }

    pub fn start(self) -> AktorStartup<Self>
    where
        Self: AktorStart,
    {
        start(self)
    }
}

pub struct AktorWorkerOptions {
    pub shutdown_grace: Duration,
    pub transport: Options,
}
impl Default for AktorWorkerOptions {
    fn default() -> Self {
        Self {
            shutdown_grace: Duration::from_secs(5),
            transport: Options::default(),
        }
    }
}

impl<S: 'static, Config: Serialize + 'static, Role: 'static> AktorStart
    for AktorWorkerSetup<S, Config, Role>
{
    type Error = AktorStartError;
    type Group = AktorGroup;
    type Handles = Worker<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ())
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        Box::pin(async move {
            let worker = Worker::<S, Role>::with_config(
                &self.kind.program,
                self.options.unwrap_or_default().transport,
                &self.config,
            )
            .map_err(|error| AktorStartError::Setup(AktorSetupError::new(error.to_string())))?;

            let kill = context.group.killswitch();

            worker.manage(self.name.name.clone(), kill);

            let shutdown = worker.new_handle();
            let cancel = worker.new_handle();
            let completion = worker.completion();
            let label = self.name.name.clone();

            if let Err(error) = context.group.register_owner(
                self.name.name,
                AktorExecution::BrowserWebWorker,
                Box::new(move || {
                    shutdown.shutdown();
                }),
                Box::new(move || cancel.terminate()),
                Box::pin(async move {
                    let mut report = ActorOutcome {
                        actor: label,
                        kind: Some(AktorExecution::BrowserWebWorker),
                        diagnostics: Vec::new(),
                        timed_out: false,
                    };

                    if let Err(error) = completion.wait_report().await {
                        report.timed_out = error.cause == WorkerCause::Closed;
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
                .map_err(|error| AktorStartError::Setup(AktorSetupError::new(error.to_string())))?;
            Ok(worker)
        })
    }
}
