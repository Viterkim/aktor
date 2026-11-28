use super::*;
use crate::{AktorActorInit, AktorStartup};

#[cfg(not(feature = "std"))]
use core::cell::OnceCell as Cell;
#[cfg(feature = "std")]
use std::sync::OnceLock as Cell;

#[cfg(any(
    feature = "tokio",
    feature = "std_thread",
    feature = "browser_local",
    feature = "wasm_browser_workers"
))]
use tokio::sync::Mutex;
#[cfg(not(any(
    feature = "tokio",
    feature = "std_thread",
    feature = "browser_local",
    feature = "wasm_browser_workers"
)))]
mod local;
#[cfg(not(any(
    feature = "tokio",
    feature = "std_thread",
    feature = "browser_local",
    feature = "wasm_browser_workers"
)))]
use local::Mutex;

pub(crate) struct Readiness<F, E> {
    stage: Mutex<Stage<F, E>>,
    receipt: Cell<Result<AktorStartup, (AktorStartError<E>, String)>>,
}

pub(crate) enum Stage<F, E> {
    Starting(F),
    Failed(StartFailure<E>),
    Finished,
}

impl<F, E> From<Stage<F, E>> for Readiness<F, E> {
    fn from(stage: Stage<F, E>) -> Self {
        Self {
            stage: Mutex::new(stage),
            receipt: Cell::new(),
        }
    }
}

impl<F, E: 'static> Readiness<F, E>
where
    F: Future<Output = Result<(), StartFailure<E>>> + Unpin,
{
    pub(super) fn refused(error: (AktorStartError<E>, String)) -> Self {
        Self {
            stage: Mutex::new(Stage::Finished),
            receipt: Cell::from(Err(error)),
        }
    }

    pub(super) async fn into_ready<G: AktorSetupGroup>(
        self,
        group: &G,
    ) -> Result<(), (AktorStartError<E>, String)> {
        self.publish(group).await;
        self.receipt
            .into_inner()
            .expect("startup published")
            .map(drop)
    }

    pub(super) fn split(mut self) -> Result<Self, (AktorStartError<E>, String)> {
        if let Some(receipt) = self.receipt.take() {
            self.receipt = Cell::from(Ok(receipt?));
        }
        Ok(self)
    }

    async fn publish<G: AktorSetupGroup>(&self, group: &G) {
        if self.receipt.get().is_none() {
            let mut stage = self.stage.lock().await;
            if let Stage::Starting(ready) = &mut *stage {
                *stage = match ready.await {
                    Ok(()) => Stage::Finished,
                    Err(error) => Stage::Failed(error),
                };
            }

            if self.receipt.get().is_none() {
                let rollback = if matches!(*stage, Stage::Failed(_)) {
                    G::stop(&group.killswitch());
                    Some(group.completion().await)
                } else {
                    None
                };
                let receipt = match core::mem::replace(&mut *stage, Stage::Finished) {
                    Stage::Finished => Ok(group.startup_receipt()),
                    Stage::Failed(error) => Err(failed(
                        error,
                        group.startup_receipt().actors,
                        rollback.expect("failed starts wait for rollback"),
                    )),
                    Stage::Starting(_) => unreachable!("startup future has returned"),
                };
                let _ = self.receipt.set(receipt);
            }
        }
    }

    pub async fn wait<G: AktorSetupGroup>(
        &self,
        group: &G,
    ) -> Result<&AktorStartup, (&AktorStartError<E>, String)> {
        self.publish(group).await;
        match self.receipt.get().expect("startup published") {
            Ok(receipt) => Ok(receipt),
            Err((error, text)) => Err((error, text.clone())),
        }
    }
}

fn failed<E: 'static>(
    failure: StartFailure<E>,
    actors: Vec<AktorActorInit>,
    report: Result<crate::AktorShutdownReport, crate::AktorPublicationLost>,
) -> (AktorStartError<E>, String) {
    let (failure, text) = failure;
    let id = failure.failure_id;
    let startup = AktorStartRollback {
        actors,
        failure_id: id,
        report: report.map(Box::new),
    };
    let rollback = match &startup.report {
        Ok(report) => {
            if let Some(row) = id.and_then(|id| report.failure(id)) {
                row.delivery.receive();
            }
            report.render(id)
        }
        Err(lost) => er_new!(*lost).er_report(),
    };
    let error = match failure.init {
        Some(error) => AktorStartError::Initializer { error, startup },
        None => AktorStartError::Failed {
            reason: failure.reason,
            startup,
        },
    };

    if rollback.is_empty() {
        (error, text)
    } else {
        crate::report::unlocated(error.er_save("startup", text).er_save("rollback", rollback))
            .into()
    }
}
