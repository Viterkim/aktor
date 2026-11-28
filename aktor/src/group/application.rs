use super::shutdown::{contain_drop, fail_application};
use super::*;
use crate::AktorGroupError;
use er::*;
use futures_util::{
    FutureExt,
    future::{AbortHandle, Abortable},
};
use std::panic::AssertUnwindSafe;
use tokio::sync::oneshot;

pub(super) struct Application {
    pub after: usize,
    abort: AbortHandle,
    done: oneshot::Receiver<()>,
    policy: crate::AktorJob,
}
impl Application {
    pub(super) async fn stop(&mut self, kill: &AktorKillSwitch, grace: Duration) {
        if self.policy == crate::AktorJob::DropAtStop {
            self.abort.abort();
        }

        let until = shutdown_deadline(Instant::now(), grace);
        if kill.bounded(until, &mut self.done).await.is_some() {
            return;
        }

        kill.control
            .lock()
            .report
            .late(crate::AktorLateWork::Application);
        if self.policy == crate::AktorJob::FinishBeforeEnd {
            self.cut(kill);
            let until = shutdown_deadline(Instant::now(), grace);
            kill.bounded(until, &mut self.done).await;
        }
    }

    pub(super) fn cut(&mut self, kill: &AktorKillSwitch) {
        if self.policy == crate::AktorJob::FinishBeforeEnd
            && self.done.try_recv() == Err(oneshot::error::TryRecvError::Empty)
        {
            self.policy = crate::AktorJob::DropAtStop;
            kill.fail(AktorFailureInput::new(
                crate::AktorFailureSource::ApplicationTask,
                er_new!(crate::report::ApplicationCut).er_report(),
            ));
        }
        self.abort.abort();
    }
}
impl Drop for Application {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl AktorGroup {
    /// Register work at this position in the group’s shutdown order.
    pub fn spawn_task<O: crate::AktorJobOutput<E>, E>(
        &self,
        future: impl Future<Output = O> + Send + 'static,
        policy: crate::AktorJob,
    ) -> Result<(), (AktorGroupError, String)> {
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(error) => {
                contain_drop(
                    future,
                    &self.killswitch(),
                    crate::AktorFailureSource::ApplicationTask,
                );
                er_bail!(AktorGroupError::NoTokio, error);
            }
        };
        let mut state = self.control.lock();

        if let Err(error) = state.check_registration() {
            drop(state);
            contain_drop(
                future,
                &self.killswitch(),
                crate::AktorFailureSource::ApplicationTask,
            );
            return Err(error.into());
        }

        let (abort, registration) = AbortHandle::new_pair();
        let (finished, done) = oneshot::channel();

        // Sweeping on every spawn costs a full scan each time while thousands stay alive.
        if state.callers.len() >= state.prune_callers_at {
            state.callers.retain_mut(|task| {
                task.done.try_recv() == Err(oneshot::error::TryRecvError::Empty)
            });
            state.prune_callers_at = state.callers.len().saturating_mul(2).max(64);
        }

        let after = state.registered;
        state.callers.push(Application {
            after,
            done,
            abort,
            policy,
        });
        state.applications += 1;
        drop(state);

        let caller = Caller {
            future: Some(Box::pin(async move { future.await.into_job().map(drop) })),
            kill: self.killswitch(),
            _finished: finished,
        };
        runtime.spawn(Abortable::new(caller.run(), registration));
        Ok(())
    }
}

type TaskFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
struct Caller {
    future: Option<TaskFuture>,
    kill: AktorKillSwitch,
    _finished: oneshot::Sender<()>,
}
impl Caller {
    async fn run(mut self) {
        if let Some(future) = self.future.as_mut() {
            match AssertUnwindSafe(future).catch_unwind().await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.kill.fail(AktorFailureInput::new(
                        crate::AktorFailureSource::ApplicationTask,
                        error,
                    ));
                }
                Err(payload) => {
                    fail_application(&self.kill, crate::panic::panic_tree(&payload));
                    contain_drop(
                        payload,
                        &self.kill,
                        crate::AktorFailureSource::ApplicationTask,
                    );
                }
            }
        }
        contain_drop(
            self.future.take(),
            &self.kill,
            crate::AktorFailureSource::ApplicationTask,
        );
    }
}
impl Drop for Caller {
    fn drop(&mut self) {
        if self.future.is_some() && !self.kill.is_stopping() {
            fail_application(&self.kill, TaskCancelled.into());
        }
        contain_drop(
            self.future.take(),
            &self.kill,
            crate::AktorFailureSource::ApplicationTask,
        );
        self.kill.control.lock().applications -= 1;
        self.kill.control.application_changed.send_replace(());
        super::native::poke();
    }
}

// Dropped without the group stopping, a stop cancelling it is the plan working.
#[derive(Er)]
#[er(format = "application task cancelled before completion")]
struct TaskCancelled;
