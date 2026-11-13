use super::*;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use crate::AktorGroupError;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use {
    super::shutdown::{contain_drop, fail_application, panic_message},
    futures_util::FutureExt,
    std::panic::AssertUnwindSafe,
};

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
impl Drop for ApplicationLife {
    fn drop(&mut self) {
        self.kill.control.lock().applications -= 1;
        self.kill.control.application_changed.send_replace(());
        self.kill.control.wake.notify_all();
    }
}

impl AktorGroup {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    /// Spawn application work that is cancelled and joined when this group stops.
    pub fn spawn_task(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), AktorGroupError> {
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                contain_drop(future, &self.killswitch(), "rejected task drop");
                return Err(AktorGroupError::NoTokio);
            }
        };
        let mut state = self.control.lock();

        let refused = if state.finished {
            Some(AktorGroupError::Stopped)
        } else if state.deadline.is_some() {
            Some(AktorGroupError::Stopping)
        } else if !state.listening {
            Some(AktorGroupError::NotStarted)
        } else {
            None
        };

        if let Some(error) = refused {
            drop(state);
            contain_drop(future, &self.killswitch(), "rejected task drop");
            return Err(error);
        }

        // Shutdown waits for the handle to be registered and for the task's destructor to return.
        state.applications += 2;
        let kill = self.killswitch();
        let registration = ApplicationLife { kill: kill.clone() };
        drop(state);
        let mut caller = Caller {
            future: Some(Box::pin(future)),
            life: ApplicationLife { kill },
        };
        let task = runtime.spawn(async move {
            if let Some(future) = caller.future.as_mut()
                && let Err(payload) = AssertUnwindSafe(future).catch_unwind().await
            {
                fail_application(&caller.life.kill, "task", panic_message(&payload));
                contain_drop(payload, &caller.life.kill, "task panic drop");
            }
            contain_drop(
                caller.future.take(),
                &caller.life.kill,
                "application task drop",
            );
            drop(caller);
        });
        let mut state = self.control.lock();
        if state.deadline.is_some() {
            drop(state);
            task.abort();
            state = self.control.lock();
        }

        state.callers.retain(|task| !task.is_finished());
        state.callers.push(task);
        drop(state);
        drop(registration);
        Ok(())
    }
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
struct Caller<F> {
    future: Option<Pin<Box<F>>>,
    life: ApplicationLife,
}
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
impl<F> Drop for Caller<F> {
    fn drop(&mut self) {
        if self.future.is_some() && !self.life.kill.is_stopping() {
            fail_application(
                &self.life.kill,
                "task",
                "application task cancelled before completion".into(),
            );
        }
        contain_drop(self.future.take(), &self.life.kill, "application task drop");
    }
}
