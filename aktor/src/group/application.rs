use super::*;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use {
    super::shutdown::{contain_drop, fail_application, panic_message},
    futures_util::FutureExt,
    std::panic::AssertUnwindSafe,
};

impl Drop for ApplicationLife {
    fn drop(&mut self) {
        self.kill.control.lock().applications -= 1;
        self.kill.control.application_changed.send_replace(());
        if self.stop_on_drop {
            self.kill.stop();
        }
    }
}

impl AktorGroup {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    /// Spawn application work that is cancelled and joined when this group stops.
    pub fn spawn_task(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), AktorError> {
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                contain_drop(future, &self.killswitch(), "rejected task drop");
                return Err(AktorError::new("application tasks need a Tokio runtime"));
            }
        };
        let mut state = self.control.lock();

        if !state.listening || state.deadline.is_some() || state.finished {
            drop(state);
            contain_drop(future, &self.killswitch(), "rejected task drop");
            return Err(AktorError::new(
                "actor group cannot accept application work",
            ));
        }

        state.applications += 1;
        let kill = self.killswitch();
        let registration = ApplicationLife {
            kill: kill.clone(),
            stop_on_drop: false,
        };
        drop(state);
        let mut caller = Caller {
            future: Some(Box::pin(future)),
            kill: kill.clone(),
        };
        let task = runtime.spawn(async move {
            if let Some(future) = caller.future.as_mut()
                && let Err(payload) = AssertUnwindSafe(future).catch_unwind().await
            {
                fail_application(&kill, "task", panic_message(&payload));
                contain_drop(payload, &kill, "task panic drop");
            }
            contain_drop(caller.future.take(), &kill, "application task drop");
            drop(caller);
        });
        let mut state = self.control.lock();
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
    kill: KillSwitch,
}
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
impl<F> Drop for Caller<F> {
    fn drop(&mut self) {
        if self.future.is_some() && !self.kill.is_stopping() {
            fail_application(
                &self.kill,
                "task",
                "application task cancelled before completion".into(),
            );
        }
        contain_drop(self.future.take(), &self.kill, "application task drop");
    }
}

impl<F> Drop for OwnedApplication<F> {
    fn drop(&mut self) {
        super::shutdown::contain_drop(self.future.take(), &self.kill, "application drop");
    }
}
impl<F: Future> Future for OwnedApplication<F> {
    type Output = F::Output;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        match self.get_mut().future.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => core::task::Poll::Pending,
        }
    }
}

impl<Owner, Application, Cleanup> Drop for ApplicationFactory<Owner, Application, Cleanup> {
    fn drop(&mut self) {
        super::shutdown::contain_drop(self.application.take(), &self.kill, "application drop");
        super::shutdown::contain_drop(self.cleanup.take(), &self.kill, "cleanup drop");
        super::shutdown::contain_drop(self.owner.take(), &self.kill, "application owner drop");
        drop(self.life.take());
    }
}
