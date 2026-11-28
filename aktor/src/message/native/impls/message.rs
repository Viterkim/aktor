use super::super::*;
use futures_util::FutureExt;

impl<S> crate::queue::Service for Message<S> {
    fn service_id(&self) -> Option<*const ()> {
        self.job.service_id()
    }
}

#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    feature = "bevy"
))]
impl<S> crate::serve::Mail<S> for Message<S> {
    type State = S;

    fn operation(&self) -> crate::operation::Operation {
        self.operation
    }

    async fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut crate::listener::hooks::CallHooks<S>,
    ) {
        self.job.run(state, hooks, self.operation).await;
        self.finished = true;
    }
}

impl<S> Message<S> {
    pub async fn run(self, state: &mut S) {
        self.run_with(state, &mut crate::listener::hooks::CallHooks::default())
            .await;
    }

    pub async fn run_with(
        mut self,
        state: &mut S,
        hooks: &mut crate::listener::hooks::CallHooks<S>,
    ) {
        let outcome = std::panic::AssertUnwindSafe(async {
            self.job.run(state, hooks, self.operation).await
        })
        .catch_unwind()
        .await;

        self.finished = true;

        if let Err(payload) = outcome {
            let mut failure = crate::panic::Failure {
                operation: Some(self.operation),
                payload,
                later: Vec::new(),
            };

            if let Err(secondary) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.job.close()))
            {
                failure.push(secondary);
            }

            std::panic::resume_unwind(Box::new(failure));
        }
    }
}
impl<S> Drop for Message<S> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        // A call that panicked drops its message while unwinding. Closing wakes the caller, and a
        // panic from that wake would be a second one and take the process down.
        if !std::thread::panicking() {
            self.job.close();
        } else if let Err(secondary) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.job.close()))
        {
            crate::panic::dispose_secondary(secondary);
        }
    }
}
