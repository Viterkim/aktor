use super::super::*;
use futures_util::FutureExt;

impl<S> Message<S> {
    pub fn counted(&self) -> bool {
        self.counted
    }

    pub async fn run(mut self, state: &mut S) {
        let outcome = std::panic::AssertUnwindSafe(async { self.job.run(state).await })
            .catch_unwind()
            .await;

        self.finished = true;
        if let Err(payload) = outcome {
            if let Err(secondary) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.job.close()))
            {
                crate::listener::failure::dispose_secondary(secondary);
            }

            std::panic::resume_unwind(Box::new(crate::listener::failure::FailurePanic {
                kind: crate::listener::FailureKind::Operation(self.operation),
                payload,
            }));
        }
    }
}
impl<S> Drop for Message<S> {
    fn drop(&mut self) {
        if !self.finished {
            self.job.close();
        }
    }
}
