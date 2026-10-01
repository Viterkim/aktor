use super::super::*;
use futures_util::FutureExt;

impl<S> Message<S> {
    pub fn latest_key(&self) -> Option<LatestKey> {
        self.latest.clone()
    }

    pub fn has_latest_key(&self, key: &LatestKey) -> bool {
        self.latest
            .as_ref()
            .is_some_and(|latest| latest.identical(key))
    }

    pub fn same_latest(&self, other: &Self) -> bool {
        match (&self.latest, &other.latest) {
            (Some(left), Some(right)) => left.same(right),
            _ => false,
        }
    }

    pub fn supersede(mut self) {
        self.finished = true;
        self.job.supersede();
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
