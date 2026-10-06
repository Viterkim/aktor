use super::*;
use futures_util::FutureExt;

impl Failures {
    pub fn new(actor: String) -> Self {
        Self {
            actor,
            first: None,
            group: None,
        }
    }

    pub fn capture<T>(&mut self, kind: FailureKind, action: impl FnOnce() -> T) -> Option<T> {
        match panic::catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => Some(output),
            Err(payload) => {
                let (kind, payload) = match payload.downcast::<FailurePanic>() {
                    Ok(failure) => (failure.kind, failure.payload),
                    Err(payload) => (kind, payload),
                };

                if self.first.is_none() {
                    self.first = Some(Failure {
                        actor: self.actor.clone(),
                        kind,
                        payload,
                    });

                    if let (Some(group), Some(failure)) = (&self.group, &self.first) {
                        group.fail(crate::group::ActorFailure {
                            kind: None,
                            actor: failure.actor.clone(),
                            phase: format!("{:?}", failure.kind),
                            message: failure.to_string(),
                        });
                    }
                } else {
                    dispose_secondary(payload);
                }

                None
            }
        }
    }

    pub async fn capture_async<T>(
        &mut self,
        kind: FailureKind,
        future: impl core::future::Future<Output = T>,
    ) -> Option<T> {
        let outcome = AssertUnwindSafe(future).catch_unwind().await;

        self.capture(kind, || match outcome {
            Ok(output) => output,
            Err(payload) => panic::resume_unwind(payload),
        })
    }

    pub fn finish(self, policy: &FailurePolicy) {
        if let Some(failure) = self.first {
            policy.fail(failure);
        }
    }
}

pub fn dispose_secondary(payload: Box<dyn std::any::Any + Send>) {
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(payload);
    }
}

impl FailureKind {
    pub fn during<T>(self, action: impl FnOnce() -> T) -> T {
        match panic::catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => output,
            Err(payload) => panic::resume_unwind(Box::new(FailurePanic {
                kind: self,
                payload,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BrokenDrop;
    impl Drop for BrokenDrop {
        fn drop(&mut self) {
            panic!("secondary payload destructor");
        }
    }

    #[test]
    fn secondary_payload_cannot_replace_the_primary_failure() {
        dispose_secondary(Box::new(BrokenDrop));
    }
}
