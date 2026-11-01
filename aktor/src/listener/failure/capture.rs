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
                let (kind, payload, annotated) = match payload.downcast::<FailurePanic>() {
                    Ok(failure) => (failure.kind, failure.payload, true),
                    Err(payload) => (kind, payload, false),
                };
                let (kind, payload, diagnostics) = match payload.downcast::<Failure>() {
                    Ok(failure) => (
                        if annotated { kind } else { failure.kind },
                        failure.payload,
                        failure.diagnostics,
                    ),
                    Err(payload) => (kind, payload, Vec::new()),
                };

                if self.first.is_none() {
                    self.first = Some(Failure {
                        actor: self.actor.clone(),
                        kind,
                        payload,
                        diagnostics: Vec::new(),
                        group_primary: false,
                    });

                    if let (Some(group), Some(failure)) = (&self.group, &mut self.first) {
                        let message = failure.to_string();
                        failure.group_primary = group.fail(crate::group::AktorFailureReport {
                            kind: None,
                            actor: failure.actor.clone(),
                            phase: format!("{:?}", failure.kind),
                            message,
                        });
                    }

                    // Completion prints these separately. Including them above would repeat them.
                    if let Some(first) = &mut self.first {
                        first.diagnostics.extend(diagnostics);
                    }
                } else if let Some(first) = &mut self.first {
                    first.diagnostics.push(crate::AktorError::new(
                        format!("{kind:?}: {}", crate::panic::panic_message(&payload)),
                        (),
                    ));
                    first.diagnostics.extend(diagnostics);
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

    pub fn resume(self) {
        if let Some(failure) = self.first {
            panic::resume_unwind(Box::new(failure));
        }
    }

    pub fn finish(self, policy: &FailurePolicy) {
        if let Some(failure) = self.first {
            policy.fail(failure);
        }
    }
}

pub use crate::panic::dispose_secondary;

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
    fn primary_failure() {
        dispose_secondary(Box::new(BrokenDrop));
    }
}
