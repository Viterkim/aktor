use super::*;

impl fmt::Display for FailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(operation) => write!(f, "{} @ {}", operation.name, operation.caller),
            Self::Cleanup => f.write_str("cleanup"),
            Self::Teardown => f.write_str("teardown"),
            Self::Cancelled => f.write_str("cancel"),
            Self::Runtime => f.write_str("runtime"),
        }
    }
}

fn hook_panic(hook: fmt::Arguments<'_>, payload: Box<dyn Any + Send>) {
    let _written = writeln!(
        std::io::stderr().lock(),
        "{hook} panicked: {}",
        crate::panic::panic_message(&payload),
    );
    dispose_secondary(payload);
}

impl Drop for ShutdownHook {
    fn drop(&mut self) {
        // A hook can own a value whose destructor panics while the actor is unwinding.
        if std::thread::panicking()
            && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(self.0.take())))
        {
            hook_panic(format_args!("actor shutdown hook destructor"), payload);
        }
    }
}

impl Failure {
    /// The panic message and diagnostics, without saying which actor or when.
    pub fn message(&self) -> String {
        let mut message = String::new();

        if let Some(text) = self.payload.downcast_ref::<&str>() {
            message.push_str(text);
        }

        if let Some(text) = self.payload.downcast_ref::<String>() {
            message.push_str(text);
        }

        for diagnostic in &self.diagnostics {
            message.push('\n');
            message.push_str(&diagnostic.diagnostics);
        }

        message
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "actor {:?} failed during {}: {}",
            self.actor,
            self.kind,
            self.message()
        )
    }
}

impl FailurePolicy {
    pub fn shutdown(action: impl Fn(&Failure) + Send + Sync + 'static) -> Self {
        Self::Shutdown(ShutdownHook(Some(Arc::new(action))))
    }

    pub fn report(&self, failure: &Failure) {
        if !matches!(self, Self::Group(_)) {
            let _written = writeln!(std::io::stderr().lock(), "{failure}");
        }

        match self {
            Self::Abort => std::process::abort(),
            Self::Shutdown(hook) => {
                if let Some(action) = &hook.0
                    && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| action(failure)))
                {
                    hook_panic(
                        format_args!("actor {:?} shutdown hook", failure.actor),
                        payload,
                    );
                }
            }
            Self::Group(group) => {
                group.fail(crate::group::AktorFailureReport {
                    kind: None,
                    actor: failure.actor.clone(),
                    phase: failure.kind.to_string(),
                    message: failure.message(),
                });
            }
            Self::Unwind => {}
        }
    }

    pub fn fail(&self, failure: Failure) -> ! {
        self.report(&failure);
        panic::resume_unwind(failure.payload)
    }
}
