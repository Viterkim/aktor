use super::*;

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor {:?} failed during {:?}", self.actor, self.kind)?;

        if let Some(message) = self.payload.downcast_ref::<&str>() {
            write!(f, ": {message}")?;
        }
        if let Some(message) = self.payload.downcast_ref::<String>() {
            write!(f, ": {message}")?;
        }

        Ok(())
    }
}

impl FailurePolicy {
    pub fn shutdown(action: impl Fn(&Failure) + Send + Sync + 'static) -> Self {
        Self::Shutdown(Arc::new(action))
    }

    pub fn report(&self, failure: &Failure) {
        if !matches!(self, Self::Group(_)) {
            let _written = writeln!(std::io::stderr().lock(), "{failure}");
        }

        match self {
            Self::Abort => std::process::abort(),
            Self::Shutdown(action) => {
                if let Err(_payload) = panic::catch_unwind(AssertUnwindSafe(|| action(failure))) {
                    let _written = writeln!(
                        std::io::stderr().lock(),
                        "actor shutdown hook panicked, aborting"
                    );
                    std::process::abort();
                }
            }
            Self::Group(group) => group.fail(crate::group::ActorFailure {
                actor: failure.actor.clone(),
                phase: format!("{:?}", failure.kind),
                message: failure.to_string(),
            }),
            Self::Unwind => {}
        }
    }

    pub fn fail(&self, failure: Failure) -> ! {
        self.report(&failure);
        panic::resume_unwind(failure.payload)
    }
}
