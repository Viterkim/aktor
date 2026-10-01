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
        let _written = writeln!(std::io::stderr().lock(), "{failure}");

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
            Self::Unwind => {}
        }
    }

    pub fn fail(&self, failure: Failure) -> ! {
        self.report(&failure);
        panic::resume_unwind(failure.payload)
    }
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(error) => error.fmt(f),
            Self::Panicked => f.write_str("actor serving loop panicked"),
        }
    }
}
impl<E: core::error::Error + 'static> core::error::Error for RunError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Panicked => None,
        }
    }
}
