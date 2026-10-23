use crate::{
    AktorArgs, AktorCleanupError, AktorCleanupOutput, AktorExecution, AktorShutdownAsync,
    AktorShutdownFuture, AktorShutdownOutput, AktorShutdownSync, ShutdownReport,
};
use alloc::string::String;
use core::fmt;

impl AktorCleanupOutput for () {
    fn into_cleanup(self) -> Result<(), AktorCleanupError> {
        Ok(())
    }
}
impl AktorCleanupOutput for Result<(), AktorCleanupError> {
    fn into_cleanup(self) -> Result<(), AktorCleanupError> {
        self
    }
}

impl AktorShutdownOutput<AktorShutdownSync> for () {
    type Future = core::future::Ready<Result<(), AktorCleanupError>>;

    fn into_shutdown(self) -> Self::Future {
        core::future::ready(Ok(()))
    }
}
impl AktorShutdownOutput<AktorShutdownSync> for Result<(), AktorCleanupError> {
    type Future = core::future::Ready<Result<(), AktorCleanupError>>;

    fn into_shutdown(self) -> Self::Future {
        core::future::ready(self)
    }
}
impl<F> AktorShutdownOutput<AktorShutdownAsync> for F
where
    F: core::future::Future,
    F::Output: AktorCleanupOutput,
{
    type Future = AktorShutdownFuture<F>;

    fn into_shutdown(self) -> Self::Future {
        AktorShutdownFuture {
            future: alloc::boxed::Box::pin(self),
        }
    }
}
impl<F> core::future::Future for AktorShutdownFuture<F>
where
    F: core::future::Future,
    F::Output: AktorCleanupOutput,
{
    type Output = Result<(), AktorCleanupError>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.get_mut()
            .future
            .as_mut()
            .poll(cx)
            .map(AktorCleanupOutput::into_cleanup)
    }
}

impl<Setup, Cleanup> AktorArgs<Setup, Cleanup> {
    /// Queue capacity starts at 32, change capacity if you need another size.
    pub fn new(name: impl Into<String>, setup: Setup, cleanup: Cleanup) -> Self {
        Self {
            name: name.into(),
            capacity: 32,
            setup,
            cleanup,
        }
    }
}

impl ShutdownReport {
    pub fn failed(&self) -> bool {
        self.failure.is_some()
            || self.timed_out
            || !self.application.is_empty()
            || self
                .actors
                .iter()
                .any(|actor| actor.timed_out || !actor.diagnostics.is_empty())
    }
}
impl fmt::Display for ShutdownReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(failure) = &self.failure {
            write!(f, "Actor {:?}", failure.actor)?;
            write_kind(f, failure.kind)?;
            writeln!(f, " died during {}: {}", failure.phase, failure.message)?;
        }

        for actor in &self.actors {
            for report in &actor.diagnostics {
                write!(f, "{}", actor.actor)?;
                write_kind(f, actor.kind)?;
                writeln!(f, " cleanup:\n{}", report.diagnostics)?;
            }

            if actor.timed_out {
                write!(f, "{}", actor.actor)?;
                write_kind(f, actor.kind)?;
                writeln!(f, " shutdown did not finish")?;
            }
        }

        for report in &self.application {
            writeln!(f, "Application:\n{}", report.diagnostics)?;
        }

        if self.timed_out {
            writeln!(f, "Shutdown deadline reached")?;
        }

        Ok(())
    }
}

fn write_kind(f: &mut fmt::Formatter<'_>, kind: Option<AktorExecution>) -> fmt::Result {
    if let Some(kind) = kind {
        write!(f, " ({kind:?})")?;
    }

    Ok(())
}
