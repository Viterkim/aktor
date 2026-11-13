use crate::{
    AktorArgs, AktorCleanupOutput, AktorError, AktorExecution, AktorGroupRetirement,
    AktorGroupShutdownError, AktorRetireStatus, AktorShutdownAsync, AktorShutdownFuture,
    AktorShutdownOutput, AktorShutdownReport, AktorShutdownSync,
};
use alloc::string::String;
use core::fmt;

impl AktorCleanupOutput for () {
    fn into_cleanup(self) -> Result<(), AktorError> {
        Ok(())
    }
}
impl AktorCleanupOutput for Result<(), String> {
    fn into_cleanup(self) -> Result<(), AktorError> {
        self.map_err(|report| AktorError::new(report, ()))
    }
}
impl<E> AktorCleanupOutput for Result<(), (E, String)> {
    fn into_cleanup(self) -> Result<(), AktorError> {
        self.map_err(|(_, report)| AktorError::new(report, ()))
    }
}
impl<E: core::error::Error + 'static> AktorCleanupOutput for er::ErResult<(), E> {
    fn into_cleanup(self) -> Result<(), AktorError> {
        match self {
            Ok(()) => Ok(()),
            Err(tree) => Err(AktorError::new(crate::report::tree_report(&tree), ())),
        }
    }
}
impl<E: core::error::Error + 'static> AktorCleanupOutput<E> for Result<(), E> {
    fn into_cleanup(self) -> Result<(), AktorError> {
        self.map_err(|error| AktorError::new(crate::report::diagnostics(&error), ()))
    }
}
impl<M, T: AktorCleanupOutput<M>> AktorShutdownOutput<(AktorShutdownSync, M)> for T {
    type Future = core::future::Ready<Result<(), AktorError>>;

    fn into_shutdown(self) -> Self::Future {
        core::future::ready(self.into_cleanup())
    }
}
impl<F, M> AktorShutdownOutput<(AktorShutdownAsync, M)> for F
where
    F: core::future::Future,
    F::Output: AktorCleanupOutput<M>,
{
    type Future = AktorShutdownFuture<F, M>;

    fn into_shutdown(self) -> Self::Future {
        AktorShutdownFuture {
            future: alloc::boxed::Box::pin(self),
            mode: core::marker::PhantomData,
        }
    }
}
impl<F, M> core::future::Future for AktorShutdownFuture<F, M>
where
    F: core::future::Future,
    F::Output: AktorCleanupOutput<M>,
{
    type Output = Result<(), AktorError>;

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

impl AktorShutdownReport {
    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn callback_timed_out(&mut self) {
        self.group_error = Some(AktorGroupShutdownError::CallbackTimedOut);
        self.ran_past_grace = true;
    }

    /// `Err` when shutdown failed, with the printed report next to the error.
    ///
    /// `group.shutdown().await.into_result().er(..)?;`
    pub fn into_result(self) -> Result<(), (crate::AktorShutdownError, String)> {
        if !self.failed() {
            return Ok(());
        }

        let text = alloc::format!("{self}");

        Err((
            crate::AktorShutdownError {
                report: alloc::boxed::Box::new(self),
            },
            text,
        ))
    }

    pub fn failed(&self) -> bool {
        self.group_error.is_some()
            || self.failure.is_some()
            || self.ran_past_grace
            || !self.application.is_empty()
            || self.actors.iter().any(|actor| {
                actor.retirement != AktorRetireStatus::Confirmed
                    || actor.ran_past_grace
                    || !actor.diagnostics.is_empty()
            })
    }

    /// Every reported owner has stopped. Returns false when no actor reported.
    /// A failed cleanup can still leave a file open or a write unfinished.
    pub fn retired(&self) -> bool {
        matches!(
            self.retirement_status(),
            AktorGroupRetirement::AllConfirmed | AktorGroupRetirement::SomeTerminated
        )
    }

    pub(crate) fn retirement_status(&self) -> AktorGroupRetirement {
        if self.group_error == Some(AktorGroupShutdownError::ReportLost) {
            return AktorGroupRetirement::ReportLost;
        }

        if self.actors.is_empty() {
            return AktorGroupRetirement::NoActors;
        }

        let mut status = AktorGroupRetirement::AllConfirmed;

        for actor in &self.actors {
            match actor.retirement {
                AktorRetireStatus::Confirmed => {}
                AktorRetireStatus::Terminated => status = AktorGroupRetirement::SomeTerminated,
                AktorRetireStatus::Unconfirmed => return AktorGroupRetirement::Unconfirmed,
            }
        }

        status
    }
}
impl AktorRetireStatus {
    pub(crate) fn diagnostic(self) -> Option<&'static str> {
        match self {
            Self::Confirmed => None,
            Self::Unconfirmed => Some("may still be running"),
            Self::Terminated => Some("was terminated"),
        }
    }
}
impl fmt::Display for AktorShutdownReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(failure) = &self.failure {
            write!(f, "Aktor {:?}", failure.actor)?;
            write_kind(f, failure.kind)?;
            writeln!(f, " died during {}: {}", failure.phase, failure.message)?;
        }

        for actor in &self.actors {
            for report in &actor.diagnostics {
                write!(f, "{}", actor.actor)?;
                write_kind(f, Some(actor.kind))?;
                writeln!(f, " diagnostics:\n{}", report.diagnostics)?;
            }

            if let Some(status) = actor.retirement.diagnostic() {
                writeln!(f, "{} {status}", actor.actor)?;
            }

            if actor.ran_past_grace {
                write!(f, "{}", actor.actor)?;
                write_kind(f, Some(actor.kind))?;
                writeln!(f, " ran past its grace")?;
            }
        }

        for report in &self.application {
            writeln!(f, "Application:\n{}", report.diagnostics)?;
        }

        if let Some(error) = self.group_error {
            writeln!(f, "{error}")?;
        } else if self.ran_past_grace {
            writeln!(f, "Shutdown ran past its grace")?;
        }

        Ok(())
    }
}

impl fmt::Display for AktorGroupShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ReportLost => "The group did not publish a shutdown report",
            Self::CallbackTimedOut => "The group shutdown callback timed out",
        })
    }
}
impl core::error::Error for AktorGroupShutdownError {}

fn write_kind(f: &mut fmt::Formatter<'_>, kind: Option<AktorExecution>) -> fmt::Result {
    if let Some(kind) = kind {
        write!(f, " ({kind:?})")?;
    }

    Ok(())
}
