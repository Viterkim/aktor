use crate::{ActorArgs, AktorExecution, ShutdownReport};
use alloc::string::String;
use core::fmt;

impl<Setup, Cleanup> ActorArgs<Setup, Cleanup> {
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
                writeln!(f, " cleanup:\n{report}")?;
            }

            if actor.timed_out {
                write!(f, "{}", actor.actor)?;
                write_kind(f, actor.kind)?;
                writeln!(f, " shutdown did not finish")?;
            }
        }

        for report in &self.application {
            writeln!(f, "Application:\n{}", report)?;
        }

        if self.timed_out {
            writeln!(f, "Shutdown deadline reached")?;
        }

        Ok(())
    }
}
impl core::error::Error for ShutdownReport {}

fn write_kind(f: &mut fmt::Formatter<'_>, kind: Option<AktorExecution>) -> fmt::Result {
    if let Some(kind) = kind {
        write!(f, " ({kind:?})")?;
    }

    Ok(())
}
