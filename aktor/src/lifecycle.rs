use crate::{AktorCleanupError, AktorError};
use alloc::{string::String, vec::Vec};
use core::fmt;

/// Setup and cleanup for an actor owned by the application.
pub struct ActorArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: usize,
    pub setup: Setup,
    pub cleanup: Cleanup,
}
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

/// What failed and how cleanup went.
#[derive(Clone, Debug, Default)]
pub struct ShutdownReport {
    pub failure: Option<ActorFailure>,
    pub actors: Vec<ActorOutcome>,
    pub application: Vec<AktorError>,
    pub timed_out: bool,
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
            writeln!(
                f,
                "Actor {:?} died during {}: {}",
                failure.actor, failure.phase, failure.message
            )?;
        }

        for actor in &self.actors {
            for report in &actor.diagnostics {
                writeln!(f, "{} cleanup:\n{}", actor.actor, report)?;
            }

            if actor.timed_out {
                writeln!(f, "{} shutdown did not finish", actor.actor)?;
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

#[derive(Clone, Debug)]
pub struct ActorFailure {
    pub actor: String,
    pub phase: String,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct ActorOutcome {
    pub actor: String,
    pub diagnostics: Vec<AktorCleanupError>,
    pub timed_out: bool,
}
