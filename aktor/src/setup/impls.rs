use super::*;
use core::{error::Error, fmt};

impl<E: core::error::Error + 'static> fmt::Display for AktorStartupError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actor startup failed")?;

        if let Some(report) = &self.report {
            if let Some(failure) = &report.failure {
                if self.source_is_failure {
                    write!(f, " for {}", failure.actor)?;
                } else {
                    write!(
                        f,
                        "\n{} failed during {}:\n{}",
                        failure.actor, failure.phase, failure.message
                    )?;
                }
            }

            for actor in &report.actors {
                for diagnostic in &actor.diagnostics {
                    write!(f, "\n{} cleanup:\n{diagnostic}", actor.actor)?;
                }

                if actor.timed_out {
                    write!(f, "\n{} rollback did not finish", actor.actor)?;
                }
            }

            for diagnostic in &report.application {
                write!(f, "\nApplication shutdown:\n{diagnostic}")?;
            }

            if report.timed_out {
                write!(f, "\nRollback deadline reached")?;
            }
        }

        if f.alternate() {
            let mut source = self.source();

            for _ in 0..64 {
                let Some(error) = source else {
                    break;
                };

                write!(f, "\ncaused by: {error}")?;
                source = error.source();
            }

            if source.is_some() {
                f.write_str("\ncaused by: ... (source chain truncated)")?;
            }
        }

        Ok(())
    }
}
impl<E: core::error::Error + 'static> fmt::Debug for AktorStartupError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:#}")
    }
}
impl<E: core::error::Error + 'static> core::error::Error for AktorStartupError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl AktorName {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for AktorNewOptions {
    fn default() -> Self {
        Self { capacity: 32 }
    }
}

impl Default for AktorOptions {
    fn default() -> Self {
        Self {
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

impl<S: 'static, Start, Kind: AktorMode, Role> AktorNew<S, Start, Kind, Role> {
    pub fn with_role<NewRole>(self, role: NewRole) -> AktorNew<S, Start, Kind, NewRole> {
        AktorNew {
            name: self.name,
            role,
            kind: self.kind,
            closures: self.closures,
            options: self.options,
        }
    }

    pub fn execution(&self) -> AktorExecution {
        Kind::EXECUTION
    }
}

impl<Handles, Group: AktorSetupGroup> AktorStarted<Handles, Group> {
    pub fn killswitch(&self) -> Group::KillSwitch {
        self.group.killswitch()
    }

    pub fn completion(&self) -> Group::Completion {
        self.group.completion()
    }

    pub fn shutdown(&self) -> Group::Closing {
        Group::stop(&self.group.killswitch());
        self.group.closing()
    }
}

impl<Group: AktorSetupGroup> Clone for AktorStartContext<Group> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.registration(),
        }
    }
}

pub fn aktor_start<Actors, Shutdown, ShutdownOutput, ShutdownMode>(
    setup: AktorSetup<Actors, Shutdown, ShutdownOutput>,
) -> AktorStartup<Actors>
where
    Actors: AktorStart,
    Actors::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::ShutdownReport) -> ShutdownOutput + 'static,
{
    let mut group = Actors::Group::new(setup.options.shutdown_grace);

    group.track_startup();
    let registered = shutdown::AktorShutdown::on_shutdown(&group, setup.shutdown);
    let mut startup = AktorStartup {
        killswitch: group.killswitch(),
        completion: group.completion(),
        begin: true,
        group: Some(group),
        setup: Some(Box::new(setup.actors)),
        starting: None,
        failure: None,
        shutdown: None,
    };

    if let Err(error) = registered {
        startup.setup = None;
        startup.failure = Some(Box::new(error.into()));

        if let Some(group) = &startup.group {
            Actors::Group::stop(&startup.killswitch);
            startup.shutdown = Some(Box::pin(group.closing()));
        }
    }

    startup
}

/// Add prepared actors to a group whose shutdown listener is already running.
/// Failure or cancellation after startup begins stops the whole group, including existing actors.
pub async fn aktor_start_in<Setup: AktorStart>(
    group: &Setup::Group,
    setup: Setup,
) -> Result<Setup::Handles, AktorStartupError<Setup::Error>> {
    group.check_started().map_err(|error| AktorStartupError {
        error: Setup::Error::from(error),
        report: None,
        source_is_failure: false,
    })?;

    let mut group = group.registration();

    group.track_startup();

    let startup = AktorStartup {
        begin: false,
        killswitch: group.killswitch(),
        completion: group.completion(),
        group: Some(group),
        setup: Some(Box::new(setup)),
        starting: None,
        failure: None,
        shutdown: None,
    };

    startup.await.map(|started| started.handles)
}

impl<E: 'static> AktorStartError<E> {
    #[doc(hidden)]
    pub fn source_is_setup(&self) -> bool {
        match self {
            Self::Init(_) => true,
            #[cfg(feature = "local")]
            Self::Local(
                crate::local::OwnerError::Setup(_) | crate::local::OwnerError::SetupPanic(_),
            ) => true,
            _ => false,
        }
    }
}

impl<E: 'static> From<AktorSetupError> for AktorStartError<E> {
    fn from(error: AktorSetupError) -> Self {
        Self::Setup(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::fmt::Write;

    #[derive(Debug)]
    struct Cause {
        source: Option<Box<Cause>>,
        cycle: bool,
    }
    impl fmt::Display for Cause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("initialization cause")
        }
    }
    impl Error for Cause {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            if self.cycle {
                Some(self)
            } else {
                self.source.as_deref().map(|source| source as &dyn Error)
            }
        }
    }

    struct Output(String);
    impl Write for Output {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            if self.0.len() + text.len() > 4096 {
                return Err(fmt::Error);
            }

            self.0.push_str(text);
            Ok(())
        }
    }

    #[test]
    fn sources() {
        let finite = AktorStartupError {
            error: Cause {
                source: Some(Box::new(Cause {
                    source: None,
                    cycle: false,
                })),
                cycle: false,
            },
            report: None,
            source_is_failure: false,
        };

        assert_eq!(
            alloc::format!("{finite:#}"),
            "actor startup failed\ncaused by: initialization cause\ncaused by: initialization cause"
        );

        let cyclic = AktorStartupError {
            error: Cause {
                source: None,
                cycle: true,
            },
            report: None,
            source_is_failure: false,
        };
        let mut output = Output(String::new());

        write!(output, "{cyclic:#}").expect("source walk exceeded its output budget");
        assert!(output.0.ends_with("(source chain truncated)"));
        assert_eq!(alloc::format!("{cyclic:?}"), output.0);
    }
}
