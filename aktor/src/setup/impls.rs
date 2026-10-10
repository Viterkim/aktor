use super::*;
use core::fmt;

#[cfg(any(
    test,
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    )
))]
pub fn diagnostics(error: &(dyn core::error::Error + 'static)) -> String {
    fn source_diagnostics(error: &(dyn core::error::Error + 'static)) -> String {
        let mut output = String::new();
        let mut source = Some(error);

        for _ in 0..64 {
            let Some(error) = source else {
                return output;
            };
            if !output.is_empty() {
                output.push_str("\ncaused by: ");
            }
            output.push_str(&alloc::format!("{error}"));
            source = error.source();
        }

        if source.is_some() {
            output.push_str("\ncaused by: ... (source chain truncated)");
        }
        output
    }

    let render = || source_diagnostics(error);
    #[cfg(feature = "std")]
    {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(render)) {
            Ok(diagnostics) => diagnostics,
            Err(payload) => {
                let message = alloc::format!(
                    "error formatter panicked: {}",
                    crate::panic::panic_message(&payload)
                );

                crate::panic::dispose_secondary(payload);
                message
            }
        }
    }
    #[cfg(not(feature = "std"))]
    {
        render()
    }
}

impl fmt::Display for AktorStartupError {
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
                    write!(f, "\n{} cleanup:\n{}", actor.actor, diagnostic.diagnostics)?;
                }

                if actor.timed_out {
                    write!(f, "\n{} rollback did not finish", actor.actor)?;
                }
            }

            for diagnostic in &report.application {
                write!(f, "\nApplication shutdown:\n{}", diagnostic.diagnostics)?;
            }

            if report.timed_out {
                write!(f, "\nRollback deadline reached")?;
            }
        }

        write!(f, "\n{}", self.diagnostics)?;

        Ok(())
    }
}
impl fmt::Debug for AktorStartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:#}")
    }
}
impl core::error::Error for AktorStartupError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        None
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

impl<S: 'static, Start, Kind: AktorMode, Role, End> AktorNew<S, Start, Kind, Role, End> {
    pub fn with_role<NewRole>(self, role: NewRole) -> AktorNew<S, Start, Kind, NewRole, End> {
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

pub fn aktor_start<Actors, Shutdown, ShutdownOutput, ShutdownMode, StartMode>(
    setup: AktorSetup<Actors, Shutdown, ShutdownOutput>,
) -> AktorStartup<Actors, StartMode>
where
    Actors: AktorStart<StartMode>,
    Actors::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::ShutdownReport) -> ShutdownOutput + 'static,
{
    let mut group = Actors::Group::new(setup.options.shutdown_grace);

    group.track_startup();
    let registered = shutdown::AktorShutdown::on_shutdown(&group, setup.shutdown);
    let mut startup = AktorStartup {
        mode: core::marker::PhantomData,
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
pub async fn aktor_start_in<Setup: AktorStart<Mode>, Mode>(
    group: &Setup::Group,
    setup: Setup,
) -> Result<Setup::Handles, AktorStartupError> {
    if let Err(error) = group.check_started() {
        let error = AktorStartError::from(error);
        let diagnostics = Setup::diagnostics(&error);

        return Err(AktorStartupError {
            diagnostics,
            error,
            report: None,
            source_is_failure: false,
        });
    }

    let mut group = group.registration();

    group.track_startup();

    let startup = AktorStartup {
        mode: core::marker::PhantomData,
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
            Self::Init(_) | Self::Setup(_) => true,
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Self::Thread(
                crate::listener::DedicatedStartError::Panicked { .. }
                | crate::listener::DedicatedStartError::InvalidCapacity,
            ) => true,
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

pub fn start_diagnostics(error: &AktorStartError) -> String {
    match error {
        AktorStartError::Init(error) => error.diagnostics.clone(),
        AktorStartError::Setup(error) => error.diagnostics.clone(),
        #[cfg(feature = "local")]
        AktorStartError::Local(error) => alloc::format!("{error:#}"),
        #[cfg(all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ))]
        AktorStartError::Thread(_) => diagnostics(error),
    }
}

pub fn validation_error<Group: AktorSetupGroup>(
    group: &Group,
    actor: &str,
    kind: AktorExecution,
    error: AktorStartError,
) -> AktorStartError {
    Group::fail_startup(
        &group.killswitch(),
        crate::ActorFailure {
            actor: actor.into(),
            kind: Some(kind),
            phase: "setup".into(),
            message: start_diagnostics(&error),
        },
    );
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::error::Error;
    use core::fmt::Write;

    #[derive(Debug)]
    struct Cause {
        source: Option<Box<Cause>>,
        cycle: bool,
        panic: bool,
    }
    impl fmt::Display for Cause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("initialization cause")
        }
    }
    impl Error for Cause {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            if self.panic {
                panic!("source panic sentinel");
            }

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
        let public = |cause| {
            let captured = crate::AktorFailure::from(AktorSetupError::new(diagnostics(&cause)));
            let error = AktorStartError::Init(captured);

            AktorStartupError {
                diagnostics: start_diagnostics(&error),
                error,
                report: None,
                source_is_failure: false,
            }
        };
        let finite = public(Cause {
            source: Some(Box::new(Cause {
                source: None,
                cycle: false,
                panic: false,
            })),
            cycle: false,
            panic: false,
        });

        assert_eq!(
            alloc::format!("{finite:#}")
                .matches("initialization cause")
                .count(),
            2
        );

        #[cfg(feature = "std")]
        {
            let hostile = public(Cause {
                source: None,
                cycle: false,
                panic: true,
            });
            let output = alloc::format!("{hostile:#}");

            assert!(output.contains("actor startup failed"));
            assert!(output.contains("error formatter panicked: source panic sentinel"));
            assert_eq!(alloc::format!("{hostile:?}"), output);
        }

        let captured = crate::AktorFailure::from(AktorSetupError::new(diagnostics(&Cause {
            source: None,
            cycle: true,
            panic: false,
        })));
        let mut output = Output(String::new());

        output
            .write_str(&captured.diagnostics)
            .expect("captured source walk exceeded its output budget");
        assert!(output.0.ends_with("(source chain truncated)"));
        assert!(captured.diagnostics.contains("source chain truncated"));
        assert!(alloc::format!("{captured:?}").contains("source chain truncated"));

        let cyclic = public(Cause {
            source: None,
            cycle: true,
            panic: false,
        });
        let mut output = Output(String::new());

        write!(output, "{cyclic:#}").expect("source walk exceeded its output budget");
        assert!(output.0.ends_with("(source chain truncated)"));
        assert_eq!(alloc::format!("{cyclic:?}"), output.0);
    }
}
