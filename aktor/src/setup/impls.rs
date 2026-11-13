use super::*;
use crate::AktorError;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
use crate::{AktorGroup, AktorShutdownPolicy};
use alloc::string::ToString;
use core::fmt;

struct StartupDetails<'a> {
    primary: &'a str,
    report: Option<&'a crate::AktorShutdownReport>,
    source_is_failure: bool,
}
impl fmt::Display for StartupDetails<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actor startup failed")?;

        let failed_for = self
            .report
            .and_then(|report| report.failure.as_ref())
            .filter(|_| self.source_is_failure);

        if let Some(failure) = failed_for {
            write!(f, " for {}", failure.actor)?;
        }

        // The reason goes first, the rollback of everything else comes after it.
        write!(f, "\n{}", self.primary)?;

        if let Some(report) = &self.report {
            if let Some(failure) = &report.failure
                && !self.source_is_failure
            {
                write!(
                    f,
                    "\n{} failed during {}:\n{}",
                    failure.actor, failure.phase, failure.message
                )?;
            }

            for actor in &report.actors {
                for diagnostic in &actor.diagnostics {
                    write!(
                        f,
                        "\n{} diagnostics:\n{}",
                        actor.actor, diagnostic.diagnostics
                    )?;
                }

                if let Some(status) = actor.retirement.diagnostic() {
                    write!(f, "\n{} {status}", actor.actor)?;
                }

                if actor.ran_past_grace {
                    write!(f, "\n{} rollback ran past its grace", actor.actor)?;
                }
            }

            for diagnostic in &report.application {
                write!(f, "\nApplication shutdown:\n{}", diagnostic.diagnostics)?;
            }

            if let Some(error) = report.group_error {
                write!(f, "\n{error}")?;
            } else if report.ran_past_grace {
                write!(f, "\nRollback ran past its grace")?;
            }
        }

        Ok(())
    }
}

impl<E> AktorStartError<E> {
    pub(crate) fn with_report(
        mut self,
        primary: String,
        report: Option<Box<crate::AktorShutdownReport>>,
        source_is_failure: bool,
    ) -> (Self, String) {
        let details = alloc::format!(
            "{}",
            StartupDetails {
                primary: &primary,
                report: report.as_deref(),
                source_is_failure,
            }
        );

        self.report = report;
        (self, details)
    }
}

impl<S: 'static, Kind: AktorMode> Default for AktorHooks<S, Kind> {
    fn default() -> Self {
        Self {
            intervals: Vec::new(),
            before_each: None,
            after_each: None,
        }
    }
}

impl<S: 'static, Start, Kind: AktorMode, End, Signature> AktorNew<S, Start, Kind, End, Signature> {
    pub fn execution(&self) -> AktorExecution {
        Kind::EXECUTION
    }
}

impl<Handles, Group: AktorSetupGroup> AktorStarted<Handles, Group> {
    pub fn killswitch(&self) -> Group::AktorKillSwitch {
        self.group.killswitch()
    }

    pub fn completion(&self) -> Group::Completion {
        self.group.completion()
    }

    /// Start shutdown and return its completion handle. Await it to get the report.
    pub fn shutdown(&self) -> Group::Completion {
        Group::stop(&self.group.killswitch());
        self.group.completion()
    }
}

impl<Group: AktorSetupGroup> Clone for AktorStartContext<Group> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.registration(),
        }
    }
}

/// Start a group and its actors. Keep the group with the handles, for example in your GUI state
/// or a store you open and close. Dropping the group starts shutdown. Add more actors with
/// `started.group.spawn(..)`.
pub fn aktor_start<Setups, Shutdown, ShutdownOutput, ShutdownMode>(
    setup: AktorSetup<Setups, Shutdown, ShutdownOutput>,
) -> AktorStartup<Setups>
where
    Setups: AktorStart,
    Setups::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::AktorShutdownReport) -> ShutdownOutput + 'static,
{
    let group = Setups::Group::new(setup.grace);

    start_group(group, setup)
}

/// Start a native group and choose what happens when shutdown gets stuck.
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub fn aktor_start_with_policy<Setups, Shutdown, ShutdownOutput, ShutdownMode>(
    setup: AktorSetup<Setups, Shutdown, ShutdownOutput>,
    policy: AktorShutdownPolicy,
) -> AktorStartup<Setups>
where
    Setups: AktorStart<Group = AktorGroup>,
    AktorGroup: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::AktorShutdownReport) -> ShutdownOutput + 'static,
{
    let group = AktorGroup::with_policy(setup.grace, policy);

    group.killswitch().set_starting(true);
    start_group(group, setup)
}

fn start_group<Setups, Shutdown, ShutdownOutput, ShutdownMode>(
    mut group: Setups::Group,
    setup: AktorSetup<Setups, Shutdown, ShutdownOutput>,
) -> AktorStartup<Setups>
where
    Setups: AktorStart,
    Setups::Group: shutdown::AktorShutdown<Shutdown, ShutdownOutput, ShutdownMode>,
    Shutdown: FnOnce(crate::AktorShutdownReport) -> ShutdownOutput + 'static,
{
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
        startup.failure = Some(Box::new(AktorStartError::not_run(AktorStartCause::Group(
            error,
        ))));

        if let Some(group) = &startup.group {
            Setups::Group::stop(&startup.killswitch);
            startup.shutdown = Some(Box::pin(core::future::IntoFuture::into_future(
                group.completion(),
            )));
        }
    }

    startup
}

#[doc(hidden)]
/// Add prepared actors to a group whose shutdown listener is already running.
/// Failure or cancellation after startup begins stops the whole group, including existing actors.
pub async fn aktor_start_in<Setup: AktorStart>(
    group: &Setup::Group,
    setup: Setup,
) -> Result<Setup::Handles, (AktorStartError<Setup::Error>, String)> {
    if let Err(error) = group.check_started() {
        let cause = AktorStartCause::Group(error);
        let diagnostics = Setup::diagnostics(&cause);

        return Err(AktorStartError::not_run(cause).with_report(diagnostics, None, false));
    }

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

impl<E: 'static> AktorStartCause<E> {
    #[cfg(any(
        all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ),
        all(
            any(feature = "browser_local", feature = "wasm_browser_workers"),
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn group_error((error, _): (AktorGroupError, String)) -> Self {
        Self::Group(error)
    }

    #[doc(hidden)]
    pub fn source_is_setup(&self) -> bool {
        match self {
            Self::Group(AktorGroupError::Stopping | AktorGroupError::Stopped) => false,
            Self::Init(_) | Self::Setup(_) | Self::Group(_) => true,
            #[cfg(all(
                feature = "wasm_browser_workers",
                target_family = "wasm",
                target_os = "unknown"
            ))]
            Self::Worker(_, _) => true,
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Self::Thread(
                crate::listener::DedicatedStartError::Panicked { .. }
                | crate::listener::DedicatedStartError::InvalidCapacity
                | crate::listener::DedicatedStartError::ThreadNotCreated(_)
                | crate::listener::DedicatedStartError::Thread(_),
            ) => true,
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Self::Startup(error) => match error.cause.primary() {
                crate::listener::DedicatedStartError::Init(_)
                | crate::listener::DedicatedStartError::Panicked { .. }
                | crate::listener::DedicatedStartError::InvalidCapacity
                | crate::listener::DedicatedStartError::ThreadNotCreated(_)
                | crate::listener::DedicatedStartError::Thread(_) => true,
                crate::listener::DedicatedStartError::Startup { .. }
                | crate::listener::DedicatedStartError::Observer { .. }
                | crate::listener::DedicatedStartError::NotStarted
                | crate::listener::DedicatedStartError::Closed
                | crate::listener::DedicatedStartError::NoRuntime => false,
            },
            #[cfg(feature = "local")]
            Self::Local(
                crate::local::OwnerError::Setup(_) | crate::local::OwnerError::SetupPanic(_),
                _,
            ) => true,
            _ => false,
        }
    }
}

impl<E: 'static> From<AktorError> for AktorStartCause<E> {
    fn from(error: AktorError) -> Self {
        Self::Setup(error)
    }
}
impl<E: 'static> From<AktorGroupError> for AktorStartCause<E> {
    fn from(error: AktorGroupError) -> Self {
        Self::Group(error)
    }
}

pub fn start_diagnostics(error: &AktorStartCause) -> String {
    match error {
        AktorStartCause::Init(error) => error.diagnostics.clone(),
        AktorStartCause::Setup(error) => error.diagnostics.clone(),
        AktorStartCause::Group(error) => error.to_string(),
        #[cfg(feature = "local")]
        AktorStartCause::Local(_, report) => report.clone(),
        #[cfg(all(
            feature = "wasm_browser_workers",
            target_family = "wasm",
            target_os = "unknown"
        ))]
        AktorStartCause::Worker(_, report) => report.clone(),
        #[cfg(all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ))]
        AktorStartCause::Thread(_) => diagnostics(error),
        #[cfg(all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ))]
        AktorStartCause::Startup(error) => error.report(),
    }
}

pub fn validation_error<Group: AktorSetupGroup>(
    group: &Group,
    actor: &str,
    kind: AktorExecution,
    error: AktorStartCause,
) -> AktorStartCause {
    Group::fail_startup(
        &group.killswitch(),
        crate::AktorFailureReport {
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
            let captured = crate::AktorFailure::from(AktorError::new(diagnostics(&cause), ()));
            let error = AktorStartCause::Init(captured);

            let diagnostics = start_diagnostics(&error);

            AktorStartError::<core::convert::Infallible>::from(error).with_report(
                diagnostics,
                None,
                false,
            )
        };
        let (_, finite) = public(Cause {
            source: Some(Box::new(Cause {
                source: None,
                cycle: false,
                panic: false,
            })),
            cycle: false,
            panic: false,
        });

        assert_eq!(finite.matches("initialization cause").count(), 2);

        #[cfg(feature = "std")]
        {
            let (hostile, output) = public(Cause {
                source: None,
                cycle: false,
                panic: true,
            });

            assert!(output.contains("actor startup failed"));
            assert!(output.contains("error formatter panicked: source panic sentinel"));
            assert!(alloc::format!("{hostile:?}").starts_with("actor startup failed"));
        }

        let captured = crate::AktorFailure::from(AktorError::new(
            diagnostics(&Cause {
                source: None,
                cycle: true,
                panic: false,
            }),
            (),
        ));
        let mut output = Output(String::new());

        output
            .write_str(&captured.diagnostics)
            .expect("captured source walk exceeded its output budget");
        assert!(output.0.ends_with("(source chain truncated)"));
        assert!(captured.diagnostics.contains("source chain truncated"));
        assert!(!alloc::format!("{captured:?}").contains("source chain truncated"));

        let (cyclic, text) = public(Cause {
            source: None,
            cycle: true,
            panic: false,
        });
        let mut output = Output(String::new());

        write!(output, "{text}").expect("source walk exceeded its output budget");
        assert!(output.0.ends_with("(source chain truncated)"));
        assert!(alloc::format!("{cyclic:?}").starts_with("actor startup failed"));
    }
}
