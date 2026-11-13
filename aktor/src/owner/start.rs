use super::{AktorStartInvalid, AktorStartKind, AktorThreadEnd, StartError};
use crate::{
    AktorFailure,
    listener::{DedicatedStartError, StartupJoin},
};

fn ended(joined: bool) -> AktorThreadEnd {
    if joined {
        AktorThreadEnd::Joined
    } else {
        AktorThreadEnd::Unconfirmed
    }
}

fn kind(error: &DedicatedStartError<AktorFailure>) -> AktorStartKind {
    match error {
        DedicatedStartError::Startup { cause, .. } => kind(cause),
        DedicatedStartError::Init(_) => AktorStartKind::Setup,
        DedicatedStartError::Panicked { .. } => AktorStartKind::Panicked,
        DedicatedStartError::InvalidCapacity => {
            AktorStartKind::Invalid(AktorStartInvalid::ZeroCapacity)
        }
        DedicatedStartError::NoRuntime => AktorStartKind::Invalid(AktorStartInvalid::NoRuntime),
        DedicatedStartError::NotStarted => {
            AktorStartKind::Invalid(AktorStartInvalid::GroupNotStarted)
        }
        DedicatedStartError::ThreadNotCreated(_)
        | DedicatedStartError::Thread(_)
        | DedicatedStartError::Observer { .. } => AktorStartKind::Spawn,
        DedicatedStartError::Closed => AktorStartKind::Stopping,
    }
}

impl<E> DedicatedStartError<E> {
    pub(crate) fn thread_end(&self) -> AktorThreadEnd {
        match self {
            DedicatedStartError::Startup {
                join: StartupJoin::Joined,
                ..
            }
            | DedicatedStartError::Observer {
                completion: Ok(()), ..
            } => AktorThreadEnd::Joined,
            DedicatedStartError::Startup {
                join: StartupJoin::Failed(cause),
                ..
            } => ended(cause.physically_joined),
            DedicatedStartError::Observer {
                completion: Err(cause),
                ..
            } => ended(cause.physically_joined()),
            DedicatedStartError::Panicked { cause, .. } => ended(cause.physically_joined),
            DedicatedStartError::NoRuntime
            | DedicatedStartError::InvalidCapacity
            | DedicatedStartError::ThreadNotCreated(_)
            | DedicatedStartError::NotStarted => AktorThreadEnd::NeverCreated,
            DedicatedStartError::Thread(_)
            | DedicatedStartError::Init(_)
            | DedicatedStartError::Closed => AktorThreadEnd::Unconfirmed,
        }
    }
}

impl From<DedicatedStartError<AktorFailure>> for StartError {
    fn from(error: DedicatedStartError<AktorFailure>) -> Self {
        let kind = kind(&error);
        let thread = error.thread_end();

        Self {
            kind,
            thread,
            cause: error,
        }
    }
}
impl StartError {
    pub(crate) fn report(&self) -> String {
        let error = &self.cause;
        let mut diagnostics = match error.primary() {
            DedicatedStartError::Init(failure) => Some(failure.diagnostics.clone()),
            DedicatedStartError::ThreadNotCreated(cause) => Some(format!("{cause:#}")),
            DedicatedStartError::Thread(cause) => Some(format!("{cause:#}")),
            DedicatedStartError::Panicked { actor, cause } => {
                Some(format!("actor {actor} initialization panicked:\n{cause}"))
            }
            DedicatedStartError::Startup { .. } => None,
            DedicatedStartError::Observer { cause, .. } => Some(format!("{cause:#}")),
            DedicatedStartError::NotStarted
            | DedicatedStartError::Closed
            | DedicatedStartError::NoRuntime
            | DedicatedStartError::InvalidCapacity => None,
        };

        let secondary = match &error {
            DedicatedStartError::Startup {
                join: StartupJoin::Failed(cause),
                ..
            } => Some(format!("Startup join failed: {cause}")),
            DedicatedStartError::Observer {
                completion: Err(cause),
                ..
            } => Some(format!("Owner completion failed: {}", cause.report())),
            _ => None,
        };

        if let Some(secondary) = secondary {
            match &mut diagnostics {
                Some(diagnostics) => {
                    diagnostics.push('\n');
                    diagnostics.push_str(&secondary);
                }
                None => diagnostics = Some(secondary),
            }
        }

        diagnostics.unwrap_or_else(|| self.to_string())
    }

    pub(crate) fn into_report(self) -> (Self, String) {
        let report = self.report();

        (self, report)
    }
}
