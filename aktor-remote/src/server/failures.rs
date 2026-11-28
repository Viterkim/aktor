use crate::{AktorDeliveryOutcome, Cause};
use aktor::FailureId;
use er::*;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone, Er)]
pub enum ServerError {
    Listen {
        cause: Cause,
    },
    Shutdown {
        #[er(skip)]
        failures: Vec<ServerFailure>,
    },
}

#[derive(Clone, PartialEq, Eq, Er)]
pub enum ServerFailure {
    Cast {
        operation: String,
        outcome: AktorDeliveryOutcome,
        cause: Cause,
        failure_id: Option<FailureId>,
    },
    Session {
        cause: Cause,
    },
    Listener {
        cause: Cause,
    },
    ShutdownTimeout,
}

#[derive(Clone, Default)]
pub(super) struct Failures(Arc<Mutex<Vec<ErTree<ServerFailure>>>>);

impl Failures {
    pub fn record(&self, error: ErTree<ServerFailure>) {
        self.0.lock().push(error);
    }

    pub fn finish(&self) -> Result<(), (ServerError, String)> {
        let errors = std::mem::take(&mut *self.0.lock());
        if errors.is_empty() {
            return Ok(());
        }

        let failures = errors.iter().map(|error| error.top.clone()).collect();
        let mut report = er_new!(ServerError::Shutdown { failures });

        for error in errors {
            report = report.er_add(error);
        }

        Err(report.er_split())
    }
}
