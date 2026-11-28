use super::*;

impl WireFailure {
    pub fn rejected(outcome: AktorDeliveryOutcome, cause: Cause, reason: Rejection) -> Self {
        Self {
            failure_id: None,
            outcome,
            cause,
            reason,
            owner_report: None,
        }
    }

    pub fn closed(error: aktor::AktorDeliveryError, reason: Rejection) -> Self {
        Self {
            failure_id: error.failure_id,
            outcome: error.outcome,
            cause: Cause::Closed,
            reason,
            owner_report: error.failure_id.map(|_| error.report),
        }
    }

    pub fn received(self) -> ErTree<Rejection> {
        let error = er_new!(self.reason);
        match self.owner_report {
            Some(report) => error.er_save(
                if self.failure_id.is_some() {
                    "actor"
                } else {
                    "server"
                },
                report,
            ),
            None => error,
        }
    }
}
