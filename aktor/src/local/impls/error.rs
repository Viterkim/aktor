use super::super::*;

impl<E> OwnerError<E> {
    pub fn without_data(&self) -> OwnerError {
        match self {
            Self::Setup(_) => OwnerError::Setup(()),
            Self::SetupDiagnostic { id } => OwnerError::SetupDiagnostic { id: *id },
            Self::SetupPanic => OwnerError::SetupPanic,
            Self::Cleanup(_) => OwnerError::Cleanup(()),
            Self::CleanupFailure { id } => OwnerError::CleanupFailure { id: *id },
            Self::CleanupUnconfirmed => OwnerError::CleanupUnconfirmed,
            Self::Runner => OwnerError::Runner,
            Self::Cancelled => OwnerError::Cancelled,
        }
    }

    pub(in crate::local) fn observed(self) -> Observed<E> {
        self.with_report(String::new())
    }

    pub(in crate::local) fn with_report(self, report: String) -> Observed<E> {
        let (_, report) = crate::report::with_report(self.without_data(), report);

        (Rc::new(self), report)
    }
}

impl<E> Clone for SharedOwnerError<E> {
    fn clone(&self) -> Self {
        Self {
            cause: self.cause.clone(),
        }
    }
}
impl<E> SharedOwnerError<E> {
    pub(in crate::local) fn from_failure(
        (error, report): Observed<E>,
        secondary: &[String],
    ) -> (Self, String) {
        let report = match secondary {
            [] => report,
            _ => {
                let mut tree = error.without_data().er_save("owner", report);
                for report in secondary {
                    tree = tree
                        .er_add(crate::report::DuringTeardown.er_save("report", report.clone()));
                }
                tree.er_report()
            }
        };

        (Self { cause: error }, report)
    }

    pub fn without_data(self) -> OwnerError {
        self.cause.without_data()
    }
}
