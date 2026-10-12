use crate::{AktorError, AktorFailure};

impl From<AktorError> for AktorFailure {
    fn from(error: AktorError) -> Self {
        Self {
            diagnostics: error.diagnostics,
            primary: false,
        }
    }
}
