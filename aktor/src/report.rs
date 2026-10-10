use crate::{AktorError, AktorFailure};
use core::{error::Error, fmt};

impl From<AktorError> for AktorFailure {
    fn from(error: AktorError) -> Self {
        Self {
            diagnostics: error.diagnostics,
            primary: false,
        }
    }
}
impl AktorFailure {
    pub fn report(&self) -> AktorError {
        AktorError::new(self.diagnostics.clone())
    }
}
impl fmt::Display for AktorFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actor lifecycle failed")
    }
}
impl fmt::Debug for AktorFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AktorFailure")
            .field("diagnostics", &self.diagnostics)
            .finish()
    }
}
impl Error for AktorFailure {}
