use crate::{AktorCleanupError, AktorError, AktorSetupError};
use alloc::string::String;

pub fn aktor_err_setup<T>(diagnostics: impl Into<String>, data: T) -> AktorSetupError<T> {
    AktorError {
        diagnostics: diagnostics.into(),
        data,
    }
}

pub fn aktor_err_cleanup<T>(diagnostics: impl Into<String>, data: T) -> AktorCleanupError<T> {
    AktorError {
        diagnostics: diagnostics.into(),
        data,
    }
}

impl AktorError {
    pub fn new(diagnostics: impl Into<String>) -> Self {
        Self {
            diagnostics: diagnostics.into(),
            data: (),
        }
    }
}
impl<T> AktorError<T> {
    pub fn report(&self) -> AktorError {
        AktorError {
            diagnostics: self.diagnostics.clone(),
            data: (),
        }
    }
}
