use crate::{AktorCleanupError, AktorError, AktorSetupError};
use alloc::string::String;
use core::fmt;

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

impl<T> fmt::Display for AktorError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.diagnostics)
    }
}
impl<T> fmt::Debug for AktorError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl<T> core::error::Error for AktorError<T> {}
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
