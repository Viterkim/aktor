use alloc::string::String;
use core::fmt;

/// The text you want printed, plus any data you want to keep.
#[cfg_attr(
    feature = "wasm_browser_workers",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, PartialEq, Eq)]
pub struct AktorError<T = ()> {
    pub diagnostics: String,
    pub data: T,
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

pub type AktorSetupError<T = ()> = AktorError<T>;
pub type AktorCleanupError<T = ()> = AktorError<T>;
