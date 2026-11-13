use aktor::message::CallError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cause {
    Connection,
    Timeout,
    Protocol,
    Codec,
    Closed,
}

/// What went wrong with a remote call or connection. Callers get `(Error, String)`,
/// so a plain `.er(..)` keeps both the error and the report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub outcome: CallError,
    pub cause: Cause,
}

/// The error and its report as they travel within the crate and over the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Failure {
    pub outcome: CallError,
    pub cause: Cause,
    pub diagnostics: String,
}
mod impls;
