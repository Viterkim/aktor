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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Error {
    pub outcome: CallError,
    pub cause: Cause,
    pub diagnostics: String,
}
mod impls;
