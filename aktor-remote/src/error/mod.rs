use aktor::{AktorData, message::AktorDeliveryOutcome};
use er::*;

mod impls;

/// The retry policy uses the cause to decide whether to reconnect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, AktorData)]
pub enum Cause {
    Connection,
    Timeout,
    Protocol,
    Codec,
    Closed,
}

#[derive(Clone, AktorData)]
pub struct WireFailure {
    pub failure_id: Option<aktor::FailureId>,
    pub outcome: AktorDeliveryOutcome,
    pub cause: Cause,
    pub reason: Rejection,
    pub owner_report: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, AktorData, Er)]
pub enum Rejection {
    ExpectedHello,
    InvalidFrameLimit { limit: u32 },
    ProtocolMismatch { received: u32, expected: u32 },
    BuildMismatch,
    UnsupportedOperation,
    SessionIdentity,
    SessionGeneration { received: u64, previous: u64 },
    ConnectionLimit,
    UnknownOperation,
    AdmissionRefused,
    ActorClosed,
    Dispatch,
    ReplyTooLarge { size: u64, limit: u32 },
    ReplyEncoding,
}
