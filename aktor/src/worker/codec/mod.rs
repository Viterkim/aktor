use super::*;
use core::fmt;
use serde::{de, ser};

mod decode;
mod encode;
pub use decode::decode;
pub use encode::encode;

const MAX_DEPTH: usize = 128;

const UNIT: u8 = 0;
const BOOL: u8 = 1;
const I8: u8 = 2;
const U8: u8 = 3;
const I16: u8 = 4;
const U16: u8 = 5;
const I32: u8 = 6;
const U32: u8 = 7;
const I64: u8 = 8;
const U64: u8 = 9;
const I128: u8 = 10;
const U128: u8 = 11;
const F32: u8 = 12;
const F64: u8 = 13;
const CHAR: u8 = 14;
const STR: u8 = 15;
const BYTES: u8 = 16;
const NONE: u8 = 17;
const SOME: u8 = 18;
const SEQ: u8 = 19;
const MAP: u8 = 20;
const NEWTYPE: u8 = 21;
const ENUM: u8 = 22;
const END: u8 = 23;
const UNIT_VARIANT: u8 = 24;

#[derive(Debug)]
struct Error(String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl core::error::Error for Error {}
impl ser::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}
impl de::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

fn error(message: &str) -> Error {
    Error(message.into())
}

fn report(error: Error) -> WorkerError {
    WorkerError::new(CallError::NotAdmitted, WorkerCause::Codec(error.0))
}
