use super::*;
use core::fmt;
use serde::{de, ser};

mod decode;
mod encode;
mod impls;
pub use decode::decode;
pub use encode::encode;

#[cfg(any(test, all(target_family = "wasm", target_os = "unknown")))]
pub fn decode_output<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WorkerError> {
    decode(bytes).map_err(|mut error| {
        error.outcome = CallError::OutcomeUnknown;
        error
    })
}

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

fn error(message: &str) -> Error {
    Error(message.into())
}

fn report(error: Error) -> WorkerError {
    WorkerError::new(CallError::NotAdmitted, WorkerCause::Codec(error.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    struct Rejected;
    impl<'de> Deserialize<'de> for Rejected {
        fn deserialize<D: de::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            Err(de::Error::custom("answer refused"))
        }
    }
    impl Serialize for Rejected {
        fn serialize<S: ser::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(ser::Error::custom("input refused"))
        }
    }

    #[test]
    fn answer_stage() {
        assert_eq!(
            encode(&Rejected).unwrap_err().outcome,
            CallError::NotAdmitted
        );

        let bytes = encode(&7u32).unwrap();
        let error = decode_output::<Rejected>(&bytes).err().unwrap();

        assert_eq!(error.outcome, CallError::OutcomeUnknown);
        assert_eq!(
            decode::<Rejected>(&bytes).err().unwrap().outcome,
            CallError::NotAdmitted
        );
    }
}
