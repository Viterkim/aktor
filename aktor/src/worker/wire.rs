use super::WorkerError;
#[cfg(feature = "serde")]
use super::{decode, encode};
#[cfg(feature = "serde")]
use crate::dispatch::SerdeCodec;
use crate::{
    data::{self, AktorData},
    dispatch::DataCodec,
    message::CallError,
};
#[cfg(feature = "serde")]
use serde::{Serialize, de::DeserializeOwned};

pub trait Codec<T> {
    const NAME: &'static str;

    fn encode(value: &T) -> Result<Vec<u8>, WorkerError>;
    fn decode(bytes: &[u8]) -> Result<T, WorkerError>;

    fn decode_output(bytes: &[u8]) -> Result<T, WorkerError> {
        Self::decode(bytes).map_err(|mut error| {
            error.outcome = CallError::OutcomeUnknown;
            error
        })
    }
}
#[cfg(feature = "serde")]
impl<T: Serialize + DeserializeOwned> Codec<T> for SerdeCodec {
    const NAME: &'static str = "";

    fn encode(value: &T) -> Result<Vec<u8>, WorkerError> {
        encode(value)
    }

    fn decode(bytes: &[u8]) -> Result<T, WorkerError> {
        decode(bytes)
    }
}

impl<T: AktorData> Codec<T> for DataCodec {
    const NAME: &'static str = " [aktor-data]";

    fn encode(value: &T) -> Result<Vec<u8>, WorkerError> {
        data::encode(value)
    }

    fn decode(bytes: &[u8]) -> Result<T, WorkerError> {
        data::decode(bytes)
    }
}
