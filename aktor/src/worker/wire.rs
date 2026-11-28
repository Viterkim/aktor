use crate::{
    data::{self, AktorData, DataError},
    dispatch::DataCodec,
};
use er::*;

pub(super) fn codec<T, E: core::error::Error + 'static>(
    result: Result<T, (DataError, String)>,
    context: impl FnOnce() -> E,
) -> ErResult<T, E> {
    match result {
        Ok(value) => Ok(value),
        Err((cause, report)) if report.is_empty() => er_bail!(context(), cause),
        Err((_, report)) => er_bail!(context().er_save("codec", report)),
    }
}

pub trait Codec<T> {
    const NAME: &'static str;

    fn encode(value: &T) -> Result<Vec<u8>, (DataError, String)>;
    fn decode(bytes: &[u8]) -> Result<T, (DataError, String)>;
}
// Same bytes and same handshake name as the codec it wraps.
impl<T, C: Codec<T>> Codec<T> for crate::dispatch::RemoteCodec<C> {
    const NAME: &'static str = C::NAME;

    fn encode(value: &T) -> Result<Vec<u8>, (DataError, String)> {
        C::encode(value)
    }

    fn decode(bytes: &[u8]) -> Result<T, (DataError, String)> {
        C::decode(bytes)
    }
}

impl<T: AktorData> Codec<T> for DataCodec {
    const NAME: &'static str = " [aktor-data]";

    fn encode(value: &T) -> Result<Vec<u8>, (DataError, String)> {
        data::encode(value)
    }

    fn decode(bytes: &[u8]) -> Result<T, (DataError, String)> {
        data::decode(bytes)
    }
}
