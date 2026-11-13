use alloc::vec::Vec;
use core::{fmt, marker::PhantomData};
use serde::{
    Deserialize, Deserializer, Serializer,
    de::{self, SeqAccess, Visitor, value::SeqAccessDeserializer},
};

pub fn serialize_slice<T: bytemuck::NoUninit, S: Serializer, const N: usize>(
    values: &[T],
    serializer: S,
    bytes: impl Fn(&T) -> [u8; N],
) -> Result<S::Ok, S::Error> {
    if cfg!(target_endian = "little") || N == 1 {
        let bytes = bytemuck::try_cast_slice(values).map_err(serde::ser::Error::custom)?;

        return serializer.serialize_bytes(bytes);
    }

    let mut output = Vec::with_capacity(values.len() * N);

    for value in values {
        output.extend_from_slice(&bytes(value));
    }

    serializer.serialize_bytes(&output)
}

pub fn deserialize_vec<'de, T, D: Deserializer<'de>, const N: usize>(
    deserializer: D,
    value: impl Fn([u8; N]) -> T,
) -> Result<Vec<T>, D::Error> {
    struct Fixed<T, F, const N: usize> {
        value: F,
        marker: PhantomData<fn() -> T>,
    }
    impl<'de, T, F: Fn([u8; N]) -> T, const N: usize> Visitor<'de> for Fixed<T, F, N> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("fixed-width values as bytes")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            let (values, tail) = bytes.as_chunks::<N>();

            if !tail.is_empty() {
                return Err(E::custom("incomplete fixed-width value"));
            }

            Ok(values.iter().map(|bytes| (self.value)(*bytes)).collect())
        }
    }

    deserializer.deserialize_byte_buf(Fixed {
        value,
        marker: PhantomData,
    })
}

pub fn deserialize_unsigned_bytes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<u8>, D::Error> {
    struct UnsignedBytes;
    impl<'de> Visitor<'de> for UnsignedBytes {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bytes")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Ok(bytes.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
            Ok(bytes)
        }
    }

    deserializer.deserialize_byte_buf(UnsignedBytes)
}

pub fn deserialize_signed_bytes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<i8>, D::Error> {
    struct SignedBytes;
    impl<'de> Visitor<'de> for SignedBytes {
        type Value = Vec<i8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bytes or a sequence of signed bytes")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Ok(bytes.iter().map(|&byte| byte as i8).collect())
        }

        fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
            Ok(bytes.into_iter().map(|byte| byte as i8).collect())
        }

        fn visit_seq<A: SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
            Vec::deserialize(SeqAccessDeserializer::new(sequence))
        }
    }

    deserializer.deserialize_byte_buf(SignedBytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::AktorData;
    use serde::de::value::{Error, SeqDeserializer};

    #[test]
    fn signed_legacy_sequence() {
        let values = [-8, 17];
        let input = SeqDeserializer::<_, Error>::new(values.into_iter());

        assert_eq!(deserialize_signed_bytes(input).unwrap(), values);
    }

    #[test]
    fn signed_human_readable_sequence() {
        let values = [-8, 17];
        let input = SeqDeserializer::<_, Error>::new(values.into_iter());

        assert_eq!(i8::deserialize_vec(input).unwrap(), values);
    }
}
