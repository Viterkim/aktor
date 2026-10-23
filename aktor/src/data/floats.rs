use alloc::vec::Vec;
use core::{fmt, marker::PhantomData};
use serde::{
    Deserializer, Serializer,
    de::{self, Visitor},
};

pub fn serialize_slice<T: bytemuck::NoUninit, S: Serializer, const N: usize>(
    values: &[T],
    serializer: S,
    _bytes: impl Fn(&T) -> [u8; N],
) -> Result<S::Ok, S::Error> {
    #[cfg(target_endian = "little")]
    {
        let bytes = bytemuck::try_cast_slice(values).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(bytes)
    }
    #[cfg(target_endian = "big")]
    {
        let mut output = Vec::with_capacity(values.len() * N);

        for value in values {
            output.extend_from_slice(&_bytes(value));
        }

        serializer.serialize_bytes(&output)
    }
}

pub fn deserialize_vec<'de, T, D: Deserializer<'de>, const N: usize>(
    deserializer: D,
    value: impl Fn([u8; N]) -> T,
) -> Result<Vec<T>, D::Error> {
    struct Floats<T, F, const N: usize> {
        value: F,
        marker: PhantomData<fn() -> T>,
    }
    impl<'de, T, F: Fn([u8; N]) -> T, const N: usize> Visitor<'de> for Floats<T, F, N> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("floating point bytes")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            let (values, tail) = bytes.as_chunks::<N>();

            if !tail.is_empty() {
                return Err(E::custom("incomplete floating point value"));
            }

            Ok(values.iter().map(|bytes| (self.value)(*bytes)).collect())
        }
    }

    deserializer.deserialize_bytes(Floats {
        value,
        marker: PhantomData,
    })
}
