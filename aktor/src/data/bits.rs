use alloc::{vec, vec::Vec};
use core::fmt;
use serde::{
    Deserializer, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::SerializeTuple,
};

pub fn serialize_slice<S: Serializer>(values: &[bool], serializer: S) -> Result<S::Ok, S::Error> {
    if values.is_empty() {
        return serializer.serialize_bytes(&[]);
    }

    let length = values.len() / 8 + 1;
    let mut inline = [0; 32];
    let mut heap;
    let bytes = if length <= inline.len() {
        &mut inline[..length]
    } else {
        heap = vec![0; length];
        &mut heap
    };

    for (byte, chunk) in bytes.iter_mut().zip(values.chunks(8)) {
        *byte = pack(chunk);
    }

    bytes[length - 1] |= 1 << (values.len() % 8);
    serializer.serialize_bytes(bytes)
}

pub fn deserialize_vec<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<bool>, D::Error> {
    struct Vector;
    impl<'de> Visitor<'de> for Vector {
        type Value = Vec<bool>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("packed booleans")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            let Some((&last, bytes)) = bytes.split_last() else {
                return Ok(Vec::new());
            };

            if last == 0 {
                return Err(E::custom("missing boolean end bit"));
            }

            let remaining = 7 - last.leading_zeros() as usize;
            let count = bytes
                .len()
                .checked_mul(8)
                .and_then(|count| count.checked_add(remaining))
                .ok_or_else(|| E::custom("boolean count overflow"))?;
            let mut values = Vec::with_capacity(count);

            for byte in bytes {
                values.extend((0..8).map(|bit| byte & (1 << bit) != 0));
            }

            values.extend((0..remaining).map(|bit| last & (1 << bit) != 0));
            Ok(values)
        }
    }

    deserializer.deserialize_bytes(Vector)
}

pub fn serialize_array<S: Serializer, const N: usize>(
    values: &[bool; N],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut tuple = serializer.serialize_tuple(N.div_ceil(8))?;

    for chunk in values.chunks(8) {
        tuple.serialize_element(&pack(chunk))?;
    }

    tuple.end()
}

pub fn deserialize_array<'de, D: Deserializer<'de>, const N: usize>(
    deserializer: D,
) -> Result<[bool; N], D::Error> {
    struct Array<const N: usize>;
    impl<'de, const N: usize> Visitor<'de> for Array<N> {
        type Value = [bool; N];

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a packed boolean array")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = [false; N];

            for chunk in values.chunks_mut(8) {
                let byte = sequence
                    .next_element::<u8>()?
                    .ok_or_else(|| de::Error::custom("truncated boolean data"))?;

                if chunk.len() < 8 && byte >> chunk.len() != 0 {
                    return Err(de::Error::custom("nonzero padding in boolean data"));
                }

                for (bit, value) in chunk.iter_mut().enumerate() {
                    *value = byte & (1 << bit) != 0;
                }
            }

            Ok(values)
        }
    }

    deserializer.deserialize_tuple(N.div_ceil(8), Array)
}

fn pack(values: &[bool]) -> u8 {
    values
        .iter()
        .enumerate()
        .fold(0_u8, |byte, (bit, value)| byte | (u8::from(*value) << bit))
}
