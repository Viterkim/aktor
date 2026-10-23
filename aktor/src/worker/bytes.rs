//! Serde `with` helpers for byte buffers.

use core::fmt;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
};

struct Bytes<'a>(&'a [u8]);
impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

struct OwnedBytes(Vec<u8>);
impl<'de> Deserialize<'de> for OwnedBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize(deserializer).map(Self)
    }
}

pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(bytes)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct ByteVisitor;
    impl<'de> Visitor<'de> for ByteVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("worker bytes")
        }

        fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Ok(bytes.to_vec())
        }

        fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
            Ok(bytes)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
            Vec::deserialize(serde::de::value::SeqAccessDeserializer::new(sequence))
        }
    }

    deserializer.deserialize_byte_buf(ByteVisitor)
}

pub mod result {
    use super::*;

    pub fn serialize<S: Serializer, E: Serialize>(
        value: &Result<Vec<u8>, E>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|bytes| Bytes(bytes))
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>, E: Deserialize<'de>>(
        deserializer: D,
    ) -> Result<Result<Vec<u8>, E>, D::Error> {
        Result::<OwnedBytes, E>::deserialize(deserializer).map(|result| result.map(|bytes| bytes.0))
    }
}
