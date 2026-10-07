use super::{AktorData, MAX_DEPTH, Owned, value_budget};
use crate::{
    message::CallError,
    worker::{WorkerCause, WorkerError},
};
use alloc::{string::ToString, vec::Vec};
use core::{cell::Cell, fmt};
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor},
};

/// Decode compact data with bounded nesting and work, rejecting trailing bytes.
///
/// The value budget is proportional to the encoded size. Extremely large collections
/// of zero-sized values can exceed it even though their encoding occupies few bytes.
pub fn decode<T: AktorData>(bytes: &[u8]) -> Result<T, WorkerError> {
    let remaining = Cell::new(value_budget(bytes.len()));
    let mut decoder = postcard::Deserializer::from_bytes(bytes);
    let value = Owned::<T>::deserialize(Bounded {
        inner: &mut decoder,
        limits: Limits {
            depth: 0,
            remaining: &remaining,
        },
    })
    .map_err(report)?;
    let tail = decoder.finalize().map_err(report)?;

    if !tail.is_empty() {
        return Err(report(postcard::Error::DeserializeBadEncoding));
    }

    Ok(value.0)
}

fn report(error: impl fmt::Display) -> WorkerError {
    WorkerError::new(
        CallError::NotAdmitted,
        WorkerCause::Codec(error.to_string()),
    )
}

#[derive(Clone, Copy)]
struct Limits<'a> {
    depth: usize,
    remaining: &'a Cell<usize>,
}
impl Limits<'_> {
    fn value<E: de::Error>(&self) -> Result<(), E> {
        let remaining = self.remaining.get();

        if remaining == 0 {
            return Err(E::custom("worker data has too many values"));
        }

        self.remaining.set(remaining - 1);
        Ok(())
    }

    fn enter<E: de::Error>(self) -> Result<Self, E> {
        if self.depth >= MAX_DEPTH {
            return Err(E::custom("worker data is nested too deeply"));
        }

        self.value()?;

        Ok(Self {
            depth: self.depth + 1,
            ..self
        })
    }
}

struct Bounded<'a, D> {
    inner: D,
    limits: Limits<'a>,
}

struct Visit<'a, V> {
    inner: V,
    limits: Limits<'a>,
}

struct Access<'a, A> {
    inner: A,
    limits: Limits<'a>,
}

struct Seed<'a, S> {
    inner: S,
    limits: Limits<'a>,
}
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<'_, S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.limits.value()?;

        self.inner.deserialize(Bounded {
            inner: deserializer,
            limits: self.limits,
        })
    }
}

macro_rules! deserialize {
    ($($method:ident $(($($argument:ident: $ty:ty),*))?),* $(,)?) => { $(
        fn $method<V: Visitor<'de>>(self, $($($argument: $ty,)*)? visitor: V) -> Result<V::Value, Self::Error> {
            let limits = self.limits.enter()?;
            self.inner.$method($($($argument,)*)? Visit { inner: visitor, limits })
        }
    )* };
}
impl<'de, D: Deserializer<'de>> Deserializer<'de> for Bounded<'_, D> {
    type Error = D::Error;

    fn is_human_readable(&self) -> bool {
        false
    }

    deserialize!(
        deserialize_any,
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_unit_struct(name: &'static str),
        deserialize_newtype_struct(name: &'static str),
        deserialize_seq,
        deserialize_tuple(len: usize),
        deserialize_tuple_struct(name: &'static str, len: usize),
        deserialize_map,
        deserialize_struct(name: &'static str, fields: &'static [&'static str]),
        deserialize_enum(name: &'static str, variants: &'static [&'static str]),
        deserialize_identifier,
        deserialize_ignored_any
    );
}

macro_rules! visit {
    ($($method:ident($ty:ty)),* $(,)?) => { $(
        fn $method<E: de::Error>(self, value: $ty) -> Result<Self::Value, E> {
            self.inner.$method(value)
        }
    )* };
}
impl<'de, V: Visitor<'de>> Visitor<'de> for Visit<'_, V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(formatter)
    }

    visit!(
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_borrowed_str(&'de str),
        visit_string(alloc::string::String),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>)
    );

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_none()
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.visit_some(Bounded {
            inner: deserializer,
            limits: self.limits,
        })
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(Bounded {
            inner: deserializer,
            limits: self.limits,
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(Access {
            inner: access,
            limits: self.limits,
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(Access {
            inner: access,
            limits: self.limits,
        })
    }

    fn visit_enum<A: EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(Access {
            inner: access,
            limits: self.limits,
        })
    }
}
impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Access<'_, A> {
    type Error = A::Error;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.inner.next_element_seed(Seed {
            inner: seed,
            limits: self.limits,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner
            .size_hint()
            .map(|count| count.min(self.limits.remaining.get()))
    }
}
impl<'de, A: MapAccess<'de>> MapAccess<'de> for Access<'_, A> {
    type Error = A::Error;

    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.inner.next_key_seed(Seed {
            inner: seed,
            limits: self.limits,
        })
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.next_value_seed(Seed {
            inner: seed,
            limits: self.limits,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner
            .size_hint()
            .map(|count| count.min(self.limits.remaining.get()))
    }
}
impl<'a, 'de, A: EnumAccess<'de>> EnumAccess<'de> for Access<'a, A> {
    type Error = A::Error;
    type Variant = Access<'a, A::Variant>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error> {
        let (value, variant) = self.inner.variant_seed(Seed {
            inner: seed,
            limits: self.limits,
        })?;

        Ok((
            value,
            Access {
                inner: variant,
                limits: self.limits,
            },
        ))
    }
}
impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for Access<'_, A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(Seed {
            inner: seed,
            limits: self.limits,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(
            len,
            Visit {
                inner: visitor,
                limits: self.limits.enter()?,
            },
        )
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.struct_variant(
            fields,
            Visit {
                inner: visitor,
                limits: self.limits.enter()?,
            },
        )
    }
}
