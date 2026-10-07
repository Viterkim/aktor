use super::{AktorData, MAX_DEPTH, Ref, value_budget};
use crate::{
    message::CallError,
    worker::{WorkerCause, WorkerError},
};
use alloc::{string::ToString, vec::Vec};
use core::cell::Cell;
use postcard::ser_flavors::{AllocVec, Flavor};
use serde::{
    Serialize, Serializer,
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

/// Encode compact data with bounded nesting and work before admission.
///
/// Work is limited by the bytes already written, with an initial allowance for
/// small values. Large collections of zero-sized values can exceed this budget.
pub fn encode<T: AktorData>(value: &T) -> Result<Vec<u8>, WorkerError> {
    let work = Work {
        values: Cell::new(0),
        bytes: Cell::new(0),
    };

    postcard::serialize_with_flavor(
        &Value {
            inner: &Ref(value),
            depth: 0,
            work: &work,
        },
        Output {
            inner: AllocVec::new(),
            work: &work,
        },
    )
    .map_err(|error| {
        WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Codec(error.to_string()),
        )
    })
}

struct Work {
    values: Cell<usize>,
    bytes: Cell<usize>,
}
impl Work {
    fn value<E: ser::Error>(&self, count: usize) -> Result<(), E> {
        let values = self.values.get().saturating_add(count);

        if values > value_budget(self.bytes.get()) {
            return Err(E::custom("worker data has too many values"));
        }

        self.values.set(values);
        Ok(())
    }
}

struct Output<'a> {
    inner: AllocVec,
    work: &'a Work,
}
impl Flavor for Output<'_> {
    type Output = Vec<u8>;

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        self.inner.try_push(byte)?;
        self.work.bytes.set(self.work.bytes.get().saturating_add(1));
        Ok(())
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        self.inner.try_extend(bytes)?;
        self.work
            .bytes
            .set(self.work.bytes.get().saturating_add(bytes.len()));
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        self.inner.finalize()
    }
}

struct Value<'a, T: ?Sized> {
    inner: &'a T,
    depth: usize,
    work: &'a Work,
}
impl<T: Serialize + ?Sized> Serialize for Value<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.depth >= MAX_DEPTH {
            return Err(ser::Error::custom("worker data is nested too deeply"));
        }

        self.work.value::<S::Error>(2)?;

        self.inner.serialize(Bounded {
            inner: serializer,
            depth: self.depth + 1,
            work: self.work,
        })
    }
}

struct Bounded<'a, S> {
    inner: S,
    depth: usize,
    work: &'a Work,
}

struct Compound<'a, C> {
    inner: C,
    depth: usize,
    work: &'a Work,
}

macro_rules! scalar {
    ($($method:ident($value:ident: $ty:ty)),* $(,)?) => { $(
        fn $method(self, $value: $ty) -> Result<Self::Ok, Self::Error> {
            self.inner.$method($value)
        }
    )* };
}

macro_rules! compound {
    ($($method:ident -> $result:ident ($($argument:ident: $ty:ty),*)),* $(,)?) => { $(
        fn $method(self, $($argument: $ty),*) -> Result<Self::$result, Self::Error> {
            Ok(Compound { inner: self.inner.$method($($argument),*)?, depth: self.depth, work: self.work })
        }
    )* };
}
impl<'a, S: Serializer> Serializer for Bounded<'a, S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Compound<'a, S::SerializeSeq>;
    type SerializeTuple = Compound<'a, S::SerializeTuple>;
    type SerializeTupleStruct = Compound<'a, S::SerializeTupleStruct>;
    type SerializeTupleVariant = Compound<'a, S::SerializeTupleVariant>;
    type SerializeMap = Compound<'a, S::SerializeMap>;
    type SerializeStruct = Compound<'a, S::SerializeStruct>;
    type SerializeStructVariant = Compound<'a, S::SerializeStructVariant>;

    fn is_human_readable(&self) -> bool {
        false
    }

    scalar!(
        serialize_bool(value: bool),
        serialize_i8(value: i8),
        serialize_i16(value: i16),
        serialize_i32(value: i32),
        serialize_i64(value: i64),
        serialize_i128(value: i128),
        serialize_u8(value: u8),
        serialize_u16(value: u16),
        serialize_u32(value: u32),
        serialize_u64(value: u64),
        serialize_u128(value: u128),
        serialize_f32(value: f32),
        serialize_f64(value: f64),
        serialize_char(value: char),
        serialize_str(value: &str),
        serialize_bytes(value: &[u8]),
        serialize_unit_struct(name: &'static str)
    );

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_none()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_unit()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_some(&Value {
            inner: value,
            depth: self.depth,
            work: self.work,
        })
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.inner.serialize_newtype_struct(
            name,
            &Value {
                inner: value,
                depth: self.depth,
                work: self.work,
            },
        )
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        if self.depth >= MAX_DEPTH {
            return Err(ser::Error::custom("worker data is nested too deeply"));
        }

        self.work.value::<S::Error>(1)?;
        self.inner.serialize_unit_variant(name, index, variant)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.work.value::<S::Error>(1)?;
        self.inner.serialize_newtype_variant(
            name,
            index,
            variant,
            &Value {
                inner: value,
                depth: self.depth,
                work: self.work,
            },
        )
    }

    compound!(
        serialize_seq -> SerializeSeq (len: Option<usize>),
        serialize_tuple -> SerializeTuple (len: usize),
        serialize_tuple_struct -> SerializeTupleStruct (name: &'static str, len: usize),
        serialize_map -> SerializeMap (len: Option<usize>),
        serialize_struct -> SerializeStruct (name: &'static str, len: usize)
    );

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        if self.depth >= MAX_DEPTH {
            return Err(ser::Error::custom("worker data is nested too deeply"));
        }

        self.work.value::<S::Error>(2)?;

        Ok(Compound {
            inner: self
                .inner
                .serialize_tuple_variant(name, index, variant, len)?,
            depth: self.depth + 1,
            work: self.work,
        })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        if self.depth >= MAX_DEPTH {
            return Err(ser::Error::custom("worker data is nested too deeply"));
        }

        self.work.value::<S::Error>(2)?;

        Ok(Compound {
            inner: self
                .inner
                .serialize_struct_variant(name, index, variant, len)?,
            depth: self.depth + 1,
            work: self.work,
        })
    }
}

macro_rules! elements {
    ($($trait:ident: $method:ident),* $(,)?) => { $(
        impl<C: $trait> $trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;

            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
                self.inner.$method(&Value { inner: value, depth: self.depth, work: self.work })
            }

            fn end(self) -> Result<Self::Ok, Self::Error> {
                self.inner.end()
            }
        }
    )* };
}
elements!(
    SerializeSeq: serialize_element,
    SerializeTuple: serialize_element,
    SerializeTupleStruct: serialize_field,
    SerializeTupleVariant: serialize_field
);

macro_rules! fields {
    ($($trait:ident),* $(,)?) => { $(
        impl<C: $trait> $trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;

            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                key: &'static str,
                value: &T,
            ) -> Result<(), Self::Error> {
                self.inner.serialize_field(key, &Value {
                    inner: value,
                    depth: self.depth,
                    work: self.work
                })
            }

            fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
                self.inner.skip_field(key)
            }

            fn end(self) -> Result<Self::Ok, Self::Error> {
                self.inner.end()
            }
        }
    )* };
}
fields!(SerializeStruct, SerializeStructVariant);

impl<C: SerializeMap> SerializeMap for Compound<'_, C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.inner.serialize_key(&Value {
            inner: key,
            depth: self.depth,
            work: self.work,
        })
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.inner.serialize_value(&Value {
            inner: value,
            depth: self.depth,
            work: self.work,
        })
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}
