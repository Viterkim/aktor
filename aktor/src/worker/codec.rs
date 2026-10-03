use super::*;
use core::fmt;
use serde::{
    de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor},
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

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

struct Encoder {
    bytes: Vec<u8>,
}
impl Encoder {
    fn length(&mut self, len: usize) {
        self.bytes.extend_from_slice(&(len as u64).to_le_bytes());
    }
    fn string(&mut self, v: &str) {
        self.length(v.len());
        self.bytes.extend_from_slice(v.as_bytes());
    }
    fn end(&mut self) -> Result<(), Error> {
        self.bytes.push(END);
        Ok(())
    }
    fn variant(&mut self, v: &str) {
        self.bytes.push(ENUM);
        self.string(v);
    }
}
impl ser::Serializer for &mut Encoder {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    fn is_human_readable(&self) -> bool {
        false
    }
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.bytes.push(I8);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.bytes.push(U8);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.bytes.push(I16);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.bytes.push(U16);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.bytes.push(I32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.bytes.push(U32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        self.bytes.push(I64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        self.bytes.push(U64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_i128(self, v: i128) -> Result<(), Error> {
        self.bytes.push(I128);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u128(self, v: u128) -> Result<(), Error> {
        self.bytes.push(U128);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.bytes.push(F32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.bytes.push(F64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.bytes.extend_from_slice(&[BOOL, u8::from(v)]);
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.bytes.push(CHAR);
        self.bytes.extend_from_slice(&(v as u32).to_le_bytes());
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.bytes.push(STR);
        self.string(v);
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.bytes.push(BYTES);
        self.length(v.len());
        self.bytes.extend_from_slice(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.bytes.push(NONE);
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<(), Error> {
        self.bytes.push(SOME);
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        self.bytes.push(UNIT);
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        self.serialize_unit()
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        self.bytes.push(NEWTYPE);
        v.serialize(self)
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<(), Error> {
        self.bytes.push(UNIT_VARIANT);
        self.string(v);
        Ok(())
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.variant(v);
        value.serialize(self)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self, Error> {
        self.bytes.push(SEQ);
        self.bytes
            .extend_from_slice(&len.map_or(u64::MAX, |len| len as u64).to_le_bytes());
        Ok(self)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Self, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
        len: usize,
    ) -> Result<Self, Error> {
        self.variant(v);
        self.serialize_seq(Some(len))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, Error> {
        self.bytes.push(MAP);
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        self.serialize_map(None)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
        _: usize,
    ) -> Result<Self, Error> {
        self.variant(v);
        self.serialize_map(None)
    }
}
impl SerializeSeq for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeTuple for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeTupleStruct for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeTupleVariant for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeMap for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeStruct for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        ser::Serializer::serialize_str(&mut **self, key)?;
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}
impl SerializeStructVariant for &mut Encoder {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        ser::Serializer::serialize_str(&mut **self, key)?;
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Encoder::end(self)
    }
}

struct Decoder<'de> {
    bytes: &'de [u8],
}
impl<'de> Decoder<'de> {
    fn take(&mut self, len: usize) -> Result<&'de [u8], Error> {
        if len > self.bytes.len() {
            return Err(error("incomplete worker value"));
        }
        let (value, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn length(&mut self) -> Result<usize, Error> {
        usize::try_from(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| error("invalid length"))?,
        ))
        .map_err(|_| error("worker value is too large"))
    }
    fn string(&mut self) -> Result<&'de str, Error> {
        let len = self.length()?;
        core::str::from_utf8(self.take(len)?).map_err(|_| error("invalid worker string"))
    }
}
impl<'de> de::Deserializer<'de> for &mut Decoder<'de> {
    type Error = Error;
    fn is_human_readable(&self) -> bool {
        false
    }
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.byte()? {
            UNIT => visitor.visit_unit(),
            BOOL => match self.byte()? {
                0 => visitor.visit_bool(false),
                1 => visitor.visit_bool(true),
                _ => Err(error("invalid boolean")),
            },
            I8 => visitor.visit_i8(i8::from_le_bytes(
                self.take(core::mem::size_of::<i8>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U8 => visitor.visit_u8(u8::from_le_bytes(
                self.take(core::mem::size_of::<u8>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I16 => visitor.visit_i16(i16::from_le_bytes(
                self.take(core::mem::size_of::<i16>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U16 => visitor.visit_u16(u16::from_le_bytes(
                self.take(core::mem::size_of::<u16>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I32 => visitor.visit_i32(i32::from_le_bytes(
                self.take(core::mem::size_of::<i32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U32 => visitor.visit_u32(u32::from_le_bytes(
                self.take(core::mem::size_of::<u32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I64 => visitor.visit_i64(i64::from_le_bytes(
                self.take(core::mem::size_of::<i64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U64 => visitor.visit_u64(u64::from_le_bytes(
                self.take(core::mem::size_of::<u64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I128 => visitor.visit_i128(i128::from_le_bytes(
                self.take(core::mem::size_of::<i128>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U128 => visitor.visit_u128(u128::from_le_bytes(
                self.take(core::mem::size_of::<u128>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            F32 => visitor.visit_f32(f32::from_le_bytes(
                self.take(core::mem::size_of::<f32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            F64 => visitor.visit_f64(f64::from_le_bytes(
                self.take(core::mem::size_of::<f64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            CHAR => visitor.visit_char(
                char::from_u32(u32::from_le_bytes(
                    self.take(4)?
                        .try_into()
                        .map_err(|_| error("invalid character"))?,
                ))
                .ok_or_else(|| error("invalid character"))?,
            ),
            STR => visitor.visit_borrowed_str(self.string()?),
            BYTES => {
                let len = self.length()?;
                visitor.visit_borrowed_bytes(self.take(len)?)
            }
            NONE => visitor.visit_none(),
            SOME => visitor.visit_some(self),
            NEWTYPE => visitor.visit_newtype_struct(self),
            SEQ => {
                let len = u64::from_le_bytes(
                    self.take(8)?
                        .try_into()
                        .map_err(|_| error("invalid sequence"))?,
                );
                let mut seq = Sequence {
                    decoder: self,
                    ended: false,
                    len: usize::try_from(len).ok(),
                };
                let value = visitor.visit_seq(&mut seq)?;
                if !seq.ended && seq.decoder.byte()? != END {
                    return Err(error("sequence has extra elements"));
                }
                Ok(value)
            }
            MAP => {
                let mut map = Mapping {
                    decoder: self,
                    ended: false,
                };
                let value = visitor.visit_map(&mut map)?;
                if !map.ended && map.decoder.byte()? != END {
                    return Err(error("map has extra fields"));
                }
                Ok(value)
            }
            UNIT_VARIANT => visitor.visit_borrowed_str(self.string()?),
            ENUM => {
                let variant = self.string()?;
                visitor.visit_map(EnumMap {
                    decoder: self,
                    variant: Some(variant),
                })
            }
            _ => Err(error("invalid worker value tag")),
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.bytes.first() {
            Some(&NONE) => {
                self.byte()?;
                visitor.visit_none()
            }
            Some(&SOME) => {
                self.byte()?;
                visitor.visit_some(self)
            }
            _ => visitor.visit_some(self),
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        if self.bytes.first() == Some(&NEWTYPE) {
            self.byte()?;
        }
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        match self.byte()? {
            tag @ (ENUM | UNIT_VARIANT | STR) => {
                let variant = self.string()?;
                visitor.visit_enum(Variant {
                    decoder: self,
                    variant,
                    payload: tag == ENUM,
                })
            }
            _ => Err(error("invalid enum")),
        }
    }
    serde::forward_to_deserialize_any! {bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map struct identifier ignored_any}
}

struct Sequence<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    ended: bool,
    len: Option<usize>,
}
impl<'de> SeqAccess<'de> for &mut Sequence<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.ended {
            return Ok(None);
        }
        if self.decoder.bytes.first() == Some(&END) {
            self.decoder.byte()?;
            self.ended = true;
            return Ok(None);
        }
        self.len = self.len.map(|len| len.saturating_sub(1));
        seed.deserialize(&mut *self.decoder).map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        self.len.map(|len| len.min(self.decoder.bytes.len()))
    }
}
struct Mapping<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    ended: bool,
}
impl<'de> MapAccess<'de> for &mut Mapping<'_, 'de> {
    type Error = Error;
    fn next_key_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.ended {
            return Ok(None);
        }
        if self.decoder.bytes.first() == Some(&END) {
            self.decoder.byte()?;
            self.ended = true;
            return Ok(None);
        }
        seed.deserialize(&mut *self.decoder).map(Some)
    }
    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(&mut *self.decoder)
    }
}
struct EnumMap<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    variant: Option<&'de str>,
}
impl<'de> MapAccess<'de> for EnumMap<'_, 'de> {
    type Error = Error;
    fn next_key_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        self.variant
            .take()
            .map(|v| seed.deserialize(de::value::BorrowedStrDeserializer::<Error>::new(v)))
            .transpose()
    }
    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(&mut *self.decoder)
    }
}
struct Variant<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    variant: &'de str,
    payload: bool,
}
impl<'a, 'de> EnumAccess<'de> for Variant<'a, 'de> {
    type Error = Error;
    type Variant = Self;
    fn variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<(T::Value, Self), Error> {
        let variant = seed.deserialize(de::value::BorrowedStrDeserializer::<Error>::new(
            self.variant,
        ))?;
        Ok((variant, self))
    }
}
impl<'de> VariantAccess<'de> for Variant<'_, 'de> {
    type Error = Error;
    fn unit_variant(self) -> Result<(), Error> {
        if self.payload {
            serde::Deserialize::deserialize(&mut *self.decoder)
        } else {
            Ok(())
        }
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(&mut *self.decoder)
    }
    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_any(&mut *self.decoder, visitor)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_any(&mut *self.decoder, visitor)
    }
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkerError> {
    let mut encoder = Encoder { bytes: Vec::new() };
    value.serialize(&mut encoder).map_err(report)?;
    Ok(encoder.bytes)
}
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WorkerError> {
    let mut decoder = Decoder { bytes };
    let value = T::deserialize(&mut decoder).map_err(report)?;
    if !decoder.bytes.is_empty() {
        return Err(report(error("worker value has trailing bytes")));
    }
    Ok(value)
}
