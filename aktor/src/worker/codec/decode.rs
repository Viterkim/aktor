use super::*;
use serde::de::{DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};

struct Decoder<'de> {
    bytes: &'de [u8],
    depth: usize,
}
impl<'de> Decoder<'de> {
    fn nested<T>(&mut self, visit: impl FnOnce(&mut Self) -> Result<T, Error>) -> Result<T, Error> {
        if self.depth >= MAX_DEPTH {
            return Err(error("worker value is nested too deeply"));
        }

        self.depth += 1;

        let result = visit(self);

        self.depth -= 1;
        result
    }

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
        self.nested(|decoder| match decoder.byte()? {
            UNIT => visitor.visit_unit(),
            BOOL => match decoder.byte()? {
                0 => visitor.visit_bool(false),
                1 => visitor.visit_bool(true),
                _ => Err(error("invalid boolean")),
            },
            I8 => visitor.visit_i8(i8::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<i8>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U8 => visitor.visit_u8(u8::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<u8>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I16 => visitor.visit_i16(i16::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<i16>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U16 => visitor.visit_u16(u16::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<u16>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I32 => visitor.visit_i32(i32::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<i32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U32 => visitor.visit_u32(u32::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<u32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I64 => visitor.visit_i64(i64::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<i64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U64 => visitor.visit_u64(u64::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<u64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            I128 => visitor.visit_i128(i128::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<i128>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            U128 => visitor.visit_u128(u128::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<u128>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            F32 => visitor.visit_f32(f32::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<f32>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            F64 => visitor.visit_f64(f64::from_le_bytes(
                decoder
                    .take(core::mem::size_of::<f64>())?
                    .try_into()
                    .map_err(|_| error("invalid number"))?,
            )),
            CHAR => visitor.visit_char(
                char::from_u32(u32::from_le_bytes(
                    decoder
                        .take(4)?
                        .try_into()
                        .map_err(|_| error("invalid character"))?,
                ))
                .ok_or_else(|| error("invalid character"))?,
            ),
            STR => visitor.visit_borrowed_str(decoder.string()?),
            BYTES => {
                let len = decoder.length()?;
                visitor.visit_borrowed_bytes(decoder.take(len)?)
            }
            NONE => visitor.visit_none(),
            SOME => visitor.visit_some(decoder),
            NEWTYPE => visitor.visit_newtype_struct(decoder),
            SEQ => {
                // Length is a hint, END closes the sequence.
                let len = u64::from_le_bytes(
                    decoder
                        .take(8)?
                        .try_into()
                        .map_err(|_| error("invalid sequence"))?,
                );

                let mut seq = Sequence {
                    decoder,
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
                    decoder,
                    ended: false,
                };
                let value = visitor.visit_map(&mut map)?;

                if !map.ended && map.decoder.byte()? != END {
                    return Err(error("map has extra fields"));
                }

                Ok(value)
            }
            UNIT_VARIANT => visitor.visit_borrowed_str(decoder.string()?),
            ENUM => {
                let variant = decoder.string()?;

                visitor.visit_map(EnumMap {
                    decoder,
                    variant: Some(variant),
                })
            }
            _ => Err(error("invalid worker value tag")),
        })
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.nested(|decoder| match decoder.bytes.first() {
            Some(&NONE) => {
                decoder.byte()?;
                visitor.visit_none()
            }
            Some(&SOME) => {
                decoder.byte()?;
                visitor.visit_some(decoder)
            }
            _ => visitor.visit_some(decoder),
        })
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.nested(|decoder| {
            if decoder.bytes.first() == Some(&NEWTYPE) {
                decoder.byte()?;
            }

            visitor.visit_newtype_struct(decoder)
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.nested(|decoder| match decoder.byte()? {
            tag @ (ENUM | UNIT_VARIANT | STR) => {
                let variant = decoder.string()?;

                visitor.visit_enum(Variant {
                    decoder,
                    variant,
                    payload: tag == ENUM,
                })
            }
            _ => Err(error("invalid enum")),
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64
        char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }
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
        if !self.payload {
            return Err(error("enum variant has no payload"));
        }

        seed.deserialize(&mut *self.decoder)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value, Error> {
        if !self.payload {
            return Err(error("enum variant has no payload"));
        }

        de::Deserializer::deserialize_any(&mut *self.decoder, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        if !self.payload {
            return Err(error("enum variant has no payload"));
        }

        de::Deserializer::deserialize_any(&mut *self.decoder, visitor)
    }
}

/// Rejects values nested beyond 128 levels, including ignored fields.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WorkerError> {
    let mut decoder = Decoder { bytes, depth: 0 };
    let value = T::deserialize(&mut decoder).map_err(report)?;

    if !decoder.bytes.is_empty() {
        return Err(report(error("worker value has trailing bytes")));
    }

    Ok(value)
}
