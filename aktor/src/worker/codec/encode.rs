use super::*;
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};

struct Encoder {
    bytes: Vec<u8>,
    depth: usize,
}
impl Encoder {
    fn check(&self) -> Result<(), Error> {
        if self.depth >= MAX_DEPTH {
            return Err(error("worker value is nested too deeply"));
        }

        Ok(())
    }

    fn nested<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Error> {
        self.check()?;
        self.depth += 1;

        let result = value.serialize(&mut *self);

        self.depth -= 1;
        result
    }

    fn compound(&mut self, levels: usize) -> Result<Compound<'_>, Error> {
        if self.depth + levels > MAX_DEPTH {
            return Err(error("worker value is nested too deeply"));
        }

        let parent = self.depth;

        self.depth += levels;
        Ok(Compound {
            encoder: self,
            parent,
        })
    }

    fn length(&mut self, len: usize) {
        self.bytes.extend_from_slice(&(len as u64).to_le_bytes());
    }

    fn string(&mut self, v: &str) {
        self.length(v.len());
        self.bytes.extend_from_slice(v.as_bytes());
    }

    fn variant(&mut self, v: &str) {
        self.bytes.push(ENUM);
        self.string(v);
    }
}
impl<'a> ser::Serializer for &'a mut Encoder {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(I8);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(U8);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(I16);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(U16);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(I32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(U32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(I64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(U64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(I128);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(U128);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(F32);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(F64);
        self.bytes.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.check()?;
        self.bytes.extend_from_slice(&[BOOL, u8::from(v)]);
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(CHAR);
        self.bytes.extend_from_slice(&(v as u32).to_le_bytes());
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(STR);
        self.string(v);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(BYTES);
        self.length(v.len());
        self.bytes.extend_from_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), Error> {
        self.check()?;
        self.bytes.push(NONE);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<(), Error> {
        self.bytes.push(SOME);
        self.nested(v)
    }

    fn serialize_unit(self) -> Result<(), Error> {
        self.check()?;
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
        self.nested(v)
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<(), Error> {
        self.check()?;
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
        self.nested(value)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Compound<'a>, Error> {
        let compound = self.compound(1)?;

        compound.encoder.bytes.push(SEQ);
        compound
            .encoder
            .bytes
            .extend_from_slice(&len.map_or(u64::MAX, |len| len as u64).to_le_bytes());
        Ok(compound)
    }

    fn serialize_tuple(self, len: usize) -> Result<Compound<'a>, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Compound<'a>, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
        len: usize,
    ) -> Result<Compound<'a>, Error> {
        let compound = self.compound(2)?;

        compound.encoder.variant(v);
        compound.encoder.bytes.push(SEQ);
        compound.encoder.length(len);
        Ok(compound)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Compound<'a>, Error> {
        let compound = self.compound(1)?;
        compound.encoder.bytes.push(MAP);
        Ok(compound)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, Error> {
        self.serialize_map(None)
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        v: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Error> {
        let compound = self.compound(2)?;

        compound.encoder.variant(v);
        compound.encoder.bytes.push(MAP);
        Ok(compound)
    }
}

struct Compound<'a> {
    encoder: &'a mut Encoder,
    parent: usize,
}
impl Compound<'_> {
    fn finish(self) -> Result<(), Error> {
        self.encoder.bytes.push(END);
        Ok(())
    }
}
impl Drop for Compound<'_> {
    fn drop(&mut self) {
        self.encoder.depth = self.parent;
    }
}
impl SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        ser::Serializer::serialize_str(&mut *self.encoder, key)?;
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}
impl SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        ser::Serializer::serialize_str(&mut *self.encoder, key)?;
        v.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

/// Rejects values nested beyond 128 levels, just like decode.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkerError> {
    let mut encoder = Encoder {
        bytes: Vec::new(),
        depth: 0,
    };

    value.serialize(&mut encoder).map_err(report)?;
    Ok(encoder.bytes)
}
