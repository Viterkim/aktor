use super::types::DataReason;
use super::{
    AktorData, AktorMaxLen, DataError, Reader, Writer, bits, charge, fixed, sealed::Sealed,
};
use alloc::{
    borrow::Cow,
    boxed::Box,
    collections::{BTreeMap, BTreeSet, VecDeque},
    string::String,
    vec::Vec,
};
#[cfg(feature = "std")]
use core::hash::{BuildHasher, Hash};
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

impl DataError {
    #[cold]
    pub fn new(reason: impl Into<Cow<'static, str>>) -> Self {
        Self {
            reason: Box::new(DataReason::Message(reason.into())),
        }
    }
}

impl core::error::Error for DataError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.reason.source()
    }
}
impl From<core::num::TryFromIntError> for DataError {
    fn from(error: core::num::TryFromIntError) -> Self {
        Self {
            reason: Box::new(DataReason::Integer(error)),
        }
    }
}
impl From<core::convert::Infallible> for DataError {
    fn from(error: core::convert::Infallible) -> Self {
        match error {}
    }
}
impl From<core::str::Utf8Error> for DataError {
    fn from(error: core::str::Utf8Error) -> Self {
        Self {
            reason: Box::new(DataReason::Utf8(error)),
        }
    }
}

impl From<DataReason> for DataError {
    fn from(reason: DataReason) -> Self {
        Self {
            reason: Box::new(reason),
        }
    }
}

impl AktorData for core::num::NonZeroUsize {
    const MEMORY: usize = usize::MEMORY;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        self.get().encode_data(writer)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let value = usize::decode_data(reader)?;
        Ok(Self::try_from(value)?)
    }
}

impl AktorData for core::convert::Infallible {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        match *self {}
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Err(DataError::from(super::DataReason::Infallible))
    }
}

pub fn encode_slice<T: AktorData>(values: &[T], writer: &mut Writer) -> Result<(), DataError> {
    writer.length(values.len());
    values.iter().try_for_each(|value| writer.write(value))
}

// No size hints on purpose. A count reserves room before a single element arrived, and nested
// collections would each reserve on their own. Growing as charged elements show up keeps the
// memory budget honest.
pub fn decode_vec<T: AktorData>(reader: &mut Reader<'_>, max: usize) -> Result<Vec<T>, DataError> {
    let count = reader.length()?;
    super::within(count, max, "values")?;

    let mut values = Vec::new();

    for _ in 0..count {
        values.push(reader.read()?);
    }

    Ok(values)
}

pub fn decode_array<T: AktorData, const N: usize>(
    reader: &mut Reader<'_>,
) -> Result<[T; N], DataError> {
    let mut values = Vec::with_capacity(N);

    for _ in 0..N {
        values.push(reader.read()?);
    }

    match values.try_into() {
        Ok(array) => Ok(array),
        Err(_) => Err(DataError::from(super::DataReason::ArrayLength)),
    }
}

fn copy_array<T: AktorData + Copy + Default, const N: usize>(
    reader: &mut Reader<'_>,
) -> Result<[T; N], DataError> {
    let mut values = [T::default(); N];

    for value in &mut values {
        *value = reader.read()?;
    }

    Ok(values)
}

impl AktorData for () {
    const MEMORY: usize = 0;

    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Ok(())
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(())
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        copy_array(reader)
    }
}

macro_rules! unsigned {
    ($($ty:ty),* $(,)?) => { $(
        impl AktorData for $ty {
            const MEMORY: usize = 0;

            #[inline]
            fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
                writer.unsigned(*self as u64);
                Ok(())
            }

            #[inline]
            fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
                Ok(Self::try_from(reader.unsigned()?)?)
            }

            fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
                copy_array(reader)
            }
        }
    )* };
}
unsigned!(u16, u32, u64, usize);

macro_rules! signed {
    ($($ty:ty),* $(,)?) => { $(
        impl AktorData for $ty {
            const MEMORY: usize = 0;

            #[inline]
            fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
                writer.signed(*self as i64);
                Ok(())
            }

            #[inline]
            fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
                Ok(Self::try_from(reader.signed()?)?)
            }

            fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
                copy_array(reader)
            }
        }
    )* };
}
signed!(i16, i32, i64, isize);

impl AktorData for u128 {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.unsigned_wide(*self);
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        reader.unsigned_wide()
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        copy_array(reader)
    }
}

impl AktorData for i128 {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.signed_wide(*self);
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        reader.signed_wide()
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        copy_array(reader)
    }
}

impl AktorData for char {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.unsigned(u64::from(*self));
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let value = reader.unsigned()?;
        u32::try_from(value)
            .ok()
            .and_then(char::from_u32)
            .ok_or_else(|| DataError::from(super::DataReason::Character(value)))
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        copy_array(reader)
    }
}

impl AktorData for bool {
    const MEMORY: usize = 0;

    #[inline]
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.bytes.push(u8::from(*self));
        Ok(())
    }

    #[inline]
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        match reader.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(DataError::from(super::DataReason::Boolean(value))),
        }
    }

    fn encode_slice(values: &[Self], writer: &mut Writer) -> Result<(), DataError> {
        bits::encode_slice(values, writer);
        Ok(())
    }

    fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<Self>, DataError> {
        bits::decode_vec(reader, max)
    }

    fn encode_array<const N: usize>(
        values: &[Self; N],
        writer: &mut Writer,
    ) -> Result<(), DataError> {
        bits::encode_array(values, writer);
        Ok(())
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        bits::decode_array(reader)
    }
}

impl AktorData for u8 {
    const MEMORY: usize = 0;

    #[inline]
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.bytes.push(*self);
        Ok(())
    }

    #[inline]
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        reader.byte()
    }

    fn encode_slice(values: &[Self], writer: &mut Writer) -> Result<(), DataError> {
        writer.slice(values);
        Ok(())
    }

    fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<Self>, DataError> {
        let bytes = reader.slice()?;
        super::within(bytes.len(), max, "values")?;

        Ok(bytes.to_vec())
    }

    fn encode_array<const N: usize>(
        values: &[Self; N],
        writer: &mut Writer,
    ) -> Result<(), DataError> {
        writer.bytes.extend_from_slice(values);
        Ok(())
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        reader.array()
    }
}

impl AktorData for i8 {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.bytes.push(*self as u8);
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(reader.byte()? as i8)
    }

    fn encode_slice(values: &[Self], writer: &mut Writer) -> Result<(), DataError> {
        fixed::encode_slice(values, writer, |value| value.to_le_bytes());
        Ok(())
    }

    fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<Self>, DataError> {
        let bytes = reader.slice()?;
        super::within(bytes.len(), max, "values")?;

        Ok(bytes.iter().map(|&byte| byte as i8).collect())
    }

    fn encode_array<const N: usize>(
        values: &[Self; N],
        writer: &mut Writer,
    ) -> Result<(), DataError> {
        writer.bytes.extend(values.iter().map(|&value| value as u8));
        Ok(())
    }

    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        Ok(reader.array::<N>()?.map(|byte| byte as i8))
    }
}

macro_rules! float {
    ($($ty:ty),* $(,)?) => { $(
        impl AktorData for $ty {
            const MEMORY: usize = 0;

            #[inline]
            fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
                writer.bytes.extend_from_slice(&self.to_le_bytes());
                Ok(())
            }

            #[inline]
            fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
                Ok(Self::from_le_bytes(reader.array()?))
            }

            fn encode_slice(values: &[Self], writer: &mut Writer) -> Result<(), DataError> {
                fixed::encode_slice(values, writer, |value| value.to_le_bytes());
                Ok(())
            }

            fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<Self>, DataError> {
                fixed::decode_vec(reader, max, Self::from_le_bytes)
            }

            fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
                copy_array(reader)
            }
        }
    )* };
}
float!(f32, f64);

impl AktorData for String {
    const MEMORY: usize = 0;

    #[inline]
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.slice(self.as_bytes());
        Ok(())
    }

    #[inline]
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        decode_str(reader).map(String::from)
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        decode_str_max(reader, max).map(String::from)
    }
}

fn decode_str<'a>(reader: &mut Reader<'a>) -> Result<&'a str, DataError> {
    decode_str_max(reader, usize::MAX)
}

fn decode_str_max<'a>(reader: &mut Reader<'a>, max: usize) -> Result<&'a str, DataError> {
    let bytes = reader.slice()?;
    super::within(bytes.len(), max, "bytes")?;

    Ok(core::str::from_utf8(bytes)?)
}

impl Sealed for String {}
impl Sealed for Box<str> {}
impl Sealed for Cow<'_, str> {}
impl<T> Sealed for Vec<T> {}
impl<T> Sealed for Box<[T]> {}
impl<T: Clone> Sealed for Cow<'_, [T]> {}

impl AktorMaxLen for String {
    const UNIT: &'static str = "bytes";

    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl AktorMaxLen for Box<str> {
    const UNIT: &'static str = "bytes";

    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl AktorMaxLen for Cow<'_, str> {
    const UNIT: &'static str = "bytes";

    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl<T> AktorMaxLen for Vec<T> {
    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl<T> AktorMaxLen for Box<[T]> {
    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl<T: Clone> AktorMaxLen for Cow<'_, [T]> {
    fn max_len_count(&self) -> usize {
        self.len()
    }
}

impl AktorData for Box<str> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.slice(self.as_bytes());
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        decode_str(reader).map(Box::from)
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        decode_str_max(reader, max).map(Box::from)
    }
}

// Decoding always owns the text, so any lifetime works and a borrowing message type can
// decode as its owned self.
impl AktorData for Cow<'_, str> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.slice(self.as_bytes());
        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        decode_str(reader).map(|text| Cow::Owned(text.into()))
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        decode_str_max(reader, max).map(|text| Cow::Owned(text.into()))
    }
}

impl<T: AktorData + Clone> AktorData for Cow<'_, [T]> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        T::encode_slice(self, writer)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        T::decode_vec(reader, usize::MAX).map(Cow::Owned)
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        T::decode_vec(reader, max).map(Cow::Owned)
    }
}

impl<T: AktorData> AktorData for Vec<T> {
    const MEMORY: usize = 0;

    #[inline]
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        T::encode_slice(self, writer)
    }

    #[inline]
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        T::decode_vec(reader, usize::MAX)
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        T::decode_vec(reader, max)
    }
}

impl<T: AktorData> AktorData for Box<[T]> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        T::encode_slice(self, writer)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        T::decode_vec(reader, usize::MAX).map(Vec::into_boxed_slice)
    }

    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        T::decode_vec(reader, max).map(Vec::into_boxed_slice)
    }
}

impl<T: AktorData> AktorData for Option<T> {
    #[inline]
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        match self {
            Some(value) => {
                writer.bytes.push(1);
                writer.write(value)
            }
            None => {
                writer.bytes.push(0);
                Ok(())
            }
        }
    }

    #[inline]
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        match reader.byte()? {
            0 => Ok(None),
            1 => reader.read().map(Some),
            value => Err(DataError::from(super::DataReason::OptionTag(value))),
        }
    }
}

impl<T: AktorData, E: AktorData> AktorData for Result<T, E> {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        match self {
            Ok(value) => {
                writer.bytes.push(0);
                writer.write(value)
            }
            Err(error) => {
                writer.bytes.push(1);
                writer.write(error)
            }
        }
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        match reader.byte()? {
            0 => reader.read().map(Ok),
            1 => reader.read().map(Err),
            value => Err(DataError::from(super::DataReason::ResultTag(value))),
        }
    }
}

// The box's own allocation counts too, the pointer was already counted by whoever holds it.
impl<T: AktorData> AktorData for Box<T> {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        charge(T::MEMORY);
        self.as_ref().encode_data(writer)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        if !charge(T::MEMORY) {
            return Err(DataError::from(super::DataReason::Memory(
                reader.memory_limit,
            )));
        }

        T::decode_data(reader).map(Box::new)
    }
}

impl<T: AktorData, const N: usize> AktorData for [T; N] {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        T::encode_array(self, writer)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        T::decode_array(reader)
    }
}

macro_rules! tuple {
    ($(($($ty:ident:$index:tt),+)),* $(,)?) => { $(
        impl<$($ty: AktorData),+> AktorData for ($($ty,)+) {
            fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
                $(writer.write(&self.$index)?;)+
                Ok(())
            }

            fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
                Ok(($(reader.read::<$ty>()?,)+))
            }
        }
    )* };
}
tuple!(
    (A:0),
    (A:0,B:1),
    (A:0,B:1,C:2),
    (A:0,B:1,C:2,D:3),
    (A:0,B:1,C:2,D:3,E:4),
    (A:0,B:1,C:2,D:3,E:4,F:5),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11)
);

impl<T: AktorData> AktorData for VecDeque<T> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.length(self.len());
        self.iter().try_for_each(|value| writer.write(value))
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        decode_vec(reader, usize::MAX).map(VecDeque::from)
    }
}

impl<T: AktorData + Ord> AktorData for BTreeSet<T> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.length(self.len());
        self.iter().try_for_each(|value| writer.write(value))
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let count = reader.length()?;
        let mut values = BTreeSet::new();

        for _ in 0..count {
            values.insert(reader.read()?);
        }

        Ok(values)
    }
}

impl<K: AktorData + Ord, V: AktorData> AktorData for BTreeMap<K, V> {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.length(self.len());

        for (key, value) in self {
            writer.write(key)?;
            writer.write(value)?;
        }

        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let count = reader.length()?;
        let mut values = BTreeMap::new();

        for _ in 0..count {
            values.insert(reader.read()?, reader.read()?);
        }

        Ok(values)
    }
}

#[cfg(feature = "std")]
impl<K, V, H> AktorData for HashMap<K, V, H>
where
    K: AktorData + Eq + Hash,
    V: AktorData,
    H: BuildHasher + Default,
{
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.length(self.len());

        for (key, value) in self {
            writer.write(key)?;
            writer.write(value)?;
        }

        Ok(())
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let count = reader.length()?;
        let mut values = HashMap::with_hasher(H::default());

        for _ in 0..count {
            values.insert(reader.read()?, reader.read()?);
        }

        Ok(values)
    }
}

#[cfg(feature = "std")]
impl<T, H> AktorData for HashSet<T, H>
where
    T: AktorData + Eq + Hash,
    H: BuildHasher + Default,
{
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        writer.length(self.len());
        self.iter().try_for_each(|value| writer.write(value))
    }

    // Straight into the table, a staging Vec would hold every value twice.
    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let count = reader.length()?;
        let mut values = HashSet::with_hasher(H::default());

        for _ in 0..count {
            values.insert(reader.read()?);
        }

        Ok(values)
    }
}
