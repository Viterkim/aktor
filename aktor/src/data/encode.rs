use super::{AktorData, DataError, MAX_DEPTH, Writer, charge, value_budget};
#[cfg(feature = "transport")]
use super::{Counting, memory_budget};
#[cfg(feature = "transport")]
use alloc::vec::Vec;
#[cfg(feature = "transport")]
use er::*;

/// Encode compact data with bounded nesting and work before admission.
///
/// Work is limited by the bytes already written, with an initial allowance for
/// small values. Large collections of zero-sized values can exceed this budget.
#[cfg(feature = "transport")]
pub fn encode<T: AktorData>(value: &T) -> Result<Vec<u8>, (DataError, String)> {
    let counting = Counting::start(usize::MAX, false);
    let mut writer = Writer {
        bytes: Vec::new(),
        depth: 0,
        values: 0,
    };

    if let Err(error) = writer.write(value) {
        er_bail!(error);
    }

    // The decoder stops at this same total for these bytes, the sender hears about it first.
    let limit = memory_budget(writer.bytes.len());
    if counting.over() || counting.charged() > limit {
        er_bail!(DataError::from(super::DataReason::Memory(limit)));
    }

    Ok(writer.bytes)
}

impl Writer {
    /// Write a value that sits inside this one. Every nested value goes through here, it's where
    /// nesting and the work per byte get counted, the same way the reader counts them.
    #[inline]
    pub fn write<T: AktorData>(&mut self, value: &T) -> Result<(), DataError> {
        self.enter()?;
        charge(T::MEMORY);
        let result = value.encode_data(self);

        self.depth -= 1;
        result
    }

    #[inline]
    fn enter(&mut self) -> Result<(), DataError> {
        if self.depth >= MAX_DEPTH {
            return Err(super::DataReason::Depth(MAX_DEPTH).into());
        }

        self.values += 1;

        if self.values > value_budget(self.bytes.len()) {
            return Err(super::DataReason::Values(value_budget(self.bytes.len())).into());
        }

        self.depth += 1;
        Ok(())
    }

    /// A length, count or enum variant, as a varint.
    #[inline]
    pub fn length(&mut self, length: usize) {
        self.unsigned(length as u64);
    }

    #[inline]
    pub fn unsigned(&mut self, value: u64) {
        if value < 0x80 {
            self.bytes.push(value as u8);
            return;
        }

        self.unsigned_long(value);
    }

    #[inline(never)]
    fn unsigned_long(&mut self, mut value: u64) {
        let mut buffer = [0; 10];
        let mut used = 0;

        while value >= 0x80 {
            buffer[used] = value as u8 | 0x80;
            value >>= 7;
            used += 1;
        }

        buffer[used] = value as u8;
        self.bytes.extend_from_slice(&buffer[..=used]);
    }

    /// Zigzag, so small negative numbers stay small too.
    #[inline]
    pub fn signed(&mut self, value: i64) {
        self.unsigned(((value << 1) ^ (value >> 63)) as u64);
    }

    #[inline]
    pub fn unsigned_wide(&mut self, mut value: u128) {
        while value >= 0x80 {
            self.bytes.push(value as u8 | 0x80);
            value >>= 7;
        }

        self.bytes.push(value as u8);
    }

    #[inline]
    pub fn signed_wide(&mut self, value: i128) {
        self.unsigned_wide(((value << 1) ^ (value >> 127)) as u128);
    }

    /// Bytes with their length in front.
    #[inline]
    pub fn slice(&mut self, bytes: &[u8]) {
        self.length(bytes.len());
        self.bytes.extend_from_slice(bytes);
    }
}
