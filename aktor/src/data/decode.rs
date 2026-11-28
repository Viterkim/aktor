use super::{AktorData, DataError, MAX_DEPTH, Reader, charge, value_budget};
#[cfg(feature = "transport")]
use super::{Counting, memory_budget};
#[cfg(feature = "transport")]
use er::*;

/// Decode compact data with bounded nesting and work, rejecting trailing bytes.
///
/// The value budget follows the bytes read so far, the same way encoding follows the bytes
/// written. Extremely large collections of zero-sized values can exceed it even though
/// their encoding occupies few bytes.
#[cfg(feature = "transport")]
pub fn decode<T: AktorData>(bytes: &[u8]) -> Result<T, (DataError, String)> {
    let counting = Counting::start(memory_budget(bytes.len()), true);
    let mut reader = Reader {
        memory_limit: counting.limit,
        rest: bytes,
        total: bytes.len(),
        depth: 0,
        values: 0,
    };

    let value = match reader.read::<T>() {
        Ok(_) if counting.over() => {
            er_bail!(DataError::from(super::DataReason::Memory(counting.limit)))
        }
        Ok(value) => value,
        Err(error) if counting.over() && !matches!(*error.reason, super::DataReason::Memory(_)) => {
            er_bail!(
                DataError::from(super::DataReason::Memory(counting.limit)),
                error
            );
        }
        Err(error) => er_bail!(error),
    };

    if !reader.rest.is_empty() {
        er_bail!(DataError::from(super::DataReason::Trailing(
            reader.rest.len()
        )));
    }

    Ok(value)
}

impl<'a> Reader<'a> {
    /// Read a value that sits inside this one, counted the same way the writer counted it.
    #[inline]
    pub fn read<T: AktorData>(&mut self) -> Result<T, DataError> {
        self.read_with(T::decode_data)
    }

    /// [`read`](Self::read) with another decode for the same type, counted the same way.
    #[inline]
    pub(super) fn read_with<T: AktorData>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, DataError>,
    ) -> Result<T, DataError> {
        self.enter()?;

        let value = if charge(T::MEMORY) {
            decode(self)
        } else {
            Err(DataError::from(super::DataReason::Memory(
                self.memory_limit,
            )))
        };

        self.depth -= 1;
        value
    }

    #[inline]
    fn enter(&mut self) -> Result<(), DataError> {
        if self.depth >= MAX_DEPTH {
            return Err(super::DataReason::Depth(MAX_DEPTH).into());
        }

        self.values += 1;

        // Bytes after this point haven't been read, they can't buy work for a message that gets
        // rejected at the end anyway.
        if self.values > value_budget(self.total - self.rest.len()) {
            return Err(
                super::DataReason::Values(value_budget(self.total - self.rest.len())).into(),
            );
        }

        self.depth += 1;
        Ok(())
    }

    #[inline]
    pub fn byte(&mut self) -> Result<u8, DataError> {
        let (&byte, rest) = self.rest.split_first().ok_or_else(truncated)?;

        self.rest = rest;
        Ok(byte)
    }

    #[inline]
    pub fn take(&mut self, count: usize) -> Result<&'a [u8], DataError> {
        let (taken, rest) = self.rest.split_at_checked(count).ok_or_else(truncated)?;

        self.rest = rest;
        Ok(taken)
    }

    #[inline]
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], DataError> {
        let (taken, rest) = self.rest.split_first_chunk().ok_or_else(truncated)?;

        self.rest = rest;
        Ok(*taken)
    }

    /// A length or count. Nothing trusts it up front, a slice still has to find its bytes and
    /// every element pays into the value budget as it shows up.
    #[inline]
    pub fn length(&mut self) -> Result<usize, DataError> {
        Ok(usize::try_from(self.unsigned()?)?)
    }

    /// Most lengths and numbers fit one byte. That stays small enough to inline into the loop
    /// reading a whole `Vec`, the longer ones go out of line.
    #[inline]
    pub fn unsigned(&mut self) -> Result<u64, DataError> {
        if let Some((&first, rest)) = self.rest.split_first()
            && first < 0x80
        {
            self.rest = rest;
            return Ok(u64::from(first));
        }

        self.unsigned_long()
    }

    #[inline(never)]
    fn unsigned_long(&mut self) -> Result<u64, DataError> {
        let first = self.byte()?;
        let mut value = u64::from(first & 0x7f);

        for shift in (7..64).step_by(7) {
            let byte = self.byte()?;

            // The tenth byte only has room for the top bit.
            if shift == 63 && byte > 1 {
                break;
            }

            value |= u64::from(byte & 0x7f) << shift;

            if byte < 0x80 {
                return Ok(value);
            }
        }

        Err(DataError::from(super::DataReason::Varint))
    }

    #[inline]
    pub fn signed(&mut self) -> Result<i64, DataError> {
        let value = self.unsigned()?;

        Ok((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    pub fn unsigned_wide(&mut self) -> Result<u128, DataError> {
        let mut value = 0;

        for shift in (0..128).step_by(7) {
            let byte = self.byte()?;

            if shift == 126 && byte > 3 {
                break;
            }

            value |= u128::from(byte & 0x7f) << shift;

            if byte < 0x80 {
                return Ok(value);
            }
        }

        Err(DataError::from(super::DataReason::Varint))
    }

    pub fn signed_wide(&mut self) -> Result<i128, DataError> {
        let value = self.unsigned_wide()?;

        Ok((value >> 1) as i128 ^ -((value & 1) as i128))
    }

    /// Bytes with their length in front.
    #[inline]
    pub fn slice(&mut self) -> Result<&'a [u8], DataError> {
        let length = self.length()?;

        self.take(length)
    }
}

fn truncated() -> DataError {
    DataError::from(super::DataReason::Truncated)
}
