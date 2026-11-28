use super::{DataError, Reader, Writer};
use alloc::vec::Vec;

/// Eight to a byte. The last byte gets one more bit set right after the values, so the
/// reader knows where they stop without a separate count.
pub fn encode_slice(values: &[bool], writer: &mut Writer) {
    if values.is_empty() {
        writer.length(0);
        return;
    }

    let length = values.len() / 8 + 1;

    writer.length(length);
    writer.bytes.reserve(length);

    let (chunks, tail) = values.as_chunks::<8>();

    for chunk in chunks {
        writer.bytes.push(pack(chunk));
    }

    writer.bytes.push(pack(tail) | 1 << tail.len());
}

pub fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<bool>, DataError> {
    let Some((&last, bytes)) = reader.slice()?.split_last() else {
        return Ok(Vec::new());
    };

    if last == 0 {
        return Err(DataError::from(super::DataReason::MissingEndBit));
    }

    let remaining = 7 - last.leading_zeros() as usize;

    if bytes.is_empty() {
        super::within(remaining, max, "values")?;
        return Ok((0..remaining).map(|bit| last & (1 << bit) != 0).collect());
    }

    let count = bytes
        .len()
        .checked_mul(8)
        .and_then(|count| count.checked_add(remaining))
        .ok_or_else(|| DataError::from(super::DataReason::CountOverflow))?;
    super::within(count, max, "values")?;

    // with_capacity panics when it can't get the memory, or on 32 bit targets when the count
    // is past isize::MAX. Both are a refusal here.
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        return Err(DataError::from(super::DataReason::BooleanAllocation));
    }

    for byte in bytes {
        values.extend_from_slice(&unpack(*byte));
    }

    values.extend((0..remaining).map(|bit| last & (1 << bit) != 0));
    Ok(values)
}

pub fn encode_array<const N: usize>(values: &[bool; N], writer: &mut Writer) {
    writer.bytes.extend(values.chunks(8).map(pack));
}

pub fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[bool; N], DataError> {
    let mut values = [false; N];
    let bytes = reader.take(N.div_ceil(8))?;

    for (chunk, byte) in values.chunks_mut(8).zip(bytes) {
        if chunk.len() < 8 && byte >> chunk.len() != 0 {
            return Err(DataError::from(super::DataReason::NonzeroPadding));
        }

        for (bit, value) in chunk.iter_mut().enumerate() {
            *value = byte & (1 << bit) != 0;
        }
    }

    Ok(values)
}

fn pack(values: &[bool]) -> u8 {
    if let &[a, b, c, d, e, f, g, h] = values {
        let bytes = [a, b, c, d, e, f, g, h].map(u8::from);
        let word = u64::from_le_bytes(bytes);

        // Each byte holds one bit. Multiplication gathers them into the top byte.
        return (word.wrapping_mul(0x0102_0408_1020_4080) >> 56) as u8;
    }

    values
        .iter()
        .enumerate()
        .fold(0_u8, |byte, (bit, value)| byte | (u8::from(*value) << bit))
}

fn unpack(byte: u8) -> [bool; 8] {
    let halves = u64::from(byte & 0x0f) | (u64::from(byte >> 4) << 32);
    // Copy each nibble at bit offsets 0, 7, 14 and 21. The mask keeps one bit from each copy.
    let bits = (halves * 0x0020_4081) & 0x0101_0101_0101_0101;
    bits.to_le_bytes().map(|bit| bit != 0)
}
