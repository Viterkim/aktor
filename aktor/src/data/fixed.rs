use super::{DataError, Reader, Writer};
use alloc::vec::Vec;

/// Little endian values back to back, behind their length in bytes. On a little endian machine
/// that's the slice's own memory.
pub fn encode_slice<T: bytemuck::NoUninit, const N: usize>(
    values: &[T],
    writer: &mut Writer,
    bytes: impl Fn(&T) -> [u8; N],
) {
    writer.length(values.len() * N);

    if (cfg!(target_endian = "little") || N == 1)
        && let Ok(raw) = bytemuck::try_cast_slice(values)
    {
        writer.bytes.extend_from_slice(raw);
        return;
    }

    for value in values {
        writer.bytes.extend_from_slice(&bytes(value));
    }
}

pub fn decode_vec<T, const N: usize>(
    reader: &mut Reader<'_>,
    max: usize,
    value: impl Fn([u8; N]) -> T,
) -> Result<Vec<T>, DataError> {
    let (values, tail) = reader.slice()?.as_chunks::<N>();

    if !tail.is_empty() {
        return Err(DataError::from(super::DataReason::IncompleteFixed));
    }

    super::within(values.len(), max, "values")?;
    Ok(collect(values, value))
}

/// On its own, LLVM sees the plain copy and hands it to memcpy. Inlined next to the reader it
/// wrote its own 16 byte loop, about a fifth slower on a misaligned 32 KB vector.
#[inline(never)]
fn collect<T, const N: usize>(values: &[[u8; N]], value: impl Fn([u8; N]) -> T) -> Vec<T> {
    values.iter().map(|bytes| value(*bytes)).collect()
}
