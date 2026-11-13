use crate::{CallError, Cause, Failure};
use std::borrow::Cow;

const MIN_SIZE: usize = 2048;
// Transport enums use small variant tags. 255 marks a compressed envelope.
const COMPRESSED: u8 = 255;

pub fn encode(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() < MIN_SIZE || bytes.len() > u32::MAX as usize || !compressible(&bytes) {
        return bytes;
    }

    let mut packed = vec![0; 5 + lz4_flex::block::get_maximum_output_size(bytes.len())];
    let Ok(length) = lz4_flex::block::compress_into(&bytes, &mut packed[5..]) else {
        return bytes;
    };

    if length + 5 > bytes.len() - bytes.len() / 8 {
        return bytes;
    }

    packed[0] = COMPRESSED;
    packed[1..5].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    packed.truncate(length + 5);
    packed
}

pub fn decode(bytes: &[u8], limit: usize) -> Result<Cow<'_, [u8]>, Failure> {
    if bytes.len() > limit {
        return Err(error("transport envelope exceeds its size limit"));
    }

    if bytes.first() != Some(&COMPRESSED) {
        return Ok(Cow::Borrowed(bytes));
    }

    let header: [u8; 4] = bytes
        .get(1..5)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| error("truncated compressed envelope"))?;
    let length = u32::from_le_bytes(header) as usize;
    let body = &bytes[5..];

    if length > limit || length > body.len().saturating_mul(255) {
        return Err(error("compressed envelope exceeds its decoded size limit"));
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| error("cannot allocate decoded envelope"))?;
    output.resize(length, 0);
    let decoded = lz4_flex::block::decompress_into(body, &mut output)
        .map_err(|source| error(source.to_string()))?;

    if decoded != length {
        return Err(error("compressed envelope length mismatch"));
    }

    Ok(Cow::Owned(output))
}

fn error(message: impl Into<String>) -> Failure {
    Failure::new(CallError::NotAdmitted, Cause::Codec, message)
}

fn compressible(bytes: &[u8]) -> bool {
    // Look for repeated words and byte pairs before paying for LZ4.
    let mut words = [0u8; 256];
    let mut following = [0u8; 256];
    let mut repeats = 0;

    for start in [0, (bytes.len() - 512) / 2, bytes.len() - 512] {
        for word in bytes[start..start + 512].as_chunks::<4>().0 {
            let [byte, next, ..] = *word;
            let last = &mut following[usize::from(byte)];
            repeats += usize::from(*last == next);
            *last = next;

            let hash = u32::from_le_bytes(*word).wrapping_mul(0x9e37_79b1);
            let bucket = (hash >> 24) as usize;
            let stamp = (hash >> 16) as u8;
            repeats += usize::from(words[bucket] == stamp);
            words[bucket] = stamp;
        }

        if repeats >= 14 {
            return true;
        }
    }

    false
}
