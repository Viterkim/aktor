use crate::{CallError, Cause, Error};
use std::borrow::Cow;

const MIN_SIZE: usize = 8192;
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
    drop(bytes);
    packed.shrink_to_fit();
    packed
}

pub fn decode(bytes: &[u8], limit: usize) -> Result<Cow<'_, [u8]>, Error> {
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

fn error(message: impl Into<String>) -> Error {
    Error::new(CallError::NotAdmitted, Cause::Codec, message)
}

fn compressible(bytes: &[u8]) -> bool {
    let mut words = [0u32; 1024];
    let mut counts = [0u16; 256];
    let mut repeated = 0;

    for start in [0, (bytes.len() - 512) / 2, bytes.len() - 512] {
        let sample = &bytes[start..start + 512];

        for byte in sample {
            counts[*byte as usize] += 1;
        }

        for word in sample.as_chunks::<4>().0 {
            let word = u32::from_le_bytes(*word);
            let slot = (word.wrapping_mul(0x9e37_79b1) >> 22) as usize;
            repeated += usize::from(words[slot] == word);
            words[slot] = word;
        }
    }

    repeated >= 48
        || counts
            .iter()
            .map(|count| u32::from(*count).pow(2))
            .sum::<u32>()
            > 1536 * 1536 / 64
}
