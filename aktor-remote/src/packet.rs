use er::*;
use std::borrow::Cow;

const MIN_SIZE: usize = 2048;
// Transport enums use small variant tags. 255 marks a compressed envelope.
const COMPRESSED: u8 = 255;
// LZ4 can expand a payload 255 times before the message decoder even sees it.
const MAX_RATIO: usize = 32;

pub fn encode(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() < MIN_SIZE || bytes.len() > u32::MAX as usize || !compressible(&bytes) {
        return bytes;
    }

    let mut packed = vec![0; 5 + lz4_flex::block::get_maximum_output_size(bytes.len())];
    let Ok(length) = lz4_flex::block::compress_into(&bytes, &mut packed[5..]) else {
        return bytes;
    };

    if length + 5 > bytes.len() - bytes.len() / 8 || bytes.len() > length.saturating_mul(MAX_RATIO)
    {
        return bytes;
    }

    packed[0] = COMPRESSED;
    packed[1..5].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    packed.truncate(length + 5);
    packed
}

#[derive(Er)]
pub enum DecodePacketErr {
    TooLarge { size: usize, limit: usize },
    Truncated { size: usize },
    DecodedTooLarge { size: usize, limit: usize },
    Reserve { size: usize },
    Decompress,
    LengthMismatch { decoded: usize, expected: usize },
}
pub fn decode(bytes: &[u8], limit: usize) -> ErResult<Cow<'_, [u8]>, DecodePacketErr> {
    if bytes.len() > limit {
        er_bail!(DecodePacketErr::TooLarge {
            size: bytes.len(),
            limit
        });
    }

    if bytes.first() != Some(&COMPRESSED) {
        return Ok(Cow::Borrowed(bytes));
    }

    let header: [u8; 4] = bytes
        .get(1..5)
        .and_then(|bytes| bytes.try_into().ok())
        .er(|| DecodePacketErr::truncated(bytes.len()))?;
    let length = u32::from_le_bytes(header) as usize;
    let body = &bytes[5..];
    let decoded_limit = limit.min(body.len().saturating_mul(MAX_RATIO));

    if length > decoded_limit {
        er_bail!(DecodePacketErr::DecodedTooLarge {
            size: length,
            limit: decoded_limit
        });
    }

    let mut output = Vec::new();

    output
        .try_reserve_exact(length)
        .er(|| DecodePacketErr::reserve(length))?;
    output.resize(length, 0);

    let decoded =
        lz4_flex::block::decompress_into(body, &mut output).er(DecodePacketErr::decompress)?;

    if decoded != length {
        er_bail!(DecodePacketErr::LengthMismatch {
            decoded,
            expected: length
        });
    }

    Ok(Cow::Owned(output))
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
