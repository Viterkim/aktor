use crate::packet;
use crate::{CallError, Cause, Failure};
use aktor::worker::Operations;
use postcard::{Error as PostcardError, ser_flavors::Flavor};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, io, sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

pub const VERSION: u32 = 0;
pub const MAX_FRAME: usize = 4 * 1024 * 1024;
pub const MIN_FRAME: usize = 128;
pub const MAX_HANDSHAKE: usize = 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub enum Message<'a> {
    Hello {
        version: u32,
        build: String,
        operations: Operations,
        max_frame: u32,
    },
    Ready {
        max_frame: u32,
    },
    Reject(Failure),
    Call {
        id: u64,
        operation: Cow<'a, str>,
        #[serde(
            serialize_with = "aktor::worker::bytes::serialize",
            deserialize_with = "owned_input"
        )]
        input: Cow<'a, [u8]>,
    },
    Answer {
        id: u64,
        #[serde(with = "aktor::worker::bytes::result")]
        output: Result<Vec<u8>, Failure>,
    },
    Admitted {
        id: u64,
    },
    Latest {
        operation: String,
        session: Option<[u8; 16]>,
        generation: u64,
    },
    LatestReady {
        session: [u8; 16],
    },
    LatestExpired,
    Update {
        session: [u8; 16],
        revision: u64,
        #[serde(
            serialize_with = "aktor::worker::bytes::serialize",
            deserialize_with = "owned_input"
        )]
        input: Cow<'a, [u8]>,
    },
    LatestSeen {
        revision: u64,
    },
    LatestAnswer {
        revision: u64,
        #[serde(with = "aktor::worker::bytes::result")]
        output: Result<Vec<u8>, Failure>,
    },
}

fn owned_input<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Cow<'static, [u8]>, D::Error> {
    aktor::worker::bytes::deserialize(deserializer).map(Cow::Owned)
}

pub fn validate(max_frame: usize) -> Result<(), Failure> {
    if max_frame < MIN_FRAME || max_frame > u32::MAX as usize {
        return Err(Failure::new(
            CallError::NotAdmitted,
            Cause::Protocol,
            "frame limit must be at least 128 bytes and fit in a u32",
        ));
    }

    Ok(())
}

pub fn encode(message: &Message<'_>, limit: usize) -> Result<Vec<u8>, Failure> {
    postcard::serialize_with_flavor(
        message,
        Frame {
            bytes: Vec::new(),
            limit,
        },
    )
    .map_err(|error| {
        let report = match error {
            PostcardError::SerializeBufferFull => {
                "remote frame exceeds the configured limit".into()
            }
            error => error.to_string(),
        };

        Failure::new(CallError::NotAdmitted, Cause::Codec, report)
    })
}

struct Frame {
    bytes: Vec<u8>,
    limit: usize,
}
impl Flavor for Frame {
    type Output = Vec<u8>;

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        if self.bytes.len() == self.limit {
            return Err(PostcardError::SerializeBufferFull);
        }

        self.bytes.push(byte);
        Ok(())
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(PostcardError::SerializeBufferFull);
        }

        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.bytes)
    }
}

pub async fn write<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &Message<'_>,
    limit: usize,
) -> Result<(), Failure> {
    write_bytes(stream, &encode(message, limit)?).await
}

pub async fn write_bytes<W: AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
) -> Result<(), Failure> {
    let prefix = (bytes.len() as u32).to_be_bytes();
    if !stream.is_write_vectored() {
        let mut first = [0; 1024];
        let head = bytes.len().min(first.len() - prefix.len());
        let end = prefix.len() + head;

        first[..prefix.len()].copy_from_slice(&prefix);
        first[prefix.len()..end].copy_from_slice(&bytes[..head]);
        stream.write_all(&first[..end]).await?;
        stream.write_all(&bytes[head..]).await?;
        return Ok(());
    }

    let written = stream
        .write_vectored(&[io::IoSlice::new(&prefix), io::IoSlice::new(bytes)])
        .await?;

    if written == 0 {
        return Err(io::Error::from(io::ErrorKind::WriteZero).into());
    }
    if written < prefix.len() {
        stream.write_all(&prefix[written..]).await?;
    }

    stream
        .write_all(&bytes[written.saturating_sub(prefix.len())..])
        .await?;

    Ok(())
}

pub async fn write_reply<W: AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
    duration: Duration,
) -> Result<(), Failure> {
    timeout(duration, write_bytes(stream, bytes))
        .await
        .map_err(|_| {
            Failure::new(
                CallError::OutcomeUnknown,
                Cause::Timeout,
                "remote reply write timed out",
            )
        })?
}

#[derive(Default)]
pub struct FrameReader {
    // Keep one allocation within the default frame limit, even when a peer can send larger frames.
    cached: Mutex<Vec<u8>>,
}

impl FrameReader {
    pub async fn read<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        limit: usize,
    ) -> Result<Message<'static>, Failure> {
        let length = stream.read_u32().await? as usize;

        self.read_body(stream, length, limit, true).await
    }

    pub async fn read_handshake<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
    ) -> Result<Message<'static>, Failure> {
        let length = stream.read_u32().await? as usize;

        self.read_body(stream, length, MAX_HANDSHAKE, false).await
    }

    pub async fn read_idle<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        limit: usize,
        duration: Duration,
    ) -> Result<Message<'static>, Failure> {
        let first = stream.read_u8().await?;

        timeout(duration, async {
            let mut prefix = [first, 0, 0, 0];
            stream.read_exact(&mut prefix[1..]).await?;
            self.read_body(stream, u32::from_be_bytes(prefix) as usize, limit, true)
                .await
        })
        .await
        .map_err(|_| {
            Failure::new(
                CallError::NotAdmitted,
                Cause::Timeout,
                "remote frame timed out",
            )
        })?
    }

    async fn read_body<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        length: usize,
        limit: usize,
        compressed: bool,
    ) -> Result<Message<'static>, Failure> {
        if length > limit {
            return Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote frame exceeds the configured limit",
            ));
        }

        let mut small = [0; 64];
        let mut large = Vec::new();
        let buffer = if let Some(bytes) = small.get_mut(..length) {
            stream.read_exact(bytes).await?;
            bytes
        } else {
            large = std::mem::take(
                &mut *self
                    .cached
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            );

            while large.len() < length {
                let remaining = length - large.len();
                if large.len() == large.capacity() {
                    let additional = large.len().max(4096).min(remaining);
                    large.try_reserve_exact(additional).map_err(|error| {
                        Failure::new(CallError::NotAdmitted, Cause::Codec, error.to_string())
                    })?;
                }

                if stream.take(remaining as u64).read_buf(&mut large).await? == 0 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
                }
            }

            &mut large
        };

        let bytes = if compressed {
            packet::decode(buffer, limit)?
        } else {
            Cow::Borrowed(&*buffer)
        };
        let (message, extra) = postcard::take_from_bytes(&bytes).map_err(|error| {
            Failure::new(CallError::NotAdmitted, Cause::Protocol, error.to_string())
        })?;
        if !extra.is_empty() {
            return Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote frame contains trailing bytes",
            ));
        }

        if large.capacity() != 0 && large.capacity() <= MAX_FRAME {
            large.clear();

            let mut cached = self
                .cached
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if large.capacity() > cached.capacity() {
                std::mem::swap(&mut large, &mut cached);
            }
        }

        Ok(message)
    }
}

#[cfg(test)]
pub async fn read<R: AsyncRead + Unpin>(
    stream: &mut R,
    limit: usize,
) -> Result<Message<'static>, Failure> {
    FrameReader::default().read(stream, limit).await
}
