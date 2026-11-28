use crate::{Cause, WireFailure, packet};
use aktor::{AktorData, worker::Operations};
use er::*;
use std::{borrow::Cow, io, sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

pub const VERSION: u32 = 0;
pub const MAX_FRAME: usize = 4 * 1024 * 1024;
pub const MIN_FRAME: usize = 128;
pub const MAX_HANDSHAKE: usize = 1024 * 1024;

#[derive(AktorData)]
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
    Reject(WireFailure),
    Call {
        id: u64,
        operation: Cow<'a, str>,
        input: Cow<'a, [u8]>,
    },
    Answer {
        id: u64,
        output: Result<Vec<u8>, WireFailure>,
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
        input: Cow<'a, [u8]>,
    },
    LatestSeen {
        revision: u64,
    },
    LatestAnswer {
        revision: u64,
        output: Result<Vec<u8>, WireFailure>,
    },
    /// A call that only wants admission. The server says Admitted and nothing after it, so the
    /// connection is free again while the actor still runs the work.
    Cast {
        id: u64,
        operation: Cow<'a, str>,
        input: Cow<'a, [u8]>,
    },
    LatestClosed,
    DeliveryReceived(aktor::FailureId),
}

#[derive(Er)]
pub enum FrameLimitErr {
    TooSmall { limit: usize, min: usize },
    TooLarge { limit: usize, max: usize },
}
pub fn validate(max_frame: usize) -> ErResult<(), FrameLimitErr> {
    if max_frame < MIN_FRAME {
        er_bail!(FrameLimitErr::TooSmall {
            limit: max_frame,
            min: MIN_FRAME
        });
    }

    if max_frame > u32::MAX as usize {
        er_bail!(FrameLimitErr::TooLarge {
            limit: max_frame,
            max: u32::MAX as usize
        });
    }

    Ok(())
}

#[derive(Er)]
pub enum EncodeFrameErr {
    Encode,
    TooLarge { size: usize, limit: usize },
}
pub fn encode(message: &Message<'_>, limit: usize) -> ErResult<Vec<u8>, EncodeFrameErr> {
    let bytes = aktor::data::encode(message).er(EncodeFrameErr::encode)?;

    if bytes.len() > limit {
        er_bail!(EncodeFrameErr::TooLarge {
            size: bytes.len(),
            limit
        });
    }

    Ok(bytes)
}

#[derive(Er)]
pub struct WriteFrameErr {
    pub cause: Cause,
}
pub async fn write<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &Message<'_>,
    limit: usize,
) -> ErResult<(), WriteFrameErr> {
    let bytes = encode(message, limit).er(Cause::Codec)?;
    write_bytes(stream, &bytes).await.er(Cause::Connection)
}

#[derive(Er)]
pub enum WriteBytesErr {
    Write,
    WriteZero,
}
pub async fn write_bytes<W: AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
) -> ErResult<(), WriteBytesErr> {
    let prefix = (bytes.len() as u32).to_be_bytes();

    if !stream.is_write_vectored() {
        let mut first = [0; 1024];
        let head = bytes.len().min(first.len() - prefix.len());
        let end = prefix.len() + head;

        first[..prefix.len()].copy_from_slice(&prefix);
        first[prefix.len()..end].copy_from_slice(&bytes[..head]);
        stream
            .write_all(&first[..end])
            .await
            .er(WriteBytesErr::write)?;
        stream
            .write_all(&bytes[head..])
            .await
            .er(WriteBytesErr::write)?;
        return Ok(());
    }

    let written = stream
        .write_vectored(&[io::IoSlice::new(&prefix), io::IoSlice::new(bytes)])
        .await
        .er(WriteBytesErr::write)?;

    if written == 0 {
        er_bail!(WriteBytesErr::WriteZero);
    }

    if written < prefix.len() {
        stream
            .write_all(&prefix[written..])
            .await
            .er(WriteBytesErr::write)?;
    }

    stream
        .write_all(&bytes[written.saturating_sub(prefix.len())..])
        .await
        .er(WriteBytesErr::write)
}

#[derive(Er)]
pub struct WriteReplyErr {
    pub cause: Cause,
}
pub async fn write_reply<W: AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
    duration: Duration,
) -> ErResult<(), WriteReplyErr> {
    let written = timeout(duration, write_bytes(stream, bytes))
        .await
        .er(Cause::Timeout)?;

    written.er(Cause::Connection)
}

#[derive(Er)]
struct FrameSizeErr {
    size: usize,
    limit: usize,
}

#[derive(Default)]
pub struct FrameReader {
    // Keep one allocation within the default frame limit, even when a peer can send larger frames.
    cached: Mutex<Vec<u8>>,
}
#[derive(Er)]
pub struct ReadFrameErr {
    pub cause: Cause,
    pub msg: &'static str,
}
impl FrameReader {
    pub async fn read<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        limit: usize,
    ) -> ErResult<Message<'static>, ReadFrameErr> {
        let length = stream.read_u32().await.er(|| ReadFrameErr {
            cause: Cause::Connection,
            msg: "read frame length",
        })? as usize;

        self.read_body(stream, length, limit, true).await
    }

    pub async fn read_handshake<R: AsyncBufRead + Unpin>(
        &self,
        stream: &mut R,
    ) -> ErResult<Option<Message<'static>>, ReadFrameErr> {
        let buffered = stream.fill_buf().await.er(|| ReadFrameErr {
            cause: Cause::Connection,
            msg: "read handshake",
        })?;
        if buffered.is_empty() {
            return Ok(None);
        }

        let length = stream.read_u32().await.er(|| ReadFrameErr {
            cause: Cause::Connection,
            msg: "read handshake length",
        })? as usize;

        self.read_body(stream, length, MAX_HANDSHAKE, false)
            .await
            .map(Some)
    }

    pub async fn read_idle<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        limit: usize,
        duration: Duration,
    ) -> ErResult<Option<Message<'static>>, ReadFrameErr> {
        let mut first = [0];
        let read = stream.read(&mut first).await.er(|| ReadFrameErr {
            cause: Cause::Connection,
            msg: "read frame",
        })?;
        if read == 0 {
            return Ok(None);
        }

        timeout(duration, async {
            let mut prefix = [first[0], 0, 0, 0];

            stream
                .read_exact(&mut prefix[1..])
                .await
                .er(|| ReadFrameErr {
                    cause: Cause::Connection,
                    msg: "read frame length",
                })?;

            self.read_body(stream, u32::from_be_bytes(prefix) as usize, limit, true)
                .await
        })
        .await
        .er(|| ReadFrameErr {
            cause: Cause::Timeout,
            msg: "remote frame timed out",
        })?
        .map(Some)
    }

    async fn read_body<R: AsyncRead + Unpin>(
        &self,
        stream: &mut R,
        length: usize,
        limit: usize,
        compressed: bool,
    ) -> ErResult<Message<'static>, ReadFrameErr> {
        if length > limit {
            er_bail!(
                ReadFrameErr {
                    cause: Cause::Protocol,
                    msg: "remote frame exceeds the configured limit"
                },
                FrameSizeErr {
                    size: length,
                    limit
                }
            );
        }

        let mut small = [0; 64];
        let mut large = Vec::new();
        let buffer = if let Some(bytes) = small.get_mut(..length) {
            stream.read_exact(bytes).await.er(|| ReadFrameErr {
                cause: Cause::Connection,
                msg: "read frame",
            })?;
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

                    large.try_reserve_exact(additional).er(|| ReadFrameErr {
                        cause: Cause::Codec,
                        msg: "reserve frame",
                    })?;
                }

                let read = stream
                    .take(remaining as u64)
                    .read_buf(&mut large)
                    .await
                    .er(|| ReadFrameErr {
                        cause: Cause::Connection,
                        msg: "read frame",
                    })?;

                if read == 0 {
                    er_bail!(ReadFrameErr {
                        cause: Cause::Connection,
                        msg: "remote stream ended inside a frame"
                    });
                }
            }

            &mut large
        };

        let bytes = match compressed {
            true => packet::decode(buffer, limit).er(|| ReadFrameErr {
                cause: Cause::Codec,
                msg: "unpack frame",
            })?,
            false => Cow::Borrowed(&*buffer),
        };
        let message = aktor::data::decode(&bytes).er(|| ReadFrameErr {
            cause: Cause::Protocol,
            msg: "decode frame",
        })?;

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
) -> ErResult<Message<'static>, ReadFrameErr> {
    FrameReader::default().read(stream, limit).await
}
