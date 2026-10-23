use crate::packet;
use crate::{CallError, Cause, Error};
use aktor::worker::Operations;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

pub const VERSION: u32 = 0;
pub const MAX_FRAME: usize = 4 * 1024 * 1024;
pub const MIN_FRAME: usize = 128;
pub const MAX_HANDSHAKE: usize = 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub enum Message {
    Hello {
        version: u32,
        build: String,
        operations: Operations,
        max_frame: u32,
    },
    Ready {
        max_frame: u32,
    },
    Reject(Error),
    Call {
        id: u64,
        operation: String,
        #[serde(with = "aktor::worker::bytes")]
        input: Vec<u8>,
    },
    Answer {
        id: u64,
        #[serde(with = "aktor::worker::bytes::result")]
        output: Result<Vec<u8>, Error>,
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
        #[serde(with = "aktor::worker::bytes")]
        input: Vec<u8>,
    },
    LatestSeen {
        revision: u64,
    },
    LatestAnswer {
        revision: u64,
        #[serde(with = "aktor::worker::bytes::result")]
        output: Result<Vec<u8>, Error>,
    },
}

pub fn validate(max_frame: usize) -> Result<(), Error> {
    if max_frame < MIN_FRAME || max_frame > u32::MAX as usize {
        return Err(Error::new(
            CallError::NotAdmitted,
            Cause::Protocol,
            "frame limit must be at least 128 bytes and fit in a u32",
        ));
    }

    Ok(())
}

pub fn encode(message: &Message, limit: usize) -> Result<Vec<u8>, Error> {
    let bytes = postcard::to_allocvec(message)
        .map_err(|error| Error::new(CallError::NotAdmitted, Cause::Codec, error.to_string()))?;

    if bytes.len() > limit {
        return Err(Error::new(
            CallError::NotAdmitted,
            Cause::Codec,
            "remote frame exceeds the configured limit",
        ));
    }

    Ok(bytes)
}

pub async fn write<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &Message,
    limit: usize,
) -> Result<(), Error> {
    write_bytes(stream, &encode(message, limit)?).await
}

pub async fn write_bytes<W: AsyncWrite + Unpin>(stream: &mut W, bytes: &[u8]) -> Result<(), Error> {
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await?;

    Ok(())
}

pub async fn write_reply<W: AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
    duration: Duration,
) -> Result<(), Error> {
    timeout(duration, write_bytes(stream, bytes))
        .await
        .map_err(|_| {
            Error::new(
                CallError::OutcomeUnknown,
                Cause::Timeout,
                "remote reply write timed out",
            )
        })?
}

pub async fn read<R: AsyncRead + Unpin>(stream: &mut R, limit: usize) -> Result<Message, Error> {
    let length = stream.read_u32().await? as usize;

    read_body(stream, length, limit).await
}

pub async fn read_idle<R: AsyncRead + Unpin>(
    stream: &mut R,
    limit: usize,
    duration: Duration,
) -> Result<Message, Error> {
    let first = stream.read_u8().await?;

    timeout(duration, async {
        let mut prefix = [first, 0, 0, 0];
        stream.read_exact(&mut prefix[1..]).await?;
        read_body(stream, u32::from_be_bytes(prefix) as usize, limit).await
    })
    .await
    .map_err(|_| {
        Error::new(
            CallError::NotAdmitted,
            Cause::Timeout,
            "remote frame timed out",
        )
    })?
}

async fn read_body<R: AsyncRead + Unpin>(
    stream: &mut R,
    length: usize,
    limit: usize,
) -> Result<Message, Error> {
    if length > limit {
        return Err(Error::new(
            CallError::NotAdmitted,
            Cause::Protocol,
            "remote frame exceeds the configured limit",
        ));
    }

    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    let bytes = packet::decode(&bytes, limit)?;

    postcard::from_bytes(&bytes)
        .map_err(|error| Error::new(CallError::NotAdmitted, Cause::Protocol, error.to_string()))
}
