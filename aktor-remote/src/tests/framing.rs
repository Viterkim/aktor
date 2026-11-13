use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;

struct Broken {
    bytes: Vec<u8>,
    limit: usize,
    vectored: bool,
}
impl AsyncWrite for Broken {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let count = bytes.len().min(self.limit - self.bytes.len());
        if count == 0 {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.bytes.extend_from_slice(&bytes[..count]);
        Poll::Ready(Ok(count))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        assert!(self.vectored);
        let mut written = 0;
        for bytes in bufs {
            let count = bytes.len().min(self.limit - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            written += count;
        }
        Poll::Ready(Ok(written))
    }
}

#[tokio::test]
async fn incomplete() {
    let payload = [7; 32];
    let mut expected = 32u32.to_be_bytes().to_vec();
    expected.extend_from_slice(&payload);

    for vectored in [false, true] {
        for limit in [2, 8] {
            let mut writer = Broken {
                bytes: Vec::new(),
                limit,
                vectored,
            };
            wire::write_bytes(&mut writer, &payload).await.unwrap_err();
            assert_eq!(writer.bytes, expected[..limit]);
        }
    }
}

#[tokio::test]
async fn trailing_bytes() {
    let mut bytes = wire::encode(&wire::Message::Admitted { id: 1 }, wire::MAX_FRAME).unwrap();
    bytes.push(0);
    let mut framed = (bytes.len() as u32).to_be_bytes().to_vec();
    framed.extend(bytes);
    assert_eq!(
        wire::read(&mut framed.as_slice(), wire::MAX_FRAME)
            .await
            .err()
            .unwrap()
            .cause,
        Cause::Protocol
    );
}
#[tokio::test]
async fn reuse_received_frame() {
    let frames = wire::FrameReader::default();
    let mut stream = Vec::new();
    let inputs = [vec![11; 128 * 1024], vec![23; 8192]];

    for input in &inputs {
        let message = wire::Message::Call {
            id: 1,
            operation: "echo".into(),
            input: input.as_slice().into(),
        };
        wire::write(&mut stream, &message, wire::MAX_FRAME)
            .await
            .unwrap();
    }

    let mut reader = stream.as_slice();
    for expected in inputs {
        let wire::Message::Call { input, .. } =
            frames.read(&mut reader, wire::MAX_FRAME).await.unwrap()
        else {
            panic!("wrong message");
        };
        assert_eq!(input, expected);
    }
}

#[tokio::test]
async fn compressed() {
    use crate::{packet, wire::Message};

    let value = vec![17; 65536];
    let message = Message::Call {
        id: 1,
        operation: "echo".into(),
        input: value.as_slice().into(),
    };
    let raw = wire::encode(&message, wire::MAX_FRAME).unwrap();
    let packed = packet::encode(raw.clone());
    assert!(packed.len() < raw.len() / 8);
    let mut framed = (packed.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&packed);

    let handshake = wire::FrameReader::default()
        .read_handshake(&mut framed.as_slice())
        .await;
    assert_eq!(handshake.err().unwrap().cause, Cause::Protocol);

    let Message::Call { input, .. } = wire::read(&mut framed.as_slice(), raw.len()).await.unwrap()
    else {
        panic!("wrong message");
    };
    assert_eq!(input, value);
    assert!(packet::decode(&packed, raw.len() - 1).is_err());

    for malformed in [
        &[255][..],
        &[255, 0xff, 0xff, 0xff, 0xff, 0],
        &[255, 10, 0, 0, 0, 255],
    ] {
        assert!(packet::decode(malformed, wire::MAX_FRAME).is_err());
    }
}
