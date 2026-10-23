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
}

#[tokio::test]
async fn incomplete() {
    for limit in [2, 8] {
        let mut writer = Broken {
            bytes: Vec::new(),
            limit,
        };
        wire::write_bytes(&mut writer, &[0; 32]).await.unwrap_err();
        assert_eq!(writer.bytes.len(), limit);
    }
}

#[tokio::test]
async fn compressed() {
    use crate::{packet, wire::Message};

    let value = vec![17; 65536];
    let message = Message::Call {
        id: 1,
        operation: "echo".into(),
        input: value.clone(),
    };
    let raw = wire::encode(&message, wire::MAX_FRAME).unwrap();
    let packed = packet::encode(raw.clone());
    assert!(packed.len() < raw.len() / 8);
    let mut framed = (packed.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&packed);

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
