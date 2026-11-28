use er::*;
use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{Notify, Semaphore},
};
use tokio_util::sync::CancellationToken;
use utp::{SocketOpts, UtpSocketUdp, UtpStream};

const ACKED: Duration = Duration::from_secs(5);

// Leave time for retransmission without spending the whole shutdown budget on a dead peer.
const SETTLE: Duration = Duration::from_secs(1);

const FIN: Duration = Duration::from_secs(1);

pub struct Socket {
    pub socket: Arc<UtpSocketUdp>,
    life: Arc<Life>,
    connecting: Semaphore,
}

/// Closing streams still need the socket to retransmit replies and acknowledge the peer's FIN.
struct Life {
    cancel: CancellationToken,
    unacked: AtomicUsize,
    acked: Notify,
}
impl Drop for Life {
    fn drop(&mut self) {
        if self.cancel.is_cancelled() {
            return;
        }

        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                // The last stream's FIN still needs the client socket after its handle is gone.
                let cancel = self.cancel.clone().drop_guard();
                runtime.spawn(async move {
                    tokio::time::sleep(FIN).await;
                    drop(cancel);
                });
            }
            Err(_) => self.cancel.cancel(),
        }
    }
}

/// A dropped stream whose last bytes may still need the socket. Dropping this takes it off the
/// count, even when the runtime drops the close task without ever polling it.
struct Unacked(Arc<Life>);
impl Unacked {
    fn new(life: &Arc<Life>) -> Self {
        life.unacked.fetch_add(1, Ordering::AcqRel);
        Self(life.clone())
    }
}
impl Drop for Unacked {
    fn drop(&mut self) {
        if self.0.unacked.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.acked.notify_waiters();
        }
    }
}

pub struct Stream {
    inner: Option<UtpStream>,
    life: Arc<Life>,
}

#[derive(Er)]
pub struct BindUdpErr;
impl Socket {
    pub async fn bind(address: SocketAddr, capacity: usize) -> ErResult<Self, BindUdpErr> {
        let options = SocketOpts {
            disable_nagle: true,
            max_live_vsocks: NonZeroUsize::new(capacity),
            ..Default::default()
        };
        let cancel = options.cancellation_token.clone();
        let socket = UtpSocketUdp::new_udp_with_opts(address, options, Default::default())
            .await
            .er(())?;

        Ok(Self {
            socket,
            // uTP tracks four handshakes per peer. Each client socket has one peer.
            connecting: Semaphore::new(4),
            life: Arc::new(Life {
                cancel,
                unacked: AtomicUsize::new(0),
                acked: Notify::new(),
            }),
        })
    }
}

#[derive(Er)]
pub struct AcceptUdpErr;
impl Socket {
    pub async fn accept(&self) -> ErResult<Stream, AcceptUdpErr> {
        let stream = self.socket.accept().await.er(())?;

        Ok(self.wrap(stream))
    }
}

#[derive(Er)]
pub struct ConnectUdpErr;
impl Socket {
    pub async fn connect(&self, address: SocketAddr) -> ErResult<Stream, ConnectUdpErr> {
        let _slot = self.connecting.acquire().await.er(())?;
        let stream = self.socket.connect(address).await.er(())?;

        Ok(self.wrap(stream))
    }

    fn wrap(&self, stream: UtpStream) -> Stream {
        Stream {
            inner: Some(stream),
            life: self.life.clone(),
        }
    }

    pub async fn settle(&self, within: Duration) {
        let settled = async {
            loop {
                let acked = self.life.acked.notified();

                tokio::pin!(acked);
                acked.as_mut().enable();

                if self.life.unacked.load(Ordering::Acquire) == 0 {
                    return;
                }

                acked.await;
            }
        };

        let _settled = tokio::time::timeout(within.min(SETTLE), settled).await;
    }

    pub async fn close(self) {
        self.life.cancel.cancel();

        // uTP's cancelled dispatcher and its streams each own the bound socket until polled.
        while Arc::strong_count(&self.socket) > 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
impl Stream {
    pub(crate) fn pending_receipt(&self) -> impl Send + use<> {
        Unacked::new(&self.life)
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let Some(mut stream) = self.inner.take() else {
            return;
        };

        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let unacked = Unacked::new(&self.life);
        runtime.spawn(async move {
            let _ = tokio::time::timeout(ACKED, stream.flush()).await;

            // Dropping both halves wakes uTP to send FIN once the final reply has drained.
            drop(stream);
            drop(unacked);
        });
    }
}

fn closed() -> io::Error {
    io::Error::from(io::ErrorKind::NotConnected)
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Some(stream) => Pin::new(stream).poll_read(cx, buffer),
            None => Poll::Ready(Err(closed())),
        }
    }
}
impl AsyncWrite for Stream {
    fn is_write_vectored(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(AsyncWrite::is_write_vectored)
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            Some(stream) => Pin::new(stream).poll_write(cx, bytes),
            None => Poll::Ready(Err(closed())),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            Some(stream) => Pin::new(stream).poll_write_vectored(cx, buffers),
            None => Poll::Ready(Err(closed())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Some(stream) => Pin::new(stream).poll_flush(cx),
            None => Poll::Ready(Err(closed())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Err(closed())),
        }
    }
}
