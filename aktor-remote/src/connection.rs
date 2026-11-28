use er::*;
use std::{
    fmt, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader, ReadBuf},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
};

#[derive(Clone, Debug)]
pub enum Address<U = PathBuf> {
    Unix(U),
    Tcp(SocketAddr),
    #[cfg(feature = "udp")]
    Udp(SocketAddr),
}

/// A Unix listener sets the socket and lock file mode when binding.
#[derive(Clone, Debug)]
pub struct UnixListenerOptions {
    pub path: PathBuf,
    pub permissions: u32,
}

pub type ListenAddress = Address<UnixListenerOptions>;

pub enum Stream {
    Unix(UnixStream),
    Tcp(TcpStream),
    #[cfg(feature = "udp")]
    Udp(crate::udp::Stream),
}

pub type BufferedStream = BufReader<Stream>;

pub enum Listener {
    Unix(UnixListener),
    Tcp(TcpListener),
    #[cfg(feature = "udp")]
    Udp(crate::udp::Socket),
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "{}", path.display()),
            Self::Tcp(address) => write!(f, "{address}"),
            #[cfg(feature = "udp")]
            Self::Udp(address) => write!(f, "udp://{address}"),
        }
    }
}
impl From<PathBuf> for Address {
    fn from(path: PathBuf) -> Self {
        Self::Unix(path)
    }
}
impl From<&Path> for Address {
    fn from(path: &Path) -> Self {
        Self::Unix(path.to_owned())
    }
}
impl From<&PathBuf> for Address {
    fn from(path: &PathBuf) -> Self {
        Self::Unix(path.clone())
    }
}
impl From<SocketAddr> for Address {
    fn from(address: SocketAddr) -> Self {
        Self::Tcp(address)
    }
}

pub fn finish(
    mut stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    budget: std::time::Duration,
) {
    use tokio::io::AsyncWriteExt;

    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    // A half close cancels the request while the peer can still finish its last write.
    runtime.spawn(async move {
        let close = async {
            stream.shutdown().await?;
            tokio::io::copy(&mut stream, &mut tokio::io::sink()).await
        };
        let _ = tokio::time::timeout(budget, close).await;
    });
}

impl Stream {
    pub fn buffered(self) -> BufferedStream {
        BufReader::with_capacity(4 * 1024, self)
    }

    pub fn compresses(&self) -> bool {
        match self {
            Self::Unix(_) => false,
            Self::Tcp(stream) => stream
                .peer_addr()
                .is_ok_and(|peer| !peer.ip().is_loopback()),
            #[cfg(feature = "udp")]
            Self::Udp(_) => true,
        }
    }
}

#[derive(Er)]
pub struct IdleDataErr;
pub fn idle(stream: &mut BufferedStream) -> ErResult<bool, IdleDataErr> {
    if !stream.buffer().is_empty() {
        er_bail!(());
    }

    let mut byte = [0];
    let result = match stream.get_ref() {
        Stream::Unix(stream) => Some(stream.try_read(&mut byte)),
        Stream::Tcp(stream) => Some(stream.try_read(&mut byte)),
        #[cfg(feature = "udp")]
        Stream::Udp(_) => None,
    };
    if let Some(result) = result {
        return match result {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(true),
            Err(_) | Ok(0) => Ok(false),
            Ok(_) => er_bail!(()),
        };
    }

    let mut buffer = ReadBuf::new(&mut byte);
    let mut context = Context::from_waker(Waker::noop());

    match Pin::new(stream).poll_read(&mut context, &mut buffer) {
        Poll::Pending => Ok(true),
        Poll::Ready(Err(_)) => Ok(false),
        Poll::Ready(Ok(())) if buffer.filled().is_empty() => Ok(false),
        Poll::Ready(Ok(())) => er_bail!(()),
    }
}

#[derive(Er)]
pub enum AcceptStreamErr {
    Retry,
    Backoff,
    Fatal,
}
impl Listener {
    pub async fn accept(&self) -> ErResult<Stream, AcceptStreamErr> {
        match self {
            Self::Unix(listener) => {
                let (stream, _) = listener
                    .accept()
                    .await
                    .er_with(|error| self.accept_error(error))?;
                Ok(Stream::Unix(stream))
            }
            Self::Tcp(listener) => {
                let (stream, _) = listener
                    .accept()
                    .await
                    .er_with(|error| self.accept_error(error))?;
                stream.set_nodelay(true).er(AcceptStreamErr::retry)?;

                Ok(Stream::Tcp(stream))
            }
            #[cfg(feature = "udp")]
            Self::Udp(listener) => {
                let stream = listener.accept().await.er(AcceptStreamErr::fatal)?;
                Ok(Stream::Udp(stream))
            }
        }
    }

    fn accept_error(&self, error: &io::Error) -> AcceptStreamErr {
        use io::ErrorKind;
        use rustix::io::Errno;

        let errno = error.raw_os_error().map(Errno::from_raw_os_error);
        if error.kind() == ErrorKind::OutOfMemory
            || matches!(
                errno,
                Some(Errno::MFILE | Errno::NFILE | Errno::NOMEM | Errno::NOBUFS)
            )
        {
            return AcceptStreamErr::Backoff;
        }

        #[cfg(target_os = "linux")]
        if matches!(self, Self::Tcp(_))
            && matches!(
                errno,
                Some(
                    Errno::PROTO
                        | Errno::NETDOWN
                        | Errno::NOPROTOOPT
                        | Errno::HOSTDOWN
                        | Errno::NONET
                        | Errno::HOSTUNREACH
                        | Errno::OPNOTSUPP
                        | Errno::NETUNREACH
                )
            )
        {
            // Linux can return a pending connection's network error from accept().
            return AcceptStreamErr::Retry;
        }

        match error.kind() {
            ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::TimedOut => AcceptStreamErr::Retry,
            _ => AcceptStreamErr::Fatal,
        }
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(stream) => Pin::new(stream).poll_read(cx, buffer),
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buffer),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => Pin::new(stream).poll_read(cx, buffer),
        }
    }
}
impl AsyncWrite for Stream {
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Unix(stream) => stream.is_write_vectored(),
            Self::Tcp(stream) => stream.is_write_vectored(),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unix(stream) => Pin::new(stream).poll_write(cx, bytes),
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, bytes),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => Pin::new(stream).poll_write(cx, bytes),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unix(stream) => Pin::new(stream).poll_write_vectored(cx, buffers),
            Self::Tcp(stream) => Pin::new(stream).poll_write_vectored(cx, buffers),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => Pin::new(stream).poll_write_vectored(cx, buffers),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "udp")]
            Self::Udp(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
