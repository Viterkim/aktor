use crate::{CallError, Cause, Failure};
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
pub enum Address {
    Unix(PathBuf),
    Tcp(SocketAddr),
    #[cfg(feature = "udp")]
    Udp(SocketAddr),
}

pub enum Stream {
    Unix(UnixStream),
    Tcp(TcpStream),
    #[cfg(feature = "udp")]
    Udp(utp::UtpStream),
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
impl From<&str> for Address {
    fn from(path: &str) -> Self {
        Self::Unix(path.into())
    }
}
impl From<String> for Address {
    fn from(path: String) -> Self {
        Self::Unix(path.into())
    }
}
impl From<SocketAddr> for Address {
    fn from(address: SocketAddr) -> Self {
        Self::Tcp(address)
    }
}
impl From<&String> for Address {
    fn from(path: &String) -> Self {
        Self::Unix(path.into())
    }
}

impl Stream {
    pub fn configure(&self) -> io::Result<()> {
        if let Self::Tcp(stream) = self {
            stream.set_nodelay(true)?;
        }

        Ok(())
    }

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

pub fn idle(stream: &mut BufferedStream) -> Result<bool, Failure> {
    let unexpected = || {
        Failure::new(
            CallError::NotAdmitted,
            Cause::Protocol,
            "unexpected data on an idle connection",
        )
    };
    if !stream.buffer().is_empty() {
        return Err(unexpected());
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
            Ok(_) => Err(unexpected()),
        };
    }

    let mut buffer = ReadBuf::new(&mut byte);
    let mut context = Context::from_waker(Waker::noop());

    match Pin::new(stream).poll_read(&mut context, &mut buffer) {
        Poll::Pending => Ok(true),
        Poll::Ready(Err(_)) => Ok(false),
        Poll::Ready(Ok(())) if buffer.filled().is_empty() => Ok(false),
        Poll::Ready(Ok(())) => Err(unexpected()),
    }
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Stream> {
        match self {
            Self::Unix(listener) => listener
                .accept()
                .await
                .map(|(stream, _)| Stream::Unix(stream)),
            Self::Tcp(listener) => listener
                .accept()
                .await
                .map(|(stream, _)| Stream::Tcp(stream)),
            #[cfg(feature = "udp")]
            Self::Udp(listener) => listener
                .socket
                .accept()
                .await
                .map(Stream::Udp)
                .map_err(io::Error::other),
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
