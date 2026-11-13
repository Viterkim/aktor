use super::*;
use socket2::{Domain, SockAddr, Socket, Type};

pub struct Bound {
    pub listener: Listener,
    pub address: Address,
    pub socket: Option<SocketPath>,
}

pub async fn bind(address: Address, options: &ServerOptions) -> Result<Bound, Failure> {
    match address {
        Address::Unix(path) => unix(&path, options).await,
        Address::Tcp(address) => {
            let listener = TcpListener::bind(address).await?;
            let address = Address::Tcp(listener.local_addr()?);
            Ok(Bound {
                listener: Listener::Tcp(listener),
                address,
                socket: None,
            })
        }
        #[cfg(feature = "udp")]
        Address::Udp(address) => {
            let socket =
                crate::udp::Socket::bind(address, options.max_connections.saturating_add(1))
                    .await?;
            let address = Address::Udp(socket.socket.bind_addr());
            Ok(Bound {
                listener: Listener::Udp(socket),
                address,
                socket: None,
            })
        }
    }
}

async fn unix(path: &Path, options: &ServerOptions) -> Result<Bound, Failure> {
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    // Keep this file between restarts so concurrent listeners lock the same inode.
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(options.permissions)
        .open(lock_path)?;
    lock.try_lock().map_err(|error| {
        Failure::new(
            CallError::NotAdmitted,
            Cause::Connection,
            format!("socket {} is already owned: {error}", path.display()),
        )
    })?;

    let address = SockAddr::unix(path)?;
    let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    match listener.bind(&address) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            let stale = matches!(
                timeout(Duration::from_millis(100), UnixStream::connect(path)).await,
                Ok(Err(ref error)) if error.kind() == std::io::ErrorKind::ConnectionRefused
            );

            if !stale || !std::fs::symlink_metadata(path)?.file_type().is_socket() {
                return Err(error.into());
            }

            std::fs::remove_file(path)?;
            listener.bind(&address)?;
        }
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    let socket = SocketPath {
        path: path.into(),
        device: metadata.dev(),
        inode: metadata.ino(),
        _lock: lock,
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(options.permissions))?;
    listener.set_nonblocking(true)?;
    listener.listen(1024)?;
    let listener = UnixListener::from_std(listener.into())?;

    Ok(Bound {
        listener: Listener::Unix(listener),
        address: Address::Unix(path.to_owned()),
        socket: Some(socket),
    })
}
