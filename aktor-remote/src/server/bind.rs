use super::*;
use socket2::{Domain, SockAddr, Socket, Type};

pub struct Bound {
    pub listener: Listener,
    pub address: Address,
    pub socket: Option<SocketPath>,
}

#[derive(Er)]
pub struct BindErr {
    pub address: String,
}
pub async fn bind(address: &ListenAddress, options: &ServerOptions) -> ErResult<Bound, BindErr> {
    #[cfg(not(feature = "udp"))]
    let _ = options;

    let e = |_| match address {
        Address::Unix(options) => options.path.display().to_string(),
        Address::Tcp(address) => address.to_string(),
        #[cfg(feature = "udp")]
        Address::Udp(address) => address.to_string(),
    };

    match address {
        Address::Unix(config) => unix(&config.path, config.permissions).await.er(e),
        Address::Tcp(address) => {
            let listener = TcpListener::bind(address).await.er(e)?;
            let address = Address::Tcp(listener.local_addr().er(e)?);

            Ok(Bound {
                listener: Listener::Tcp(listener),
                address,
                socket: None,
            })
        }
        #[cfg(feature = "udp")]
        Address::Udp(address) => {
            let socket =
                crate::udp::Socket::bind(*address, options.max_connections.get().saturating_add(1))
                    .await
                    .er(e)?;
            let address = Address::Udp(socket.socket.bind_addr());

            Ok(Bound {
                listener: Listener::Udp(socket),
                address,
                socket: None,
            })
        }
    }
}

#[derive(Er)]
pub struct BindUnixErr {
    pub path: PathBuf,
    pub msg: &'static str,
}
async fn unix(path: &Path, permissions: u32) -> ErResult<Bound, BindUnixErr> {
    let e = |msg| (path, msg);
    let mut lock_path = path.as_os_str().to_owned();

    lock_path.push(".lock");

    // Keep this file between restarts so concurrent listeners lock the same inode.
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(permissions)
        .open(lock_path)
        .er(e("open the lock file"))?;

    lock.try_lock().er(e("socket is already owned"))?;

    let address = SockAddr::unix(path).er(e("socket address"))?;
    let listener = Socket::new(Domain::UNIX, Type::STREAM, None).er(e("create socket"))?;

    match listener.bind(&address) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            let stale = matches!(
                timeout(Duration::from_millis(100), UnixStream::connect(path)).await,
                Ok(Err(ref error)) if error.kind() == std::io::ErrorKind::ConnectionRefused
            );
            let socket = std::fs::symlink_metadata(path)
                .er(e("check the old socket"))?
                .file_type()
                .is_socket();

            if !stale || !socket {
                er_bail!(e("socket is in use"), error);
            }

            std::fs::remove_file(path).er(e("remove the stale socket"))?;
            listener.bind(&address).er(e("bind"))?;
        }
        Err(error) => er_bail!(e("bind"), error),
    }

    let metadata = std::fs::symlink_metadata(path).er(e("read the socket"))?;
    let socket = SocketPath {
        path: Some(path.into()),
        device: metadata.dev(),
        inode: metadata.ino(),
        _lock: lock,
    };

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(permissions))
        .er(e("set socket permissions"))?;
    listener
        .set_nonblocking(true)
        .er(e("make it nonblocking"))?;
    listener.listen(1024).er(e("listen"))?;

    let listener = UnixListener::from_std(listener.into()).er(e("hand it to tokio"))?;

    Ok(Bound {
        listener: Listener::Unix(listener),
        address: Address::Unix(path.to_owned()),
        socket: Some(socket),
    })
}
