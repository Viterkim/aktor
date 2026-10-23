use crate::{
    CallError, Cause, Error,
    connection::{Address, Listener, Stream},
    wire::{self, Message},
};
use aktor::{Aktor, listener::Handle, message::call_async, operation::Operation, worker::Registry};
use std::{
    fs::File,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixListener, UnixStream},
    sync::{broadcast, watch},
    task::JoinSet,
    time::timeout,
};

mod bind;
mod impls;
mod latest;
mod service;
mod sessions;
use service::Service;

#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub build: String,
    pub max_frame: usize,
    pub max_connections: usize,
    pub shutdown_timeout: Duration,
    /// Bounds handshakes, started frames and reply writes. Quiet connections stay open.
    pub io_timeout: Duration,
    pub permissions: u32,
}

/// Owns the listener and its client sessions. The actor belongs to its existing group.
pub struct Server {
    pub address: Address,
    stop: watch::Sender<bool>,
    finished: watch::Receiver<Option<Result<(), Error>>>,
    errors: broadcast::Sender<Error>,
    #[cfg(test)]
    pub pending: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pub cancelled: Arc<tokio::sync::Notify>,
}

struct SocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

pub async fn listen<S: 'static, Role: 'static>(
    address: impl Into<Address>,
    actor: &Aktor<S, Role>,
) -> Result<Server, Error> {
    Server::listen_with(address, actor, ServerOptions::default()).await
}
