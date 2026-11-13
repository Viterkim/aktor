use crate::{
    CallError, Cause, Error, Failure,
    connection::{Address, BufferedStream, Listener, Stream},
    wire::{self, Message},
};
use aktor::{Aktor, listener::Handle, message::call_async, operation::Operation, worker::Registry};
use std::{
    borrow::Cow,
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
    /// Time limit for handshakes, frames already started and reply writes.
    /// Quiet connections stay open.
    pub io_timeout: Duration,
    pub permissions: u32,
}

/// Owns the listener and its client sessions. The actor belongs to its existing group.
pub struct Server {
    pub address: Address,
    stop: watch::Sender<bool>,
    finished: watch::Receiver<Option<Result<(), Failure>>>,
    errors: broadcast::Sender<(Error, String)>,
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

fn preview(value: &str) -> Cow<'_, str> {
    const LIMIT: usize = 128;
    if value.len() <= LIMIT {
        return Cow::Borrowed(value);
    }

    let end = value.floor_char_boundary(LIMIT);
    Cow::Owned(format!("{}...", &value[..end]))
}

pub async fn listen<S: 'static, Role: 'static>(
    address: impl Into<Address>,
    actor: &Aktor<S, Role>,
) -> Result<Server, (Error, String)> {
    Server::listen_with(address, actor, ServerOptions::default()).await
}
