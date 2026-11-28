mod answer;
mod bind;
mod failures;
mod impls;
mod latest;
mod service;
mod sessions;

use crate::{
    AktorDeliveryOutcome, Cause, Rejection, WireFailure,
    connection::{Address, BufferedStream, ListenAddress, Listener, Stream},
    wire::{self, Message},
};
use aktor::{Aktor, listener::Handle, message::call_async, operation::Operation, worker::Registry};
use er::*;
use failures::Failures;
pub use failures::{ServerError, ServerFailure};
use service::Service;
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
    sync::watch,
    task::JoinSet,
    time::timeout,
};

#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub build: String,
    pub max_frame: usize,
    pub max_connections: core::num::NonZeroUsize,
    pub shutdown_timeout: Duration,
    /// Time limit for handshakes, frames already started and reply writes.
    /// Quiet connections stay open.
    pub io_timeout: Duration,
}

/// Owns the listener and its client sessions. The actor belongs to its existing group.
pub struct Server {
    pub address: Address,
    stop: watch::Sender<bool>,
    task: tokio::sync::Mutex<ListenerTask>,
    failures: Failures,
    #[cfg(test)]
    pub pending: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pub cancelled: Arc<tokio::sync::Notify>,
}

enum ListenerTask {
    Running(tokio::task::JoinHandle<()>),
    Finished(Result<(), (ServerError, String)>),
}

struct SocketPath {
    path: Option<PathBuf>,
    device: u64,
    inode: u64,
    _lock: File,
}

#[derive(Er)]
struct UnknownOperationErr {
    operation: String,
}

fn preview(value: &str) -> Cow<'_, str> {
    const LIMIT: usize = 128;
    if value.len() <= LIMIT {
        return Cow::Borrowed(value);
    }

    let end = value.floor_char_boundary(LIMIT);
    let mut prefix = value[..end].to_owned();
    prefix.push_str("...");
    Cow::Owned(prefix)
}
