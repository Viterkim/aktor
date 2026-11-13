use crate::{
    AktorRetryOpts, CallError, Cause, Error, Failure, Request,
    connection::{Address, BufferedStream, Stream, idle},
    wire::{self, Message},
};
use aktor::{
    dispatch::Transport,
    operation::Operation,
    worker::{Codec, Operations},
};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, oneshot, watch},
    time::{Instant, sleep, sleep_until, timeout},
};

mod impls;

#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Change this on both sides when a type's wire layout changes.
    pub build: String,
    /// Time for queueing, reconnecting and waiting for the reply.
    pub timeout: Duration,
    pub retry_delay: Duration,
    pub max_frame: usize,
}

pub struct Client<S, Role = ()> {
    pub(crate) inner: Arc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
}

pub(crate) struct Inner {
    address: Address,
    pub options: ClientOptions,
    operations: Operations,
    pub frames: crate::wire::FrameReader,
    #[cfg(feature = "udp")]
    udp: tokio::sync::OnceCell<crate::udp::Socket>,
    connection: Mutex<Connection>,
}

struct Connection {
    stream: Option<Active>,
    next: u64,
}

pub(crate) struct Active {
    pub stream: BufferedStream,
    pub max_frame: usize,
    pub compress: bool,
}

pub async fn connect<S: 'static>(
    address: impl Into<Address>,
) -> Result<Client<S>, (Error, String)> {
    Client::connect_with(address, ClientOptions::default()).await
}
