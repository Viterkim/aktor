use crate::{
    AktorRetryOpts, CallError, Cause, Error, Request,
    connection::{Address, Stream},
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
    /// Change on both sides when a transported type changes its wire layout.
    pub build: String,
    /// Includes queueing, reconnecting and waiting for the reply.
    pub timeout: Duration,
    pub retry_delay: Duration,
    pub max_frame: usize,
}

pub struct Client<S, Role = ()> {
    pub inner: Arc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
}

pub struct Inner {
    address: Address,
    pub options: ClientOptions,
    operations: Operations,
    #[cfg(feature = "udp")]
    udp: tokio::sync::OnceCell<crate::udp::Socket>,
    connection: Mutex<Connection>,
}

struct Connection {
    stream: Option<Active>,
    next: u64,
}

pub struct Active {
    pub stream: Stream,
    pub max_frame: usize,
}

pub async fn connect<S: 'static>(address: impl Into<Address>) -> Result<Client<S>, Error> {
    Client::connect_with(address, ClientOptions::default()).await
}
