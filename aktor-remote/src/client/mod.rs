mod impls;

use crate::{
    AktorDeliveryOutcome, AktorRetryOptions, Cause, Request,
    connection::{Address, BufferedStream, Stream, idle},
    request::RetryOptionsErr,
    wire::{self, Message},
};
use aktor::{
    dispatch::Transport,
    operation::Operation,
    worker::{Codec, Operations},
};
use er::*;
use impls::AttemptErr;
pub use impls::ClientOptionsError;
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, oneshot, watch},
    time::{Instant, sleep, sleep_until},
};

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
    idle: Vec<Active>,
    next: u64,
}

// Each idle connection reserves a slot on the server.
const IDLE: usize = 4;

enum Tried {
    Answered(ErResult<Vec<u8>, AttemptErr>),
    Options(ErTree<RetryOptionsErr>),
    Elapsed,
}

pub(crate) struct Active {
    pub stream: BufferedStream,
    pub max_frame: usize,
    pub compress: bool,
}

struct Exchange<'a> {
    client: &'a Inner,
    active: Option<Active>,
}
