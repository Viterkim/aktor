#![forbid(unsafe_code)]
#![cfg(unix)]
#![doc = include_str!("../README.md")]

mod client;
mod connection;
mod error;
mod latest;
mod packet;
mod receipt;
mod request;
mod server;
#[cfg(feature = "udp")]
mod udp;
mod wire;

#[cfg(test)]
mod tests;

pub use client::{Client, ClientOptions, ClientOptionsError};
pub use connection::{Address, ListenAddress, UnixListenerOptions};
pub use error::Cause;
use error::{Rejection, WireFailure};
pub use latest::{LatestResults, LatestSender};
pub use request::{AktorRetryOptions, Reply, Request};
pub use server::{Server, ServerError, ServerFailure, ServerOptions};

pub use aktor::{AktorDeliveryError, AktorDeliveryOutcome};
