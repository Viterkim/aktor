#![forbid(unsafe_code)]
#![cfg(unix)]
#![doc = include_str!("../README.md")]

mod client;
mod connection;
mod error;
mod latest;
mod packet;
mod request;
mod server;
#[cfg(feature = "udp")]
mod udp;
mod wire;

#[cfg(test)]
mod tests;

pub use client::{Client, ClientOptions, connect};
pub use connection::Address;
pub use error::{Cause, Error};
pub use latest::{LatestResults, LatestSender};
pub use request::{AktorRetryOpts, Reply, Request};
pub use server::{Server, ServerOptions, listen};

pub use aktor::message::CallError;
