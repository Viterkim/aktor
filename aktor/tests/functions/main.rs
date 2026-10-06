#![cfg(feature = "macros")]

use aktor::listener::{FailureKind, channel, spawn, spawn_local, spawn_thread};
use aktor::message::Reply;
use aktor::*;

mod calls;
mod composition;
#[cfg(feature = "wasm_browser_workers")]
mod data;
mod location;
mod outputs;
mod signatures;
