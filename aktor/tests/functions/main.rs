#![cfg(feature = "macros")]

use aktor::listener::{FailureKind, channel, spawn, spawn_local, spawn_thread};
use aktor::message::{CallError, Reply};
use aktor::*;

mod calls;
mod composition;
mod location;
mod outputs;
mod signatures;
