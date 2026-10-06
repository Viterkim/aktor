use aktor::listener::{
    AbandonedSetup, DedicatedStartError, Handle, LifecycleError, ReplaceError, RunError,
    WeakHandle, channel, spawn, spawn_local, spawn_local_with_policy, spawn_runner,
};
use aktor::message::{ActorError, Reply, Request, TrySendError, call};
use aktor::*;

#[path = "../support/mod.rs"]
pub mod support;

use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};
use support::{panics, poll};

mod admission;
mod deadlines;
#[cfg(feature = "macros")]
mod latest;
mod ownership;
mod pause;
mod replace;
mod runner;
#[cfg(all(feature = "macros", not(target_family = "wasm")))]
mod setup;
mod shutdown;
mod state;

#[cfg(feature = "macros")]
mod task;

#[cfg(all(feature = "macros", feature = "local", feature = "embassy_cross_core"))]
mod teardown;
