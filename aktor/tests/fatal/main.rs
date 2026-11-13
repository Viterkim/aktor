use aktor::listener::{
    FailureKind, FailurePolicy, Handle, RunError, SpawnArgs, channel, spawn,
    spawn_local_with_policy, spawn_runner,
};
use aktor::message::{Request, call};
#[cfg(feature = "macros")]
use aktor::*;

#[path = "../support/mod.rs"]
pub mod support;

use std::sync::{Arc, Mutex};
use support::poll;
use tokio::sync::oneshot;

mod drop;
mod panic;
mod policy;
#[cfg(feature = "macros")]
mod task;

fn crash(handle: &Handle<usize>) -> Request<'_, usize, ()> {
    call(
        handle,
        |state, ()| {
            *state += 1;
            panic!("operation failed");
        },
        (),
    )
}
