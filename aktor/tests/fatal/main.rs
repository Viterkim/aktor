#![cfg(feature = "macros")]

use aktor::*;

#[path = "../support/mod.rs"]
pub mod support;

use std::sync::{Arc, Mutex};

mod task;
