use core::fmt;
use er::Er;
use std::{
    any::Any,
    io::Write,
    panic::{self, AssertUnwindSafe},
    sync::Arc,
};

pub mod capture;
pub mod impls;
pub use capture::dispose_secondary;

pub use crate::operation::Operation;

#[derive(Clone, Copy, Debug)]
pub enum FailureKind {
    Operation(Operation),
    Cleanup,
    Teardown,
    Cancelled,
    Runtime,
}

pub struct Failure {
    pub actor: String,
    pub kind: FailureKind,
    pub payload: Box<dyn Any + Send>,
}

#[derive(Clone, Default)]
pub enum FailurePolicy {
    Abort,
    Shutdown(Arc<dyn Fn(&Failure) + Send + Sync>),
    #[default]
    Unwind,
}

pub struct Failures {
    pub actor: String,
    pub first: Option<Failure>,
}

pub struct FailurePanic {
    pub kind: FailureKind,
    pub payload: Box<dyn Any + Send>,
}

#[derive(Er, PartialEq, Eq)]
pub enum RunError<E> {
    Failed(#[er(source)] E),
    #[er(format = "actor serving loop panicked")]
    Panicked,
}
