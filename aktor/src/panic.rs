use alloc::{boxed::Box, string::String};
use er::*;
use std::panic::{self, AssertUnwindSafe};

type Payload = Box<dyn core::any::Any + Send>;

/// The original unwind and any panics caught while closing the call or dropping other work.
pub struct Failure {
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub operation: Option<crate::operation::Operation>,
    pub payload: Payload,
    pub later: alloc::vec::Vec<Payload>,
}

#[derive(Er)]
#[er(format = "{message}")]
pub(crate) struct PanicErr {
    message: String,
}

fn message(payload: &Payload) -> &str {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "panic payload had no message"
    }
}

// The catch site isn't where the actor panicked.
pub(crate) fn panic_tree(payload: &Payload) -> ErBranch<PanicErr> {
    if let Some(failure) = payload.downcast_ref::<Failure>() {
        return panic_tree(&failure.payload).er_add(failure.later.iter().map(panic_tree));
    }

    PanicErr::new(message(payload)).into()
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "bevy"
))]
impl Failure {
    pub(crate) fn push(&mut self, payload: Payload) {
        self.later.push(payload);
    }
}

#[cfg(any(
    feature = "tokio",
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm"),
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub(crate) fn each(payload: Payload, mut caught: impl FnMut(Payload, bool)) {
    fn visit(payload: Payload, secondary: bool, caught: &mut impl FnMut(Payload, bool)) {
        match payload.downcast::<Failure>() {
            Ok(failure) => {
                visit(failure.payload, secondary, caught);
                for payload in failure.later {
                    visit(payload, true, caught);
                }
            }
            Err(payload) => caught(payload, secondary),
        }
    }
    visit(payload, false, &mut caught);
}

pub fn dispose_secondary(payload: Box<dyn std::any::Any + Send>) {
    dispose_with(payload, |_| {});
}

pub fn dispose_with(
    payload: Box<dyn std::any::Any + Send>,
    report: impl FnOnce(&Box<dyn std::any::Any + Send>),
) {
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        report(&payload);
        std::mem::forget(payload);
    }
}
