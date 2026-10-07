use alloc::{boxed::Box, string::String};
use std::panic::{self, AssertUnwindSafe};

pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic payload had no message".into()
    }
}

pub fn dispose_secondary(payload: Box<dyn std::any::Any + Send>) {
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(payload);
    }
}
