use alloc::{boxed::Box, string::String};
use std::panic::{self, AssertUnwindSafe};

pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    {
        if let Some(failure) = payload.downcast_ref::<crate::listener::failure::FailurePanic>() {
            return panic_message(&failure.payload);
        }

        if let Some(failure) = payload.downcast_ref::<crate::listener::failure::Failure>() {
            let mut message = panic_message(&failure.payload);

            for diagnostic in &failure.diagnostics {
                message.push('\n');
                message.push_str(&diagnostic.diagnostics);
            }

            return message;
        }
    }

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
