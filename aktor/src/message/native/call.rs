use super::*;
use crate::listener::Handle;

/// Call without the macro.
#[track_caller]
pub fn call<S: 'static, Role, F, I, O>(
    handle: &Handle<S, Role>,
    function: F,
    input: I,
) -> Request<'_, S, O>
where
    F: FnOnce(&mut S, I) -> O + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    let packet = Arc::new(Packet::new(function, input));

    Request {
        sender: &handle.inner.sender,
        admission: &handle.inner.admission,
        available: None,
        submission: Submission::Unsent(Message {
            operation: crate::listener::Operation {
                name: std::any::type_name::<F>(),
                caller: core::panic::Location::caller(),
            },
            job: packet.clone(),
            finished: false,
            counted: true,
        }),
        reply: Reply {
            admission: handle.inner.admission.clone(),
            answer: packet,
            finished: handle.inner.finished.clone(),
            closing: None,
            error: None,
            group: handle.inner.admission.group(),
            parked: false,
            taken: false,
        },
    }
}

#[track_caller]
pub fn call_async<S: 'static, Role, F, I, O>(
    handle: &Handle<S, Role>,
    function: F,
    input: I,
) -> Request<'_, S, O>
where
    F: for<'s> core::ops::AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    let packet = Arc::new(AsyncJob(Packet::new(function, input)));

    Request {
        sender: &handle.inner.sender,
        admission: &handle.inner.admission,
        available: None,
        submission: Submission::Unsent(Message {
            operation: crate::listener::Operation {
                name: std::any::type_name::<F>(),
                caller: core::panic::Location::caller(),
            },
            job: packet.clone(),
            finished: false,
            counted: true,
        }),
        reply: Reply {
            admission: handle.inner.admission.clone(),
            answer: packet,
            finished: handle.inner.finished.clone(),
            closing: None,
            error: None,
            group: handle.inner.admission.group(),
            parked: false,
            taken: false,
        },
    }
}
