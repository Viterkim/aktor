use super::*;
use crate::listener::Handle;

#[doc(hidden)]
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
    let packet = Arc::new(Packet::new(
        function,
        input,
        Some(handle.inner.admission.clone()),
    ));

    let operation = crate::listener::Operation {
        name: std::any::type_name::<F>(),
        caller: core::panic::Location::caller(),
    };

    Request {
        sender: &handle.inner.sender,
        admission: &handle.inner.admission,
        submission: Submission::Unsent(Message {
            operation,
            job: packet.clone(),
            finished: false,
        }),
        reply: Reply {
            receive: true,
            admission: handle.inner.admission.clone(),
            answer: packet,
            taken: false,
        },
    }
}

#[doc(hidden)]
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
    let packet = Arc::new(AsyncJob(Packet::new(
        function,
        input,
        Some(handle.inner.admission.clone()),
    )));

    let operation = crate::listener::Operation {
        name: std::any::type_name::<F>(),
        caller: core::panic::Location::caller(),
    };

    Request {
        sender: &handle.inner.sender,
        admission: &handle.inner.admission,
        submission: Submission::Unsent(Message {
            operation,
            job: packet.clone(),
            finished: false,
        }),
        reply: Reply {
            receive: true,
            admission: handle.inner.admission.clone(),
            answer: packet,
            taken: false,
        },
    }
}

/// A message nobody waits on, like an interval's turn.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn detached<S: 'static, F>(
    operation: crate::listener::Operation,
    function: F,
) -> Message<S>
where
    F: for<'s> core::ops::AsyncFnOnce(&'s mut S, ()) + Send + 'static,
{
    Message {
        operation,
        job: Arc::new(AsyncJob(Packet::new(function, (), None))),
        finished: false,
    }
}
