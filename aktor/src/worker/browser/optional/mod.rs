mod impls;

use super::*;

/// A worker whose ordinary calls return transport failures to their caller.
/// Choose this at construction when the worker is optional to the application.
pub struct OptionalWorker<S, Role = (), E = ()> {
    worker: Result<Worker<S, Role, E>, Unavailable<E>>,
}

#[must_use = "await the request or send it"]
pub struct OptionalRequest<'a, S, O, Role = ()> {
    request: Result<WorkerRequest<'a, S, O, Role>, WorkerError>,
}

#[must_use = "await the reply to observe its outcome"]
pub struct OptionalReply<O> {
    reply: WorkerReply<O>,
}

struct Unavailable<E> {
    error: WorkerError,
    completion: Completion<E>,
}
