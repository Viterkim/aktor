use super::super::*;

/// Capacity bounds queued work. One operation can also be running.
#[doc(hidden)]
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, AktorStateError> {
    channel_with_capacity(N)
}

#[doc(hidden)]
pub fn channel_with_capacity<S, const N: usize, E>(
    capacity: usize,
) -> Result<Channel<S, N, E>, AktorStateError> {
    channel_with_clock::<S, N, E, ()>(capacity)
}

#[doc(hidden)]
pub fn channel_with_clock<S, const N: usize, E, Clock>(
    capacity: usize,
) -> Result<Channel<S, N, E, Clock>, AktorStateError> {
    if capacity == 0 {
        return Err(AktorStateError::InvalidCapacity);
    }

    let inner = Rc::new(Inner {
        queue: RefCell::new(VecDeque::new()),
        services: RefCell::new(VecDeque::new()),
        prefer_service: Cell::new(false),
        group: RefCell::new(None),
        sessions: RefCell::new(alloc::vec::Vec::new()),
        prune_at: Cell::new(64),
        open: Cell::new(true),
        cancelled: Cell::new(false),
        handles: Cell::new(1),
        capacity,
        owner_waker: RefCell::new(None),
        space: Event::default(),
        closed: Event::default(),
        completion: Rc::new(Completed {
            cleanup_completed: Cell::new(false),
            ready: RefCell::new(None),
            result: RefCell::new(None),
            diagnostics: Rc::new(RefCell::new(alloc::vec::Vec::new())),
            panic_reported: Cell::new(false),
            cut_call: Cell::new(false),
            ran_out: Cell::new(false),
            changed: Event::default(),
        }),
    });

    Ok((
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        Owner {
            inner,
            hooks: hooks::CallHooks::default(),
        },
    ))
}
