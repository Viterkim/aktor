use super::super::*;

/// Capacity bounds queued work. One operation can also be running.
#[doc(hidden)]
pub fn channel<S, E>(capacity: core::num::NonZeroUsize) -> Channel<S, E> {
    channel_with_clock::<S, E, ()>(capacity)
}

#[doc(hidden)]
pub fn channel_with_clock<S, E, Clock>(capacity: core::num::NonZeroUsize) -> Channel<S, E, Clock> {
    build(capacity.get())
}

/// One slot for a handle whose actor never launched, the caller closes it right away.
pub(crate) fn closed_channel<S, Clock>() -> Channel<S, (), Clock> {
    let (handle, owner) = build(1);
    owner.inner.finish(Ok(()));
    (handle, owner)
}

fn build<S, E, Clock>(capacity: usize) -> Channel<S, E, Clock> {
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
            lost: Default::default(),
            refused: Default::default(),
            returned: Default::default(),
            cleanup_completed: Cell::new(false),
            init: Cell::new(crate::AktorInitEvidence::NeverEntered),
            ready: RefCell::new(None),
            result: Rc::new(RefCell::new(None)),
            diagnostics: Rc::new(RefCell::new(alloc::vec::Vec::new())),
            cut_call: Cell::new(false),
            ran_out: Cell::new(false),
            changed: Event::default(),
        }),
    });

    (
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        Owner {
            inner: Rc::new(OwnerState {
                mailbox: inner,
                result: Default::default(),
            }),
            hooks: hooks::CallHooks::default(),
        },
    )
}
