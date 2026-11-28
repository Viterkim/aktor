use super::*;
use core::num::NonZeroUsize;

#[test]
fn cancelled() {
    let (handle, _owner) = crate::local::channel::<(), ()>(NonZeroUsize::MIN);

    for _ in 0..1000 {
        let (sender, results) = (&handle).session(
            crate::operation::Operation {
                name: "cancelled",
                caller: core::panic::Location::caller(),
            },
            async |_, input: u32| input,
        );
        sender.send(9);
        drop((sender, results));
    }

    assert!(handle.inner.services.borrow().is_empty());
}
