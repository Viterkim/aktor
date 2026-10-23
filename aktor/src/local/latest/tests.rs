use super::*;

#[test]
fn cancelled() {
    let (handle, _owner) = crate::local::channel::<(), 1, ()>().unwrap();

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
