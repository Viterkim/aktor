#![deny(deprecated)]
use aktor::aktor;
use std::sync::atomic::{AtomicUsize, Ordering};

static VISITS: AtomicUsize = AtomicUsize::new(0);

#[probe::trace]
#[aktor(crate = aktor)]
async fn outer(state: &mut u32) -> u32 {
    *state += 1;
    *state
}

#[aktor(crate = aktor)]
#[probe::trace]
async fn inner(state: &mut u32) -> u32 {
    *state += 1;
    *state
}

#[aktor(crate = aktor)]
#[cfg_attr(all(), probe::trace)]
async fn conditional<T: Send + 'static>(state: &mut u32, _: T) -> u32 {
    *state += 1;
    *state
}

#[aktor(crate = aktor)]
#[deprecated(note = "use fresh")]
async fn old(state: &mut u32) -> u32 {
    *state += 1;
    *state
}

#[aktor(crate = aktor)]
#[cfg_attr(all(), deprecated(note = "use fresh"))]
async fn old_generic<T: Send + 'static>(state: &mut u32, _: T) -> u32 {
    *state += 1;
    *state
}

#[cfg_attr(all(), aktor(crate = aktor))]
#[cfg(any())]
async fn absent(_: &Missing) -> Missing {
    missing()
}

#[aktor(crate = aktor)]
#[cfg(any())]
async fn also_absent(_: &Missing) -> Missing {
    missing()
}

#[test]
fn composition() {
    aktor::executor::block_on(async {
        macro_rules! check {
            ($function:ident $(, $input:expr)?) => {
                VISITS.store(0, Ordering::Relaxed);
                assert_eq!($function(&mut 0 $(, $input)?).await, 1);
                assert_eq!(VISITS.swap(0, Ordering::Relaxed), 1, "direct {}", stringify!($function));

                let (handle, mut listener) = aktor::listener::channel::<u32>(1).unwrap();
                let reply = $function(&handle $(, $input)?).send().await;
                assert_eq!(VISITS.load(Ordering::Relaxed), 0, "hook ran on submission");
                listener.recv().await.unwrap().run(&mut 0).await;
                assert_eq!(reply.await, 1);
                assert_eq!(VISITS.swap(0, Ordering::Relaxed), 1, "queued {}", stringify!($function));
            };
        }

        #[allow(deprecated)]
        {
            assert_eq!(old(&mut 0).await, 1);

            let (handle, mut listener) = aktor::listener::channel::<u32>(1).unwrap();
            let reply = old_generic(&handle, ()).send().await;

            listener.recv().await.unwrap().run(&mut 0).await;
            assert_eq!(reply.await, 1);
        }

        check!(outer);
        check!(inner);
        check!(conditional, ());
    });
}
