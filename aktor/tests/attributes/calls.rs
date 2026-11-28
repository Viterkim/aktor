#![deny(deprecated)]
use aktor::{
    Aktor, AktorGroup, AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor, aktor_start,
};
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

fn counter() -> (
    Aktor<u32>,
    aktor::AktorStarted<(), AktorGroup>,
    std::sync::mpsc::Sender<()>,
) {
    let (release, gate) = std::sync::mpsc::channel();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "attribute probe".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async move || {
                gate.recv().unwrap();
                0
            }),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    });

    {
        let (group, handle) = started.into_parts();
        (handle, group, release)
    }
}

#[test]
fn composition() {
    aktor::executor::block_on(async {
        macro_rules! check {
            ($function:ident $(, $input:expr)?) => {
                VISITS.store(0, Ordering::Relaxed);
                assert_eq!($function(&mut 0 $(, $input)?).await, 1);
                assert_eq!(VISITS.swap(0, Ordering::Relaxed), 1, "direct {}", stringify!($function));

                let (handle, _group, release) = counter();
                let reply = $function(&handle $(, $input)?).send().await.unwrap();
                assert_eq!(VISITS.load(Ordering::Relaxed), 0, "hook ran on submission");
                release.send(()).unwrap();
                assert_eq!(reply.await.unwrap(), 1);
                assert_eq!(VISITS.swap(0, Ordering::Relaxed), 1, "queued {}", stringify!($function));
            };
        }

        #[allow(deprecated)]
        {
            assert_eq!(old(&mut 0).await, 1);

            let (handle, _group, release) = counter();
            let reply = old_generic(&handle, ()).send().await.unwrap();

            release.send(()).unwrap();
            assert_eq!(reply.await.unwrap(), 1);
        }

        check!(outer);
        check!(inner);
        check!(conditional, ());
    });
}
