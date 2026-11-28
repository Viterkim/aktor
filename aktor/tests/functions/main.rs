#![cfg(feature = "macros")]

use aktor::message::Reply;
use aktor::*;
use core::num::NonZeroUsize;

mod calls;
mod composition;
#[cfg(feature = "wasm_browser_workers")]
mod data;
mod location;
mod outputs;
#[cfg(feature = "testing")]
mod presets;
mod signatures;

/// The handles and the group that owns the actors, keep the group around while calling.
fn start<Actors: AktorStart<Group = AktorGroup>>(
    actors: Actors,
) -> (Actors::Handles, AktorStarted<(), AktorGroup, Actors::Error>) {
    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors,
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    });

    {
        let (owner, actors) = started.into_parts();
        (actors, owner)
    }
}

/// An actor holding `state` on its own thread, for calling through a handle.
fn thread_actor<S: Send + 'static>(state: S) -> (Aktor<S>, AktorStarted<(), AktorGroup>) {
    start(AktorNew {
        name: "test".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async move || state),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    })
}
