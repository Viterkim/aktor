// A TokioTask call runs on another task, so a body holding an `Rc` across `.await` has to be
// rejected. rustc used to say "future cannot be sent between threads safely" and point at the `Rc`.
// Since calls are `IntoFuture` it only says the call isn't a future, until the dispatch redesign
// can ask for `Send` where the call gets built.
use aktor::*;
use core::num::NonZeroUsize;
use core::time::Duration;
use std::rc::Rc;

#[aktor(crate = aktor)]
async fn read(_: &mut u32) -> usize {
    let value = Rc::new(7);
    std::future::ready(()).await;
    *value
}

async fn queued() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || 0_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    read(actors.handles()).await;
}
