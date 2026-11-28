use aktor::*;
use core::num::NonZeroUsize;
use core::time::Duration;

struct Database;
struct Audio;

#[aktor(crate = aktor, role = Database)]
async fn read(state: &usize) -> usize {
    *state
}

async fn queued() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "audio".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || 0_usize),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    let audio = actors.handles().clone().with_role::<Audio>();
    let _request = read(&audio);
}
