use aktor::*;
use core::num::NonZeroUsize;
use core::time::Duration;

fn missing_shutdown() {
    let _startup = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || 0_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        grace: Duration::from_secs(5),
    });
}
