use aktor::*;
use core::num::NonZeroUsize;

fn missing_end() {
    let _actor = AktorNew {
        name: "counter".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || 0_u32),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };
}
