use aktor::*;
use core::num::NonZeroUsize;
use std::io::Error;

fn database() -> impl AktorDefinition<Aktor<Result<u32, Error>>, Error> {
    AktorNew {
        name: "database".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Try(async || {
            Err::<u32, _>((Error::other("open failed"), "open failed".into()))
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    }
}
