use aktor::*;
use std::io::Error;

fn database() -> impl AktorDefinition<Aktor<Result<u32, Error>>, Error> {
    AktorNew {
        name: "database".into(),
        kind: AktorKind::TokioThread,
        start: async || Err::<u32, _>(Error::other("open failed")),
        end: (),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    }
}
