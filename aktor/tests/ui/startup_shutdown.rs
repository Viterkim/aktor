use aktor::*;
use core::time::Duration;

fn missing_shutdown() {
    let _startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioThread,
            start: async || Ok::<_, String>(0_u32),
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        grace: Duration::from_secs(5),
    });
}
