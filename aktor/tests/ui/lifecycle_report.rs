use aktor::*;

fn setup() {
    let _startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("database"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Err::<u32, _>(std::io::Error::other("raw error")),
                end: (),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
        shutdown: |_| {},
        options: Default::default(),
    });
}
