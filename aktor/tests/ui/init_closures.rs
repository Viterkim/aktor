use aktor::*;

fn missing_end() {
    let _actor = AktorNew {
        name: AktorName::new("counter"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(0_u32),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Default::default(),
    };
}
