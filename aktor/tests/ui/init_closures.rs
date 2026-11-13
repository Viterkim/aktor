use aktor::*;

fn missing_end() {
    let _actor = AktorNew {
        name: "counter".into(),
        kind: AktorKind::TokioThread,
        start: async || Ok::<_, String>(0_u32),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };
}
