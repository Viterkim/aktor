use aktor::*;
use core::time::Duration;

fn missing_shutdown() {
    let _startup = aktor_start(AktorSetup {
        actors: AktorNew {

            name: AktorName::new("counter"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
start: async || Ok(0_u32),

                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        },
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    });
}
