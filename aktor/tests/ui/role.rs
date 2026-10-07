use aktor::*;
use core::time::Duration;

struct Database;
struct Audio;

#[aktor(crate = aktor, role = Database)]
async fn read(state: &usize) -> usize {
    *state
}

async fn queued() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {

            name: AktorName::new("audio"),
            role: Audio,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
start: async || Ok(0_usize),

                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let _request = read(&actors.handles);
}
