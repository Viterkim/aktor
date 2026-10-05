use aktor::*;

struct Database;
struct Audio;

#[aktor(crate = aktor, role = Database)]
async fn read(state: &usize) -> usize {
    *state
}

async fn queued() {
    let actors = aktor::start(AktorSetup {
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
        options: None,
    })
    .await
    .unwrap();

    let _request = read(&actors.handles);
}
