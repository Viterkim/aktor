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
            name: "audio".into(),
            kind: AktorKind::TokioThread,
            start: async || Ok::<_, String>(0_usize),
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .unwrap();

    let audio = actors.handles.clone().with_role::<Audio>();
    let _request = read(&audio);
}
