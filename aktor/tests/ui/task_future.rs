use aktor::*;
use core::time::Duration;
use std::rc::Rc;

#[aktor(crate = aktor)]
async fn read(_: &mut u32) -> usize {
    let value = Rc::new(7);
    std::future::ready(()).await;
    *value
}

async fn queued() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioTask,
            start: async || Ok::<_, String>(0_u32),
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

    read(&actors.handles).await;
}
