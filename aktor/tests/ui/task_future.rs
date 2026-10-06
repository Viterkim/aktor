use aktor::*;
use std::rc::Rc;

#[aktor(crate = aktor)]
async fn read(_: &mut u32) -> usize {
    let value = Rc::new(7);
    std::future::ready(()).await;
    *value
}

async fn queued() {
    let actors = start(AktorSetup {
        name: AktorName::new("counter"),
        role: AktorNoRole,
        kind: AktorKind::TokioTask,
        closures: AktorClosures {
            start: async || Ok(0_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    })
    .await
    .unwrap();

    read(&actors.handles).await;
}
