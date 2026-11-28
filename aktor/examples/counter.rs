use aktor::*;
use core::num::NonZeroUsize;
use er::*;
use std::time::Duration;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let aktor_setup = AktorNew {
        name: "counter".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || 0_u32),
        end: async |count: u32| {
            println!("final count: {count}");
        },
        hooks: AktorHooks {
            intervals: vec![],
            before_each: |_: &mut u32, operation: operation::Operation| {
                println!("starting {}", operation.name);
            },
            after_each: |count: &mut u32, operation: operation::Operation| {
                println!("finished {}, count: {count}", operation.name);
            },
        },
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: aktor_setup,
        shutdown: |_| println!("application cleanup finished"),
        grace: Duration::from_secs(5),
    });
    actors
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    println!("reply: {}", add(actors.handles(), 2).await.unwrap());
    println!("reply: {}", add(actors.handles(), 3).await.unwrap());

    actors.stop().await.into_result().er(())?;

    Ok(())
}
