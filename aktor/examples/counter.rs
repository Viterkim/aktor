use aktor::*;
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
        start: async || aktor_ready(0_u32),
        end: async |count: u32| {
            println!("final count: {count}");
        },
        hooks: AktorHooks {
            intervals: vec![],
            before_each: Some(
                (|_: &mut u32, operation: operation::Operation| {
                    println!("starting {}", operation.name);
                })
                .into(),
            ),
            after_each: Some(
                (|count: &mut u32, operation: operation::Operation| {
                    println!("finished {}, count: {count}", operation.name);
                })
                .into(),
            ),
        },
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        actors: aktor_setup,
        shutdown: |report| {
            if report.failed() {
                eprintln!("{report}");
            }

            println!("application cleanup finished");
        },
        grace: Duration::from_secs(5),
    })
    .await
    .er_report()?;

    println!("reply: {}", add(&actors.handles, 2).await);
    println!("reply: {}", add(&actors.handles, 3).await);

    actors.shutdown().await.into_result().er_report()?;

    Ok(())
}
