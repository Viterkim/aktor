use aktor::*;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let aktor_setup = AktorNew {
        name: AktorName::new("counter"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,

        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(0_u32),
            end: Some(
                (async |count: u32| {
                    println!("final count: {count}");
                    Ok(())
                })
                .into(),
            ),
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
            intervals: vec![],
        },
        options: Default::default(),
    };

    let actors = aktor_start(AktorSetup {
        actors: aktor_setup,
        shutdown: |report| {
            if report.failed() {
                eprintln!("{report}");
            }
            println!("application cleanup finished");
        },
        options: Default::default(),
    })
    .await?;

    println!("reply: {}", add(&actors.handles, 2).await);
    println!("reply: {}", add(&actors.handles, 3).await);
    let report = actors.shutdown().await;
    if report.failed() {
        return Err(report.into());
    }

    Ok(())
}
