#[path = "../../../cli.rs"]
mod cli;

use aktor_remote::{Address, AktorRetryOptions, Client, ClientOptions};
use cli::Report;
use power_api::{
    PowerManager, PowerRemote, WIRE_REVISION, get_state, power_cycle_gnss, set_target,
};
use std::{
    io::{self, Write},
    process::ExitCode,
};

#[tokio::main]
async fn main() -> ExitCode {
    cli::exit(run().await)
}

async fn run() -> Result<(), Report> {
    let path = std::env::args().nth(1).ok_or("socket path required")?;

    let power = Client::<PowerManager, PowerRemote>::new(
        Address::Unix(path.into()),
        ClientOptions {
            build: WIRE_REVISION.into(),
            timeout: std::time::Duration::from_secs(5),
            retry_delay: std::time::Duration::from_millis(100),
            max_frame: 4 * 1024 * 1024,
        },
    )?;

    let cycle = power_cycle_gnss(&power, 1).await?;
    cycle.map_err(|_| "cycle refused")?;

    let (updates, mut results) = set_target(&power, 2).latest();
    let first = results.next().await.ok_or("latest stream ended")?;
    first?;

    println!("ready");
    io::stdout().flush()?;

    io::stdin().read_line(&mut String::new())?;
    if !matches!(results.next().await, Some(Err(_))) || results.next().await.is_some() {
        return Err("the lost owner did not end the latest stream with a delivery error".into());
    }
    drop(updates);

    let (updates, mut results) = set_target(&power, 3)
        .retry(AktorRetryOptions {
            attempts: core::num::NonZeroUsize::new(50).unwrap(),
            timeout: std::time::Duration::from_secs(1),
        })
        .latest();
    println!("updating");
    io::stdout().flush()?;

    io::stdin().read_line(&mut String::new())?;
    updates.send(4);
    println!("updated");
    io::stdout().flush()?;
    drop(updates);

    let last = results.next().await.ok_or("latest stream ended")?;
    let target = last?;

    if target != 4 || results.next().await.is_some() {
        return Err("latest session did not drain its final revision".into());
    }

    let cycle = power_cycle_gnss(&power, 1).await?;
    cycle.map_err(|_| "cycle refused")?;

    let state = get_state(&power).await?;

    if state.cycles != 1 || state.target != 4 {
        return Err("replacement service did not start with fresh state".into());
    }

    println!("reconnected to replacement owner");
    Ok(())
}
