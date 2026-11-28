#[path = "../../../cli.rs"]
mod cli;

use aktor_remote as remote;
use power_api::{PowerManager, PowerRemote, WIRE_REVISION, get_state, power_cycle_gnss};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    cli::exit(run().await)
}

async fn run() -> Result<(), cli::Report> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/aktor-power.sock".into());

    let power = remote::Client::<PowerManager, PowerRemote>::new(
        remote::Address::Unix(path.into()),
        remote::ClientOptions {
            build: WIRE_REVISION.into(),
            timeout: std::time::Duration::from_secs(5),
            retry_delay: std::time::Duration::from_millis(100),
            max_frame: 4 * 1024 * 1024,
        },
    )?;

    let before = get_state(&power).await?;
    let cycled = power_cycle_gnss(&power, 1).await?;
    let after = get_state(&power).await?;

    println!("before: {before:?}, command: {cycled:?}, after: {after:?}");
    Ok(())
}
