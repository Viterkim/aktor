use aktor_remote as remote;
use power_api::{PowerManager, PowerRemote, WIRE_REVISION, get_state, power_cycle_gnss};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/aktor-power.sock".into());
    let power = remote::Client::<PowerManager, PowerRemote>::connect_with(
        path,
        remote::ClientOptions {
            build: WIRE_REVISION.into(),
            ..Default::default()
        },
    )
    .await?;
    let before = get_state(&power).await?;
    let cycled = power_cycle_gnss(&power, 1).await?;
    let after = get_state(&power).await?;

    println!("before: {before:?}, command: {cycled:?}, after: {after:?}");
    Ok(())
}
