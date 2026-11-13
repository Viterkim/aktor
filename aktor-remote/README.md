# Aktor remote

Keep the resource in one service and let other processes call its functions. `aktor-remote` runs on Unix with Tokio. It connects over Unix sockets, TCP or uTP over UDP.

```toml
[dependencies]
aktor = { version = "0.0.13", features = ["tokio"] }
aktor-remote = "0.0.13"
er = "0.9"
```

## Shared functions

Put the functions in a crate that both the server and client use.

```rust
#[aktor(remote)]
pub async fn set_target(power: &mut PowerManager, target: i32) -> i32 {
    power.target = target;
    target
}
```

Derive `AktorData` on your own argument and reply types. Plain `#[aktor]` helpers stay local.

## Server

Start the actor in its group and pass the handle to the listener. With `use er::*;`, `.er(())?` keeps the error and report when either step fails.

```rust
let power = group.spawn(power_setup).await.er(())?;
let server = Server::listen_with("/run/power.sock", &power, ServerOptions {
    build: "power-v1".into(),
    ..Default::default()
}).await.er(())?;
```

Every client queues work on the same actor. Call `server.shutdown().await.er(())?` to stop the listener from taking new work before you close the actor's group.

## Client

```rust
let power = Client::<PowerManager>::with_options("/run/power.sock", ClientOptions {
    build: "power-v1".into(),
    ..Default::default()
}).er(())?;

let target = set_target(&power, 10).await.er(())?;
```

A failed delivery gives you `(Error, String)`. Use `.er(())?` to keep the error and report. A successful delivery gives you whatever the function returned, including its own `Result`.

By default, a call has five seconds for one attempt, including time to connect. Allow retries only for commands you can safely run again.

```rust
let target = set_target(&power, 10)
    .retry(AktorRetryOpts {
        attempts: 4,
        timeout: Duration::from_secs(5),
    })
    .await
    .er(())?;
```

That's four attempts including the first call. A lost reply can mean the work already ran, so a retry can run it again. The function's own errors don't trigger retries.

## TCP / UDP

For TCP, pass `Address::Tcp("127.0.0.1:4700".parse()?)` on both sides. `Address::Udp` uses reliable, ordered uTP and needs the `udp` feature. TCP and uTP connections have no TLS or authentication. Unix sockets default to `0600` permissions.

When you change what goes over the wire, change the build string on both sides. The handshake checks names and types, so it can catch a missing function. Reordering fields can change the bytes without changing what the handshake sees.
