use super::*;
use aktor::{Aktor, AktorArgs, AktorData, AktorGroup, aktor};
use std::{
    cell::Cell,
    future::{Future, poll_fn},
    path::Path,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Notify,
    task::JoinHandle,
};

mod framing;
mod latest;
#[cfg(feature = "udp")]
mod udp;

struct State {
    value: Rc<Cell<u32>>,
    reads: u32,
    calls: Vec<u32>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct Release(Arc<Notify>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(AktorData, Debug, PartialEq)]
struct Rejected(u32);

#[aktor(remote)]
async fn read(state: &mut State) -> (u32, u32) {
    state.reads += 1;
    (state.value.get(), state.reads)
}

#[aktor(remote, data)]
async fn increment(state: &mut State, amount: u32) -> Result<u32, Rejected> {
    if amount == 0 {
        return Err(Rejected(17));
    }

    state.value.set(state.value.get() + amount);
    Ok(state.value.get())
}

#[aktor]
async fn gates(state: &State) -> (Arc<Notify>, Arc<Notify>) {
    (state.entered.clone(), state.release.clone())
}

#[aktor(remote)]
async fn crash(_: &mut State) {
    panic!("remote owner failure");
}

#[aktor(remote)]
async fn clear(state: &mut State) {
    state.value.set(0);
}

#[aktor(remote, data)]
async fn slow(state: &mut State) -> u32 {
    state.value.set(state.value.get() + 1);
    state.entered.notify_one();
    state.release.notified().await;
    999
}

#[aktor(remote)]
async fn slow_unit(state: &mut State) {
    slow(state).await;
}

#[aktor(remote)]
async fn large(_: &State) -> Vec<u8> {
    vec![0; 8192]
}

#[aktor(remote)]
async fn echo(state: &mut State, bytes: Vec<u8>) -> Vec<u8> {
    state.value.set(state.value.get() + 1);
    bytes
}

#[aktor(remote)]
async fn target(state: &mut State, value: u32) -> Result<u32, Rejected> {
    state.calls.push(value);
    if value == 1 {
        state.entered.notify_one();
        state.release.notified().await;
    }

    if value == 0 {
        Err(Rejected(17))
    } else {
        Ok(value)
    }
}

#[aktor]
async fn history(state: &State) -> Vec<u32> {
    state.calls.clone()
}

async fn actor() -> (AktorGroup, Aktor<State>) {
    let mut actors = AktorGroup::new();
    let _finished = actors.start_with(|_| {}).unwrap();
    let mut args = AktorArgs::new(
        "power",
        || {
            Ok(State {
                value: Rc::new(Cell::new(0)),
                reads: 0,
                calls: Vec::new(),
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            })
        },
        |_| {},
    );
    args.capacity = 1;
    let actor = actors.spawn(args).await.unwrap();

    (actors, actor)
}

#[tokio::test]
async fn network() {
    for udp in [false, true] {
        #[cfg(not(feature = "udp"))]
        if udp {
            continue;
        }

        let (actors, actor) = actor().await;
        let address = "127.0.0.1:0".parse().unwrap();
        let address = if udp {
            #[cfg(feature = "udp")]
            {
                Address::Udp(address)
            }
            #[cfg(not(feature = "udp"))]
            {
                continue;
            }
        } else {
            Address::Tcp(address)
        };
        let server = Server::listen_with(address, &actor, ServerOptions::default())
            .await
            .unwrap();
        let client = Client::<State>::new(server.address.clone());
        assert_eq!(
            increment(&client, 7)
                .send()
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap(),
            7
        );
        let (updates, mut results) = target(&client, 8).latest();
        drop(updates);
        assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 8);

        server.shutdown().await.unwrap();
        actors.shutdown().await;
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn preset() {
    use aktor::testing::Preset;

    let successful = || Preset::new().on(increment::handler(async |amount| Ok(amount)));
    let mut actors = AktorGroup::new();
    let _completion = actors.start_with(|_| {}).unwrap();
    let first = successful().start(&mut actors).await.unwrap();
    let second = successful()
        .on(increment::handler(async |_| Err(Rejected(88))))
        .start(&mut actors)
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let a = directory.path().join("a.sock");
    let b = directory.path().join("b.sock");
    let first = Server::listen_mock(&a, &first, ServerOptions::default())
        .await
        .unwrap();
    let second = Server::listen_mock(&b, &second, ServerOptions::default())
        .await
        .unwrap();
    let a = Client::<State>::new(a);
    let b = Client::<State>::new(b);

    assert_eq!(increment(&a, 7).await.unwrap(), Ok(7));
    assert_eq!(increment(&b, 7).await.unwrap(), Err(Rejected(88)));
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn unavailable() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let client = Client::<State>::new(&socket);
    let error = read(&client).await.unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Connection, CallError::NotAdmitted)
    );

    let server = listen(&socket, &actor).await.unwrap();
    assert_eq!(read(&client).await.unwrap().1, 1);
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn admission() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let mut running = Box::pin(slow(&actor));
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = Client::<State>::new(&socket);
    let sent = tokio::time::timeout(Duration::from_secs(2), increment(&client, 10).send()).await;
    release.notify_one();
    running.await;
    let sent = sent.unwrap().unwrap();
    tokio::time::timeout(Duration::from_millis(500), read(&client))
        .await
        .expect("unread reply blocked the next call")
        .unwrap();
    assert_eq!(sent.await.unwrap().unwrap(), 11);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn conversion() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let server = listen(directory.path().join("power.sock"), &actor)
        .await
        .unwrap();
    let client = Client::<State>::new(server.address.clone());

    let unblock = Release(release.clone());
    let running = slow(&actor).send().await;
    entered.notified().await;
    let queued = increment(&actor, 10).send().await;
    let mut call = increment(&client, 100);
    assert!(poll_fn(|cx| Poll::Ready(Pin::new(&mut call).poll(cx).is_pending())).await);
    let mut admission = Box::pin(call.send());
    let waiting = tokio::time::timeout(Duration::from_millis(30), &mut admission).await;
    drop(unblock);
    running.await;
    queued.await.unwrap();
    assert!(waiting.is_err());
    let reply = admission.await.unwrap();
    assert_eq!(reply.await.unwrap(), Ok(111));

    let unblock = Release(release.clone());
    let mut call = slow(&client);
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow call finished early: {result:?}"),
    }
    let reply = call.send().await.unwrap();
    drop(unblock);
    assert_eq!(reply.await.unwrap(), 999);

    let unblock = Release(release.clone());
    let mut call = slow_unit(&client);
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow cast finished early: {result:?}"),
    }
    call.cast().await.unwrap();
    drop(unblock);
    assert_eq!(read(&actor).await.0, 113);

    let unblock = Release(release);
    let mut call = slow(&client);
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow call finished early: {result:?}"),
    }
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        call.timeout(Duration::from_millis(10)),
    )
    .await
    .unwrap()
    .unwrap_err();
    drop(unblock);
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Timeout, CallError::OutcomeUnknown)
    );

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn owner_failure() {
    for latest in [false, true] {
        let (actors, actor) = actor().await;
        let directory = tempfile::tempdir().unwrap();
        let server = listen(directory.path().join("power.sock"), &actor)
            .await
            .unwrap();
        let client = Client::<State>::new(server.address.clone());
        let error = if latest {
            let (_updates, mut results) = crash(&client).latest();
            tokio::time::timeout(Duration::from_secs(1), results.next())
                .await
                .expect("latest waited for the listener deadline after owner failure")
                .unwrap()
                .unwrap_err()
        } else {
            tokio::time::timeout(Duration::from_secs(1), crash(&client))
                .await
                .expect("call waited for the listener deadline after owner failure")
                .unwrap_err()
        };
        assert_eq!(error.cause, Cause::Closed);

        let _ = server.shutdown().await;
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn roundtrip() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = connect::<State>(&socket).await.unwrap();

    assert_eq!(increment(&client, 0).await.unwrap(), Err(Rejected(17)));
    increment(&actor, 2).await.unwrap();
    increment(&client.clone(), 4).await.unwrap().unwrap();
    drop(client);

    let client = connect::<State>(&socket).await.unwrap();
    assert_eq!(read(&client).await.unwrap().0, 6);
    clear(&client).cast().await.unwrap();
    assert_eq!(read(&client).await.unwrap().0, 0);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

async fn proxy_pair(listener: &UnixListener, target: &Path) -> (UnixStream, UnixStream) {
    let (mut client, _) = listener.accept().await.unwrap();
    let mut backend = UnixStream::connect(target).await.unwrap();
    let hello = wire::read(&mut client, wire::MAX_FRAME).await.unwrap();
    wire::write(&mut backend, &hello, wire::MAX_FRAME)
        .await
        .unwrap();
    let ready = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
    wire::write(&mut client, &ready, wire::MAX_FRAME)
        .await
        .unwrap();
    (client, backend)
}

async fn lose_reply(path: &Path, target: &Path) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    let target = target.to_owned();

    tokio::spawn(async move {
        let (mut client, mut backend) = proxy_pair(&listener, &target).await;
        let request = wire::read(&mut client, wire::MAX_FRAME).await.unwrap();
        wire::write(&mut backend, &request, wire::MAX_FRAME)
            .await
            .unwrap();
        if matches!(request, wire::Message::Latest { .. }) {
            let ready = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
            wire::write(&mut client, &ready, wire::MAX_FRAME)
                .await
                .unwrap();
            let update = wire::read(&mut client, wire::MAX_FRAME).await.unwrap();
            wire::write(&mut backend, &update, wire::MAX_FRAME)
                .await
                .unwrap();
            loop {
                let response = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
                if matches!(response, wire::Message::LatestAnswer { .. }) {
                    break;
                }
                wire::write(&mut client, &response, wire::MAX_FRAME)
                    .await
                    .unwrap();
            }
        } else {
            loop {
                let answer = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
                if matches!(answer, wire::Message::Answer { .. }) {
                    break;
                }
                wire::write(&mut client, &answer, wire::MAX_FRAME)
                    .await
                    .unwrap();
            }
        }
        drop(client);
        drop(backend);

        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(target).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
    })
}

#[tokio::test]
async fn lost_reply() {
    for retry in [true, false] {
        let (actors, actor) = actor().await;
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("power.sock");
        let proxy = directory.path().join("proxy.sock");
        let server = listen(&socket, &actor).await.unwrap();
        let forwarding = lose_reply(&proxy, &socket).await;
        let client = connect::<State>(&proxy).await.unwrap();

        if retry {
            assert_eq!(
                increment(&client, 1)
                    .retry(AktorRetryOpts {
                        attempts: 2,
                        timeout: Duration::from_secs(2)
                    })
                    .await
                    .unwrap()
                    .unwrap(),
                2
            );
        } else {
            let error = increment(&client, 1).await.unwrap_err();
            assert_eq!(error.outcome, CallError::OutcomeUnknown);
            assert_eq!(read(&actor).await.0, 1);
        }

        forwarding.abort();
        let _ = forwarding.await;
        let _ = std::fs::remove_file(proxy);
        server.shutdown().await.unwrap();
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn retry_limit() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let (held, received) = tokio::sync::oneshot::channel();
    let forwarding = tokio::spawn(async move {
        let (mut client, mut backend) = proxy_pair(&listener, &socket).await;
        let request = wire::read(&mut client, wire::MAX_FRAME).await.unwrap();
        wire::write(&mut backend, &request, wire::MAX_FRAME)
            .await
            .unwrap();

        loop {
            let answer = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
            if matches!(answer, wire::Message::Answer { .. }) {
                held.send(()).unwrap();
                core::future::pending::<()>().await;
                return;
            }

            wire::write(&mut client, &answer, wire::MAX_FRAME)
                .await
                .unwrap();
        }
    });
    let delay = Duration::from_secs(5);
    let client = Client::<State>::with_options(
        &proxy,
        ClientOptions {
            retry_delay: delay,
            ..Default::default()
        },
    )
    .unwrap();
    let options = AktorRetryOpts {
        attempts: 2,
        timeout: Duration::from_secs(1),
    };
    let mut request = increment(&client, 1).retry(options).into_request();
    tokio::select! {
        result = received => result.unwrap(),
        output = &mut request => panic!("withheld reply arrived: {output:?}"),
    }

    tokio::time::pause();
    tokio::time::advance(options.timeout).await;
    // Process the expired timer before polling into backoff.
    tokio::time::sleep(Duration::from_millis(1)).await;
    let backoff = poll_fn(|cx| Poll::Ready(Pin::new(&mut request).poll(cx))).await;
    let mut request = request.retry(AktorRetryOpts {
        attempts: 1,
        ..options
    });
    tokio::time::advance(delay).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let output = poll_fn(|cx| Poll::Ready(Pin::new(&mut request).poll(cx))).await;
    tokio::time::resume();
    forwarding.abort();
    let _ = forwarding.await;
    let value = read(&actor).await.0;
    server.shutdown().await.unwrap();
    actors.shutdown().await;

    assert!(backoff.is_pending());
    let Poll::Ready(output) = output else {
        panic!("retry limit allowed another attempt")
    };
    let error = output.unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Timeout, CallError::OutcomeUnknown)
    );
    assert_eq!(value, 1);
}

#[tokio::test]
async fn interrupted_write() {
    use tokio::io::AsyncReadExt;

    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let target = socket.clone();

    let forwarding = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(&target).await.unwrap();
        let hello = wire::read(&mut client, wire::MAX_FRAME).await.unwrap();
        wire::write(&mut backend, &hello, wire::MAX_FRAME)
            .await
            .unwrap();
        let ready = wire::read(&mut backend, wire::MAX_FRAME).await.unwrap();
        wire::write(&mut client, &ready, wire::MAX_FRAME)
            .await
            .unwrap();

        let mut prefix = [0; 8];
        client.read_exact(&mut prefix).await.unwrap();
        use tokio::io::AsyncWriteExt;
        backend.write_all(&prefix).await.unwrap();
        drop(client);
        drop(backend);

        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(target).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
    });
    let client = connect::<State>(&proxy).await.unwrap();
    let bytes = vec![23; 1024 * 1024];
    let executions = match echo(&client, bytes.clone())
        .retry(AktorRetryOpts {
            attempts: 2,
            timeout: Duration::from_secs(2),
        })
        .await
    {
        Ok(output) => {
            assert_eq!(output, bytes);
            1
        }
        Err(error) => {
            assert_eq!(error.outcome, CallError::OutcomeUnknown);
            0
        }
    };
    assert_eq!(read(&actor).await.0, executions);

    forwarding.abort();
    let _ = forwarding.await;
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn reconnect() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    let server = listen(&socket, &actor).await.unwrap();
    assert!(listen(&socket, &actor).await.is_err());
    let client = connect::<State>(&socket).await.unwrap();
    server.shutdown().await.unwrap();

    let (reply, server) = tokio::join!(
        increment(&client, 1).retry(AktorRetryOpts {
            attempts: 4,
            timeout: Duration::from_secs(1)
        }),
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            listen(&socket, &actor).await.unwrap()
        }
    );
    assert_eq!(reply.unwrap().unwrap(), 1);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn cancellation() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = connect::<State>(&socket).await.unwrap();
    let mut call = Box::pin(slow(&client));

    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut call => panic!("slow call completed before release"),
    }
    drop(call);
    release.notify_one();

    assert_eq!(read(&client).await.unwrap().0, 1);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn deadline() {
    let (actors, actor) = actor().await;
    let (_, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = Client::<State>::new(&socket);
    let error = slow(&client)
        .timeout(Duration::from_millis(50))
        .await
        .unwrap_err();

    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Timeout, CallError::OutcomeUnknown)
    );
    release.notify_one();

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn publication() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let mut errors = server.errors();
    let mut operations = aktor::worker::Operations::for_remote::<State, ()>();
    operations.0.truncate(1);
    let mut stream = UnixStream::connect(&socket).await.unwrap();
    wire::write(
        &mut stream,
        &wire::Message::Hello {
            version: wire::VERSION,
            build: String::new(),
            operations,
            max_frame: wire::MAX_FRAME as u32,
        },
        wire::MAX_HANDSHAKE,
    )
    .await
    .unwrap();
    assert!(matches!(
        wire::read(&mut stream, wire::MAX_FRAME).await.unwrap(),
        wire::Message::Ready { .. }
    ));

    wire::write(
        &mut stream,
        &wire::Message::Call {
            id: 1,
            operation: history::NAME.into(),
            input: Vec::new(),
        },
        wire::MAX_FRAME,
    )
    .await
    .unwrap();
    let wire::Message::Answer {
        output: Err(error), ..
    } = wire::read(&mut stream, wire::MAX_FRAME).await.unwrap()
    else {
        panic!("local helper was remotely callable")
    };
    assert_eq!(error.cause, Cause::Protocol);
    assert!(
        diagnostic(&mut errors, "unpublished operation")
            .await
            .diagnostics
            .contains(history::NAME)
    );

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn compatibility() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let error = Client::<State>::connect_with(
        &socket,
        ClientOptions {
            build: "different layout".into(),
            ..Default::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.cause, Cause::Protocol);

    let mut stream = UnixStream::connect(&socket).await.unwrap();
    use tokio::io::AsyncWriteExt;
    stream.write_u32(u32::MAX).await.unwrap();
    let client = connect::<State>(&socket).await.unwrap();
    assert!(read(&client).await.is_ok());

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn frame_limit() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen_with(
        &socket,
        &actor,
        ServerOptions {
            max_frame: 128,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let client = Client::<State>::connect_with(
        &socket,
        ClientOptions {
            max_frame: 128,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let error = large(&client).await.unwrap_err();

    assert_eq!(error.cause, Cause::Codec);

    let error = echo(&client, vec![0; 8192]).await.unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Codec, CallError::NotAdmitted)
    );
    assert_eq!(read(&actor).await.0, 0);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn pending_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    for address in [
        Address::Unix(directory.path().join("power.sock")),
        Address::Tcp("127.0.0.1:0".parse().unwrap()),
        #[cfg(feature = "udp")]
        Address::Udp("127.0.0.1:0".parse().unwrap()),
    ] {
        let (actors, actor) = actor().await;
        let (entered, release) = gates(&actor).await;
        let mut running = Box::pin(slow(&actor));
        tokio::select! {
            _ = entered.notified() => {},
            _ = &mut running => panic!("slow call completed before release"),
        }
        let queued = increment(&actor, 10).send().await;

        let server = Server::listen_with(
            address,
            &actor,
            ServerOptions {
                max_connections: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let client = Client::<State>::new(server.address.clone());
        let mut pending = Box::pin(increment(&client, 100));
        tokio::select! {
            _ = server.pending.notified() => {},
            _ = &mut pending => panic!("remote request did not wait for admission"),
        }
        drop(pending);
        let cancelled =
            tokio::time::timeout(Duration::from_secs(1), server.cancelled.notified()).await;

        let _next = Client::<State>::connect_with(
            server.address.clone(),
            ClientOptions {
                timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await;

        release.notify_one();
        running.await;
        queued.await.unwrap();
        cancelled.unwrap();
        _next.unwrap();
        assert_eq!(read(&actor).await.0, 11);

        server.shutdown().await.unwrap();
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn listener_stop() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let server = listen(directory.path().join("power.sock"), &actor)
        .await
        .unwrap();
    let client = Client::<State>::new(server.address.clone());
    let reply = slow(&client).send().await.unwrap();
    entered.notified().await;
    drop(reply);

    tokio::time::timeout(Duration::from_secs(1), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    actors.shutdown().await;
}

#[tokio::test]
async fn stopping_admission() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let mut running = Box::pin(slow(&actor));
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let queued = increment(&actor, 10).send().await;
    let directory = tempfile::tempdir().unwrap();
    let server = listen(directory.path().join("power.sock"), &actor)
        .await
        .unwrap();
    let client = Client::<State>::new(server.address.clone());
    let mut pending = Box::pin(increment(&client, 100));
    tokio::select! {
        _ = server.pending.notified() => {},
        _ = &mut pending => panic!("remote call did not wait for queue space"),
    }
    let (stopped, output) = tokio::join!(server.shutdown(), &mut pending);
    stopped.unwrap();
    assert_eq!(output.unwrap_err().outcome, CallError::NotAdmitted);
    release.notify_one();
    running.await;
    queued.await.unwrap();
    assert_eq!(read(&actor).await.0, 11);
    actors.shutdown().await;
}

async fn raw_client(path: &Path) -> UnixStream {
    let mut stream = UnixStream::connect(path).await.unwrap();
    wire::write(
        &mut stream,
        &wire::Message::Hello {
            version: wire::VERSION,
            build: String::new(),
            operations: aktor::worker::Operations::for_remote::<State, ()>(),
            max_frame: wire::MAX_FRAME as u32,
        },
        wire::MAX_FRAME,
    )
    .await
    .unwrap();
    let _ = wire::read(&mut stream, wire::MAX_FRAME).await.unwrap();
    stream
}

#[tokio::test]
async fn protocol_deadlines() {
    use tokio::io::AsyncWriteExt;

    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen_with(
        &socket,
        &actor,
        ServerOptions {
            max_connections: 1,
            io_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut errors = server.errors();
    let silent = UnixStream::connect(&socket).await.unwrap();
    assert_eq!(
        diagnostic(&mut errors, "handshake").await.diagnostics,
        "remote handshake timed out"
    );
    drop(silent);

    for prefix in [&[0][..], &[0, 0, 1, 0, 0][..]] {
        let mut partial = raw_client(&socket).await;
        partial.write_all(prefix).await.unwrap();
        assert_eq!(
            diagnostic(&mut errors, "frame").await.diagnostics,
            "remote frame timed out"
        );
    }

    let client = connect::<State>(&socket).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(read(&client).await.is_ok());

    let (mut stalled, _unread) = tokio::io::duplex(32);
    let error = wire::write_reply(&mut stalled, &[0; 1024], Duration::from_millis(50))
        .await
        .unwrap_err();
    assert_eq!(
        (error.outcome, error.diagnostics.as_str()),
        (CallError::OutcomeUnknown, "remote reply write timed out")
    );

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

async fn diagnostic(errors: &mut tokio::sync::broadcast::Receiver<Error>, phase: &str) -> Error {
    tokio::time::timeout(Duration::from_secs(2), errors.recv())
        .await
        .unwrap_or_else(|_| panic!("{phase} did not produce a diagnostic"))
        .unwrap()
}

#[tokio::test]
async fn rejection() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen_with(
        &socket,
        &actor,
        ServerOptions {
            max_frame: 4096,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut errors = server.errors();
    let mut stream = UnixStream::connect(&socket).await.unwrap();
    let incompatible = "incompatible".repeat(250);
    wire::write(
        &mut stream,
        &wire::Message::Hello {
            version: wire::VERSION,
            build: String::new(),
            operations: aktor::worker::Operations(vec![incompatible.clone()]),
            max_frame: 4096,
        },
        4096,
    )
    .await
    .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), wire::read(&mut stream, 4096))
        .await
        .unwrap()
        .unwrap();
    let wire::Message::Reject(error) = response else {
        panic!("missing rejection")
    };
    assert_eq!(error.diagnostics, "remote operation catalogue mismatch");
    assert!(
        diagnostic(&mut errors, "catalogue")
            .await
            .diagnostics
            .contains(&incompatible)
    );
    assert!(wire::validate(wire::MIN_FRAME - 1).is_err());

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn owner_shutdown() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let server = listen(directory.path().join("power.sock"), &actor)
        .await
        .unwrap();
    actors.shutdown().await;

    assert_eq!(server.wait().await.unwrap_err().cause, Cause::Closed);
}
