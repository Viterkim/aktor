use super::*;
use tokio::time::{sleep, timeout};

#[derive(serde::Deserialize)]
struct Counted {
    value: u32,
    #[serde(skip)]
    count: Arc<std::sync::atomic::AtomicUsize>,
}

impl serde::Serialize for Counted {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut value = serializer.serialize_struct("Counted", 1)?;
        value.serialize_field("value", &self.value)?;
        value.end()
    }
}

#[aktor(remote, serde)]
async fn counted(_: &State, value: Counted) -> u32 {
    value.value
}

#[tokio::test]
async fn encoding() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = Client::<State>::new(&socket);
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (updates, mut results) = counted(
        &client,
        Counted {
            value: 1,
            count: count.clone(),
        },
    )
    .latest();
    for value in 2..=1000 {
        updates.send(Counted {
            value,
            count: count.clone(),
        });
    }
    results.next().await.unwrap().unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);

    drop((updates, results));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn queued() {
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
    let client = connect::<State>(&socket).await.unwrap();
    let (updates, mut results) = target(&client, 2).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    let cloned = updates.clone();
    cloned.send(3);
    updates.send(4);
    timeout(Duration::from_secs(2), results.seen(3))
        .await
        .unwrap();
    drop((updates, cloned));

    release.notify_one();
    running.await;
    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 4);
    assert!(results.next().await.is_none());
    assert_eq!(history(&actor).await, [4]);

    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn running() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = connect::<State>(&socket).await.unwrap();
    let (updates, mut results) = target(&client, 1).latest();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    for value in 2..=1000 {
        updates.send(value);
    }
    timeout(Duration::from_secs(2), results.seen(1000))
        .await
        .unwrap();
    release.notify_one();

    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 1000);
    assert_eq!(history(&actor).await, [1, 1000]);
    updates.send(0);
    assert_eq!(results.next().await.unwrap().unwrap(), Err(Rejected(17)));

    drop((updates, results));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn independent() {
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
    let client = connect::<State>(&socket).await.unwrap();
    let (first, mut a) = target(&client, 2).latest();
    let (second, mut b) = target(&client, 3).latest();
    timeout(Duration::from_secs(2), a.seen(1)).await.unwrap();
    timeout(Duration::from_secs(2), b.seen(1)).await.unwrap();
    first.send(4);
    timeout(Duration::from_secs(2), a.seen(2)).await.unwrap();

    release.notify_one();
    running.await;
    assert_eq!(
        (
            a.next().await.unwrap().unwrap().unwrap(),
            b.next().await.unwrap().unwrap().unwrap()
        ),
        (4, 3)
    );

    drop((first, second, a, b));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn receiver() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let _unblock = Release(release.clone());
    let mut running = Box::pin(slow(&actor));
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen_with(
        &socket,
        &actor,
        ServerOptions {
            max_connections: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let client = connect::<State>(&socket).await.unwrap();
    let (updates, results) = target(&client, 9).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    drop(results);
    timeout(Duration::from_secs(2), server.cancelled.notified())
        .await
        .unwrap();

    let next = timeout(Duration::from_secs(2), connect::<State>(&socket))
        .await
        .unwrap()
        .unwrap();
    let reply = timeout(Duration::from_secs(2), read(&next).send())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    running.await;
    reply.await.unwrap();
    assert!(history(&actor).await.is_empty());

    drop(updates);
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn budget() {
    let (actors, actor) = actor().await;
    let (_, release) = gates(&actor).await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let client = Client::<State>::connect_with(
        &socket,
        ClientOptions {
            timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (updates, mut results) = target(&client, 2).latest();
    results.next().await.unwrap().unwrap().unwrap();
    sleep(Duration::from_millis(200)).await;
    updates.send(3);
    results.next().await.unwrap().unwrap().unwrap();
    updates.send(1);
    let error = timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Timeout, CallError::OutcomeUnknown)
    );
    release.notify_one();

    drop((updates, results));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn generations() {
    let directory = tempfile::tempdir().unwrap();
    for address in [
        Address::Unix(directory.path().join("power.sock")),
        Address::Tcp("127.0.0.1:0".parse().unwrap()),
        #[cfg(feature = "udp")]
        Address::Udp("127.0.0.1:0".parse().unwrap()),
    ] {
        generation(address).await;
    }
}

async fn generation(address: Address) {
    use aktor::dispatch::DataCodec;
    use aktor::worker::Codec;
    use wire::Message;

    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let _unblock = Release(release.clone());
    let server = Server::listen_with(
        address,
        &actor,
        ServerOptions {
            max_connections: 1,
            ..ServerOptions::default()
        },
    )
    .await
    .unwrap();
    let client = connect::<State>(server.address.clone()).await.unwrap();
    let mut old = client.inner.take_idle().unwrap().unwrap().stream;

    wire::write(
        &mut old,
        &Message::Latest {
            operation: target::NAME.into(),
            session: None,
            generation: 1,
        },
        wire::MAX_FRAME,
    )
    .await
    .unwrap();
    let Message::LatestReady { session } = wire::read(&mut old, wire::MAX_FRAME).await.unwrap()
    else {
        panic!("missing latest acknowledgement")
    };
    for revision in 1..=2 {
        wire::write(
            &mut old,
            &Message::Update {
                session,
                revision,
                input: <DataCodec as Codec<(u32,)>>::encode(&(revision as u32,)).unwrap(),
            },
            wire::MAX_FRAME,
        )
        .await
        .unwrap();
        let _ = timeout(
            Duration::from_secs(2),
            wire::read(&mut old, wire::MAX_FRAME),
        )
        .await
        .unwrap()
        .unwrap();
        if revision == 1 {
            timeout(Duration::from_secs(2), entered.notified())
                .await
                .unwrap();
        }
    }

    let mut replacement = timeout(Duration::from_secs(2), client.inner.open())
        .await
        .unwrap()
        .unwrap()
        .stream;
    wire::write(
        &mut replacement,
        &Message::Latest {
            operation: target::NAME.into(),
            session: Some(session),
            generation: 2,
        },
        wire::MAX_FRAME,
    )
    .await
    .unwrap();
    let ready = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(ready, Message::LatestReady { session: ready } if ready == session));
    wire::write(
        &mut replacement,
        &Message::Update {
            session,
            revision: 3,
            input: <DataCodec as Codec<(u32,)>>::encode(&(3,)).unwrap(),
        },
        wire::MAX_FRAME,
    )
    .await
    .unwrap();
    let _ = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .unwrap();
    release.notify_one();
    let _ = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(history(&actor).await, [1, 3]);

    drop((old, replacement));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn lost() {
    for retry in [true, false] {
        let (actors, actor) = actor().await;
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("power.sock");
        let proxy = directory.path().join("proxy.sock");
        let server = listen(&socket, &actor).await.unwrap();
        let forwarding = lose_reply(&proxy, &socket).await;
        let client = connect::<State>(&proxy).await.unwrap();

        if retry {
            let (_updates, mut results) = increment(&client, 1)
                .retry(AktorRetryOpts {
                    attempts: 2,
                    timeout: Duration::from_secs(2),
                })
                .latest();
            assert_eq!(
                timeout(Duration::from_secs(2), results.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                2
            );
        } else {
            let (_updates, mut results) = increment(&client, 1).latest();
            let error = timeout(Duration::from_secs(2), results.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.outcome, CallError::OutcomeUnknown);
            assert_eq!(read(&actor).await.0, 1);
        }

        forwarding.abort();
        let _ = forwarding.await;
        server.shutdown().await.unwrap();
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn uncertain_retry() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let forwarding = lose_reply(&proxy, &socket).await;
    let client = Client::<State>::with_options(
        &proxy,
        ClientOptions {
            retry_delay: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .unwrap();
    let (_updates, mut results) = echo(&client, vec![0; 1024])
        .retry(AktorRetryOpts {
            attempts: 2,
            timeout: Duration::from_secs(2),
        })
        .latest();
    timeout(Duration::from_secs(2), async {
        while read(&actor).await.0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();
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

    let error = timeout(Duration::from_secs(3), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Codec, CallError::OutcomeUnknown)
    );

    forwarding.abort();
    let _ = forwarding.await;
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn backoff() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let client = Client::<State>::new(&socket);
    let (updates, mut results) = read(&client)
        .retry(AktorRetryOpts {
            attempts: 2,
            timeout: Duration::from_millis(50),
        })
        .latest();
    let clone = updates.clone();
    sleep(Duration::from_millis(20)).await;
    drop(clone);
    sleep(Duration::from_millis(60)).await;
    let server = listen(&socket, &actor).await.unwrap();

    timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    drop((updates, results));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn obsolete() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let target_socket = socket.clone();
    let held = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let captured = held.clone();
    let resumed = release.clone();
    let forwarding = tokio::spawn(async move {
        let (client, backend) = proxy_pair(&listener, &target_socket).await;
        let (mut incoming, mut replies) = client.into_split();
        let (mut answers, mut requests) = backend.into_split();
        let reading = async {
            tokio::io::copy(&mut incoming, &mut requests).await.unwrap();
        };
        let writing = async {
            let mut first = true;
            loop {
                let answer = wire::read(&mut answers, wire::MAX_FRAME).await.unwrap();
                if first && matches!(answer, wire::Message::LatestAnswer { .. }) {
                    first = false;
                    captured.notify_one();
                    resumed.notified().await;
                }
                wire::write(&mut replies, &answer, wire::MAX_FRAME)
                    .await
                    .unwrap();
            }
        };
        tokio::join!(reading, writing);
    });
    let client = connect::<State>(&proxy).await.unwrap();
    let (updates, mut results) = target(&client, 7).latest();
    timeout(Duration::from_secs(2), held.notified())
        .await
        .unwrap();
    updates.send(8);
    release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(2), results.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap(),
        8
    );

    updates.send(9);
    timeout(Duration::from_secs(2), async {
        while !results.buffered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    updates.send(10);
    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 10);

    drop((updates, results));
    forwarding.abort();
    let _ = forwarding.await;
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn actor_shutdown() {
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
    let client = connect::<State>(&socket).await.unwrap();
    let (_updates, mut results) = target(&client, 9).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();

    let closing = actors.shutdown();
    let handle = actor.new_handle();
    timeout(Duration::from_secs(2), handle.wait_closing())
        .await
        .unwrap();
    release.notify_one();
    running.await;

    let (_, output) = timeout(Duration::from_secs(2), async {
        tokio::join!(async { closing.await }, results.next())
    })
    .await
    .unwrap();
    assert_eq!(output.unwrap().unwrap().unwrap(), 9);
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn shutdown() {
    for overdue in [false, true] {
        let (actors, actor) = actor().await;
        let (entered, release) = gates(&actor).await;
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("power.sock");
        let server = Server::listen_with(
            &socket,
            &actor,
            ServerOptions {
                shutdown_timeout: if overdue {
                    Duration::from_millis(30)
                } else {
                    Duration::from_secs(1)
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let client = connect::<State>(&socket).await.unwrap();
        let (updates, mut results) = slow(&client).latest();
        timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let closing = server.shutdown();
        tokio::pin!(closing);

        if overdue {
            assert_eq!(closing.await.unwrap_err().cause, Cause::Timeout);
            assert_eq!(
                results.next().await.unwrap().unwrap_err().outcome,
                CallError::OutcomeUnknown
            );
            release.notify_one();
        } else {
            tokio::select! {
                _ = sleep(Duration::from_millis(20)) => release.notify_one(),
                _ = &mut closing => panic!("listener finished before accepted work"),
            }
            closing.await.unwrap();
            assert_eq!(results.next().await.unwrap().unwrap(), 999);
        }

        drop((updates, results));
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn runtime() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = listen(&socket, &actor).await.unwrap();
    let (updates, mut results) = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let client = connect::<State>(&socket).await.unwrap();
            target(&client, 2).latest()
        })
    })
    .await
    .unwrap();

    let error = timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.cause, Cause::Closed);

    drop(updates);
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn rejected() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await;
    let _unblock = Release(release.clone());
    let mut running = Box::pin(slow(&actor));
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen_with(
        &socket,
        &actor,
        ServerOptions {
            max_connections: 1,
            max_frame: 4096,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let client = connect::<State>(&socket).await.unwrap();
    let (updates, mut results) = echo(&client, vec![1]).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    let overflow = connect::<State>(&socket).await.unwrap();
    let error = increment(&overflow, 100).await.unwrap_err();
    assert_eq!(
        (error.cause, error.outcome),
        (Cause::Closed, CallError::NotAdmitted)
    );
    updates.send(vec![2]);
    timeout(Duration::from_secs(2), results.seen(2))
        .await
        .unwrap();

    updates.send(vec![0; 8192]);
    assert_eq!(
        results.next().await.unwrap().unwrap_err().cause,
        Cause::Codec
    );

    timeout(Duration::from_secs(2), server.cancelled.notified())
        .await
        .unwrap();
    let next = timeout(Duration::from_secs(2), connect::<State>(&socket))
        .await
        .unwrap()
        .unwrap();
    let reply = timeout(Duration::from_secs(2), read(&next).send())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    running.await;
    assert_eq!(reply.await.unwrap().0, 1);

    drop((updates, results));
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}
