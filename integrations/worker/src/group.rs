use aktor::{
    AktorCleanupError, AktorClosures, AktorError, AktorGroup, AktorSetupError,
    worker::{self, Options, Server, WorkerCause},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::Duration,
};
use wasm_bindgen::prelude::*;

thread_local! { static SERVER: RefCell<Option<Server>> = const { RefCell::new(None) }; }
thread_local! { static CLOSING: RefCell<Option<worker::Worker<u32>>> = const { RefCell::new(None) }; }

#[wasm_bindgen(inline_js = r#"
export function gate() {
    return globalThis.aktorGate;
}

export function fail_gate() {
    return new Promise(() => {
        globalThis.aktorGate.then(() => {
            setTimeout(() => { throw new Error('secondary worker sentinel'); }, 0);
        });
    });
}

export function release_gate() {
    globalThis.aktorGateRelease();
}

export function cleanup_gate() {
    return globalThis.aktorCleanupGate ? globalThis.aktorGate : Promise.resolve();
}

export function note_cleanup() {
    if (globalThis.aktorCleanupGate !== undefined) postMessage({ proof: 'cleanup' });
}

export function note_configured() {
    postMessage({ proof: 'configured' });
}

export function configured() {
    return globalThis.proofWorkers.at(-1).proofConfigured;
}

let originalPost;
export function break_post() {
    originalPost = Worker.prototype.postMessage;
    Worker.prototype.postMessage = function() {
        throw new Error('admission posting probe');
    };
}

export function restore_post() {
    Worker.prototype.postMessage = originalPost;
}
"#)]
extern "C" {
    fn gate() -> js_sys::Promise;
    fn fail_gate() -> js_sys::Promise;
    fn release_gate();
    fn cleanup_gate() -> js_sys::Promise;
    fn note_cleanup();
    fn note_configured();
    fn configured() -> js_sys::Promise;
    fn break_post();
    fn restore_post();
}

#[derive(Deserialize)]
struct ClosingInput;
impl Serialize for ClosingInput {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let worker = CLOSING.with(|worker| worker.borrow_mut().take()).unwrap();
        drop(worker.shutdown());
        (7u32,).serialize(serializer)
    }
}

#[wasm_bindgen]
pub async fn serialization_shutdown_check(url: String) -> Result<bool, JsValue> {
    use aktor::{dispatch::Transport, operation::Operation};

    use futures_util::FutureExt;
    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .worker::<u32>("serialization shutdown", &url, options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    assert_eq!(worker.outstanding(), (0, 0));
    CLOSING.with(|stored| *stored.borrow_mut() = Some(worker.new_handle()));

    let request =
        <&worker::Worker<u32> as Transport<u32, ClosingInput, Result<u32, String>>>::request(
            &worker,
            Operation {
                name: operation::NAME,
                caller: std::panic::Location::caller(),
            },
            ClosingInput,
        );

    let closed = request.send().now_or_never().is_none();

    assert_eq!(worker.outstanding(), (0, 0));
    let failed = worker
        .completion()
        .wait()
        .await
        .expect_err("refused managed call");
    assert_eq!(failed.outcome, aktor::message::CallError::NotAdmitted);
    assert_eq!(failed.cause, worker::WorkerCause::Closed);
    let report = actors.completion().wait().await;
    assert!(report.failed());
    assert!(!report.timed_out);
    assert!(report.actors.iter().all(|actor| !actor.timed_out));

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .worker::<u32>("posting failure", &url, options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    break_post();
    let committed = operation(&worker, 0).send().now_or_never();
    restore_post();
    drop(committed.expect("posting failure happens after admission"));
    let failed = worker
        .completion()
        .wait()
        .await
        .expect_err("posting failure");
    assert!(matches!(failed.cause, worker::WorkerCause::Codec(_)));
    assert!(actors.completion().wait().await.failed());

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .worker::<u32>("explicit termination", &url, options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    worker.terminate();

    let report = actors.completion().wait().await;
    assert!(report.failed());
    assert!(!report.timed_out);
    assert!(report.actors.iter().all(|actor| !actor.timed_out));

    Ok(closed)
}

#[aktor::aktor]
async fn operation(_: &u32, mode: u32) -> Result<u32, String> {
    match mode {
        0 => Err("ordinary operation error".into()),
        1 => panic!("group worker died"),
        2 => core::future::pending().await,
        3 => {
            gloo_timers::future::TimeoutFuture::new(30).await;
            panic!("crashed during shutdown");
        }
        4 | 5 => {
            wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();

            if mode == 5 {
                panic!("gated worker crashed");
            }

            Ok(mode)
        }
        9 => panic!("unadmitted request executed"),
        10 => {
            wasm_bindgen_futures::JsFuture::from(fail_gate())
                .await
                .unwrap();
            Ok(mode)
        }
        _ => Ok(mode),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Choice {
    Text { text: String },
    Number(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Portable {
    options: Vec<Option<Option<u8>>>,
    map: std::collections::BTreeMap<(u32, String), u128>,
    choice: Vec<Choice>,
    limits: (i128, u128),
    invalid: Option<RejectedInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct RejectedInput;
impl<'de> Deserialize<'de> for RejectedInput {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("input refused"))
    }
}

#[derive(Serialize)]
struct RejectedOutput;
impl<'de> Deserialize<'de> for RejectedOutput {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        REJECTED_DECODES.with(|count| count.set(count.get() + 1));
        Err(serde::de::Error::custom("output refused"))
    }
}

#[aktor::aktor]
async fn bad_output(_: &u32) -> RejectedOutput {
    RejectedOutput
}

struct BrokenData;
impl Serialize for BrokenData {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("cleanup data refused"))
    }
}

#[aktor::aktor]
async fn roundtrip(_: &u32, value: Portable) -> Portable {
    value
}

#[derive(Serialize, Deserialize)]
struct CleanupData {
    code: u32,
    option: Option<Option<u8>>,
}

struct Other;

#[aktor::aktor(actor = Other)]
async fn other_operation(state: &u32) -> u32 {
    *state
}

fn options() -> Options {
    Options {
        build: "group-v1".into(),
        ..Options::default()
    }
}

#[wasm_bindgen]
pub async fn closure_check(
    url: String,
    bytes: bool,
    cleanup: bool,
    crash: bool,
) -> Result<bool, JsValue> {
    use futures_util::FutureExt;

    let mut actors = AktorGroup::with_grace(Duration::from_secs(2));

    actors.start().unwrap();

    let worker = actors
        .worker::<u32>(
            "gated",
            &url,
            Options {
                capacity: if bytes { 2 } else { 1 },
                max_outstanding_bytes: if bytes {
                    1
                } else {
                    Options::default().max_outstanding_bytes
                },
                ..options()
            },
        )
        .await
        .unwrap();

    let mut reply = Some(
        operation(
            &worker,
            if cleanup {
                7
            } else if crash {
                5
            } else {
                4
            },
        )
        .send()
        .await,
    );

    if cleanup {
        assert_eq!(reply.take().unwrap().await, Ok(7));
    } else {
        while !worker.executing() {
            gloo_timers::future::TimeoutFuture::new(0).await;
        }
    }

    let mut blocked = Box::pin(operation(&worker, 9));

    if !cleanup {
        assert!(blocked.as_mut().now_or_never().is_none());
    }

    let closing = worker.shutdown();

    actors.killswitch().stop();
    assert!(blocked.as_mut().now_or_never().is_none());

    let prematurely_finished = worker.completion().wait().now_or_never().is_some();

    drop(blocked);
    release_gate();

    let result = closing.await;

    if !cleanup && !crash {
        assert_eq!(reply.take().unwrap().await, Ok(4));
    }

    drop(reply);

    let report = actors.completion().await;

    if prematurely_finished {
        return Err(JsValue::from_str("blocked admission terminated the worker"));
    }

    assert_eq!(result.is_err(), crash);
    assert_eq!(report.failed(), crash);
    Ok(true)
}

#[wasm_bindgen]
pub fn cancel_startup_check(url: String) -> bool {
    use futures_util::FutureExt;

    let mut opening = Box::pin(worker::Worker::<u32>::open(&url, options()));
    let pending = opening.as_mut().now_or_never().is_none();

    drop(opening);
    pending
}

#[wasm_bindgen]
pub async fn start_unready_worker() -> Result<(), JsValue> {
    let server = worker::serve_setup((), options(), |state: u32| {
        let start = async move || {
            note_configured();
            wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();
            Ok::<_, AktorSetupError>(state)
        };
        AktorClosures {
            start,
            end: (),
            intervals: vec![],
            before_each: None,
            after_each: None,
        }
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub async fn cancel_configured_check(url: String) -> Result<bool, JsValue> {
    use futures_util::FutureExt;

    let worker = worker::Worker::<u32>::with_config(&url, options(), &7u32)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut ready = Box::pin(worker.ready());

    assert!(ready.as_mut().now_or_never().is_none());
    wasm_bindgen_futures::JsFuture::from(configured()).await?;
    assert!(ready.as_mut().now_or_never().is_none());
    drop(ready);
    drop(worker);
    Ok(true)
}

#[wasm_bindgen]
pub async fn live_sessions_check(url: String) -> Result<bool, JsValue> {
    use futures_util::Stream;
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Wake, Waker},
    };
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let worker = worker::Worker::<u32>::open(&url, options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let (_input, mut output) = operation::latest(&worker);

    assert!(
        Pin::new(&mut output)
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let mut sessions = Vec::new();

    for _ in 0..512 {
        sessions.push(operation::latest(&worker));
    }

    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    worker
        .shutdown()
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    Ok(wake.0.load(Ordering::Relaxed) > 0)
}

#[wasm_bindgen]
pub fn start_group_worker(
    fail_cleanup: bool,
    wrong_data: bool,
    fail_data: bool,
) -> Result<(), JsValue> {
    let server = if fail_data {
        worker::serve_with(
            0u32,
            async |_| {
                Err(AktorCleanupError {
                    diagnostics: "backup folder is read only".into(),
                    data: BrokenData,
                })
            },
            options(),
        )
    } else if wrong_data {
        worker::serve_with(
            0u32,
            async |_| {
                Err(AktorCleanupError {
                    diagnostics: "backup folder is read only".into(),
                    data: String::from("unexpected data type"),
                })
            },
            options(),
        )
    } else {
        worker::serve_with(
            0u32,
            async move |_| {
                note_cleanup();
                wasm_bindgen_futures::JsFuture::from(cleanup_gate())
                    .await
                    .unwrap();

                if fail_cleanup {
                    Err(AktorCleanupError {
                        diagnostics: "Flush settings\n  backup folder is read only".into(),
                        data: CleanupData {
                            code: 17,
                            option: Some(None),
                        },
                    })
                } else {
                    Ok(())
                }
            },
            options(),
        )
    }
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub fn start_role_worker() -> Result<(), JsValue> {
    let server = worker::serve_for::<Other, _, _, _, ()>(17u32, async |_| Ok(()), options())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub async fn registration_check(url: String, other: bool) -> Result<String, JsValue> {
    if other {
        let actor = worker::Worker::<u32, Other>::open(&url, options())
            .await
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let output = other_operation(&actor).await;

        actor
            .shutdown()
            .await
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        return serde_json::to_string(&output)
            .map_err(|error| JsValue::from_str(&error.to_string()));
    }

    let actor = worker::Worker::<u32>::with_options(&url, options())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let result = actor.ready().await;

    actor.terminate();
    serde_json::to_string(&result).map_err(|error| JsValue::from_str(&error.to_string()))
}

#[derive(Serialize)]
struct Check {
    failure: Option<String>,
    failure_message: Option<String>,
    diagnostics: Vec<(String, Vec<String>)>,
    actors: usize,
    hook_count: usize,
    ordinary_error: bool,
    timed_out: bool,
    timed_out_actors: Vec<String>,
    typed_cleanup: bool,
    deferred_reply_safe: bool,
}

#[wasm_bindgen]
pub async fn group_check(url: String, mode: u32) -> Result<String, JsValue> {
    let group = AktorGroup::with_grace(Duration::from_millis(300));
    let completed = group.completion();
    let kill = group.killswitch();
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let ordinary_error = Rc::new(Cell::new(false));
    let query_error = ordinary_error.clone();
    let retained = Rc::new(RefCell::new(None));
    let capture = retained.clone();
    let deferred = Rc::new(RefCell::new(None));
    let saved_reply = deferred.clone();
    let _result = group
        .run(
            async move |app| {
                let storage_url = match mode {
                    4 => format!("{url}?fail_cleanup=true"),
                    6 => format!("{url}?wrong_data=true"),
                    7 => format!("{url}?fail_data=true"),
                    _ => url.clone(),
                };

                let storage = app
                    .worker_for_data::<(), u32, CleanupData>("storage", &storage_url, options())
                    .await
                    .unwrap();

                *capture.borrow_mut() = Some(storage.completion());

                let audio = app
                    .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
                    .await
                    .unwrap();

                assert_eq!(other_operation(&audio).await, 17);

                match mode {
                    0 => {
                        let value = Portable {
                            options: vec![None, Some(None), Some(Some(7))],
                            map: std::collections::BTreeMap::from([((7, "key".into()), u128::MAX)]),
                            choice: vec![
                                Choice::Text { text: "cat".into() },
                                Choice::Number(u64::MAX),
                            ],
                            limits: (i128::MIN, u128::MAX),
                            invalid: None,
                        };

                        assert_eq!(roundtrip(&storage, value.clone()).await, value);
                        query_error.set(operation(&storage, 0).await.is_err());
                    }
                    1 => {
                        *saved_reply.borrow_mut() = Some(operation(&storage, 1).send().await);
                        core::future::pending::<()>().await;
                    }
                    2 | 3 => {
                        let reply = operation::request(&storage, mode).send().await;

                        while !storage.executing() {
                            gloo_timers::future::TimeoutFuture::new(1).await;
                        }

                        kill.stop();
                        kill.stop();
                        reply.await.unwrap();
                    }
                    8 => {
                        let mut reply = bad_output(&storage).send().await;

                        let error = storage.shutdown().await.unwrap_err();
                        assert!(matches!(error.cause, WorkerCause::Codec(_)));
                        assert!(kill.is_stopping());
                        assert!(reply.try_take().is_none());
                        core::future::pending::<()>().await;
                    }
                    5 => {
                        let value = Portable {
                            options: Vec::new(),
                            map: Default::default(),
                            choice: Vec::new(),
                            limits: (0, 0),
                            invalid: Some(RejectedInput),
                        };

                        drop(roundtrip(&storage, value).send().await);
                        core::future::pending::<()>().await;
                    }
                    _ => {}
                }

                Ok::<_, AktorError>(())
            },
            move |_| hook_count.set(hook_count.get() + 1),
        )
        .await;

    let report = completed.wait().await;
    assert_eq!(
        report
            .to_string()
            .matches("backup folder is read only")
            .count(),
        usize::from(mode == 4 || mode == 6 || mode == 7)
    );
    let typed_cleanup = if mode == 4 {
        let completion = retained.borrow_mut().take().unwrap();
        let error = completion.wait().await.unwrap_err();

        assert!(!error.to_string().contains("backup folder is read only"));
        assert!(format!("{error:#}").contains("backup folder is read only"));

        error
            .data
            .is_some_and(|data| data.code == 17 && data.option == Some(None))
    } else if mode == 6 || mode == 7 {
        let completion = retained.borrow_mut().take().unwrap();
        let error = completion.wait().await.unwrap_err();
        let diagnostic = format!("{error:#}");

        error.data.is_none()
            && diagnostic.contains("backup folder is read only")
            && diagnostic.contains("Optional lifecycle data")
    } else {
        true
    };

    let deferred_reply_safe = if let Some(mut reply) = deferred.borrow_mut().take() {
        (0..2).all(|_| {
            std::pin::Pin::new(&mut reply)
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        })
    } else {
        true
    };

    serde_json::to_string(&Check {
        failure_message: report
            .failure
            .as_ref()
            .map(|failure| failure.message.clone()),
        failure: report.failure.map(|failure| failure.actor),
        timed_out_actors: report
            .actors
            .iter()
            .filter(|actor| actor.timed_out)
            .map(|actor| actor.actor.clone())
            .collect(),
        actors: report.actors.len(),
        diagnostics: report
            .actors
            .into_iter()
            .map(|actor| {
                (
                    actor.actor,
                    actor
                        .diagnostics
                        .iter()
                        .map(|error| error.diagnostics.clone())
                        .collect(),
                )
            })
            .collect(),
        hook_count: hooks.get(),
        typed_cleanup,
        deferred_reply_safe,
        ordinary_error: ordinary_error.get(),
        timed_out: report.timed_out,
    })
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub async fn group_listener_check(url: String) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::with_grace(Duration::from_millis(300));
    let kill = actors.killswitch();
    let completed = actors.completion();
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook_count.set(hook_count.get() + 1);
            Ok::<_, AktorError>(())
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    let _closing = closing;

    let storage = actors
        .worker::<u32>(
            "storage",
            &url,
            Options {
                capacity: 1,
                ..options()
            },
        )
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    let audio = actors
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut reply = operation(&storage, 0).send().await;
    let ordinary_error = loop {
        if let Some(result) = reply.try_take() {
            break result.is_err();
        }

        gloo_timers::future::TimeoutFuture::new(1).await;
    };

    assert!(reply.try_take().is_none());

    let mut pending = operation(&storage, 0).send().await;

    assert!(pending.try_take().is_none());

    let wake = std::sync::Arc::new(ReplyWake {
        changed: tokio::sync::Notify::new(),
    });
    let waker = std::task::Waker::from(wake.clone());

    assert!(
        std::pin::Pin::new(&mut pending)
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    assert!(pending.try_take().is_none());
    assert!(
        matches!(
            futures_util::future::select(
                Box::pin(wake.changed.notified()),
                Box::pin(gloo_timers::future::TimeoutFuture::new(2000)),
            )
            .await,
            futures_util::future::Either::Left(_)
        ),
        "try_take replaced the worker reply waker"
    );
    assert!(pending.try_take().unwrap().is_err());
    assert!(pending.try_take().is_none());
    assert!(!kill.is_stopping());
    assert_eq!(other_operation(&audio).await, 17);

    let occupied = operation(&storage, 2).send().await;
    let mut sending = Box::pin(operation(&storage, 0).send());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    assert!(sending.as_mut().poll(&mut context).is_pending());
    storage.terminate();

    for _ in 0..4 {
        assert!(sending.as_mut().poll(&mut context).is_pending());
    }

    drop(sending);
    drop(occupied);

    kill.wait_stopping().await;

    let report = (&completed).await;

    assert_eq!(completed.await.actors.len(), report.actors.len());

    let mut plain = AktorGroup::new();
    let closing = plain
        .start()
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let _audio = plain
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let shutdown = plain.shutdown();

    assert!(plain.killswitch().is_stopping());
    assert!(!(&closing).await.failed());
    assert_eq!(closing.await.actors.len(), shutdown.await.actors.len());

    let cancelled = AktorGroup::new();
    let observed = cancelled.completion();
    let ready = Rc::new(Cell::new(false));
    let started = ready.clone();
    let driver = cancelled.run(
        async |group| -> Result<(), AktorError> {
            let _worker = group
                .worker::<u32>("cancelled driver", &url, options())
                .await
                .unwrap();

            started.set(true);
            core::future::pending().await
        },
        |_| {},
    );

    let mut driver = Box::pin(driver);

    futures_util::future::poll_fn(|context| {
        assert!(driver.as_mut().poll(context).is_pending());

        if ready.get() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    drop(driver);

    let cancelled = observed.wait().await;

    assert_eq!(cancelled.actors.len(), 1);
    assert!(!cancelled.timed_out);
    assert!(
        cancelled
            .application
            .iter()
            .any(|error| error.diagnostics.contains("cancelled"))
    );

    Ok(ordinary_error
        && hooks.get() == 1
        && report.actors.len() == 2
        && report
            .failure
            .as_ref()
            .is_some_and(|failure| failure.actor == "storage")
        && report
            .actors
            .iter()
            .any(|actor| actor.actor == "audio" && !actor.timed_out))
}

#[wasm_bindgen]
pub async fn group_startup_check(url: String, mode: u32) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::with_grace(Duration::from_millis(500));
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook_count.set(hook_count.get() + 1);
            Ok::<_, AktorError>(())
        })
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    let audio = actors
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut storage_options = options();

    if mode == 0 {
        storage_options.capacity = 0;
    } else if mode == 2 {
        storage_options.build = "wrong build".into();
    }

    let storage_url = if mode == 1 {
        format!("{url}?fail_setup")
    } else {
        url
    };

    let failed = actors
        .worker_for_data::<(), u32, CleanupData>("storage", &storage_url, storage_options)
        .await
        .err()
        .unwrap();

    assert_eq!(hooks.get(), 1);

    let typed_data = mode != 1 || failed.data.is_some_and(|data| data.code == 23);
    let report = closing.wait().await;

    Ok(typed_data
        && report.startup
        && report
            .failure
            .as_ref()
            .is_some_and(|failure| failure.actor == "storage")
        && audio.completion().wait_report().await.is_ok()
        && report
            .actors
            .iter()
            .any(|actor| actor.actor == "audio" && !actor.timed_out))
}

struct Empty;
struct Signature;
#[aktor::aktor(actor=Signature)]
async fn signature(_: &u32, _: u64) -> u32 {
    17
}

#[wasm_bindgen]
pub fn start_registration_worker(signature: bool) -> Result<(), JsValue> {
    let server = if signature {
        worker::serve_for::<Signature, _, _, _, ()>(0u32, async |_| Ok(()), options())
    } else {
        worker::serve_for::<Empty, _, _, _, ()>(0u32, async |_| Ok(()), options())
    }
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub fn typed_setup_failed() -> Result<(), JsValue> {
    worker::setup_failed(aktor::AktorSetupError {
        diagnostics: "could not open storage".into(),
        data: CleanupData {
            code: 23,
            option: Some(None),
        },
    })
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen]
pub async fn typed_setup_check(url: String) -> Result<bool, JsValue> {
    let actor = worker::Worker::<u32, (), CleanupData>::with_options(&url, options())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let error = actor.ready().await.unwrap_err();

    assert!(!error.to_string().contains("could not open storage"));
    assert!(format!("{error:#}").contains("could not open storage"));
    assert!(
        error
            .data
            .is_some_and(|data| data.code == 23 && data.option == Some(None))
    );

    let error = actor.completion().wait().await.unwrap_err();
    assert!(!error.to_string().contains("could not open storage"));
    assert!(format!("{error:#}").contains("could not open storage"));

    Ok(error
        .data
        .is_some_and(|data| data.code == 23 && data.option == Some(None)))
}

struct ReplyWake {
    changed: tokio::sync::Notify,
}
impl std::task::Wake for ReplyWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.changed.notify_one();
    }
}

#[wasm_bindgen]
pub async fn start_group_setup_worker() -> Result<(), JsValue> {
    let server = worker::serve_setup((), options(), |()| AktorClosures {
        start: async || Ok::<_, aktor::AktorSetupError>(0_u32),
        end: (),
        intervals: vec![],
        before_each: None,
        after_each: None,
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

async fn managed_worker(
    group: &mut AktorGroup,
    name: &str,
    url: &str,
    prepared: bool,
) -> worker::Worker<u32> {
    if prepared {
        aktor::aktor_start_in(
            group,
            aktor::AktorWorkerNew {
                name: aktor::AktorName::new(name),
                role: aktor::AktorNoRole,
                kind: aktor::AktorKind::BrowserWebWorker(format!("{url}?configured")),
                config: (),
                options: aktor::AktorWorkerOptions {
                    transport: options(),
                },
            },
        )
        .await
        .unwrap()
    } else {
        group.worker(name, url, options()).await.unwrap()
    }
}

#[wasm_bindgen]
pub async fn worker_failure_check(
    url: String,
    prepared: bool,
    same_worker: bool,
) -> Result<bool, JsValue> {
    use futures_util::FutureExt;

    let mut group = AktorGroup::with_grace(Duration::from_secs(2));
    group.start().unwrap();
    let primary = managed_worker(&mut group, "primary", &url, prepared).await;
    let secondary = if same_worker {
        None
    } else {
        Some(managed_worker(&mut group, "secondary", &url, prepared).await)
    };
    let draining = secondary.as_ref().unwrap_or(&primary);
    let mut reply = operation(draining, 10).send().await;

    while !draining.executing() {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    let rejection = worker::WorkerRequest::<u32, ()>::new(
        &primary,
        operation::NAME,
        Err(worker::WorkerError::new(
            aktor::message::CallError::NotAdmitted,
            worker::WorkerCause::Codec("primary worker sentinel".into()),
        )),
    );
    assert!(rejection.now_or_never().is_none());
    release_gate();

    let report = group.shutdown().await;
    assert!(!report.timed_out, "{report}");
    assert_eq!(report.failure.as_ref().unwrap().actor, "primary");
    assert_eq!(
        report
            .to_string()
            .matches("primary worker sentinel")
            .count(),
        1
    );
    let label = if same_worker { "primary" } else { "secondary" };
    let outcome = report
        .actors
        .iter()
        .find(|actor| actor.actor == label)
        .unwrap();
    assert!(!outcome.timed_out);
    if outcome.diagnostics.len() != 1 {
        return Err(JsValue::from_str(&format!(
            "secondary worker failure was lost: {report}"
        )));
    }
    assert!(
        outcome.diagnostics[0]
            .diagnostics
            .contains("secondary worker sentinel"),
        "{report}"
    );
    assert_eq!(
        report
            .to_string()
            .matches("secondary worker sentinel")
            .count(),
        1
    );
    assert!(reply.try_take().is_none());
    assert!(reply.try_take().is_none());
    assert!(operation(draining, 0).now_or_never().is_none());
    assert_eq!(
        group.completion().wait().await.to_string(),
        report.to_string()
    );
    drop(reply);
    Ok(true)
}

thread_local! {
    static REJECTED_DECODES: Cell<usize> = const { Cell::new(0) };
    static COUNTED_DECODES: Cell<usize> = const { Cell::new(0) };
}

struct CountedOutput(Rc<Cell<usize>>);
impl<'de> Deserialize<'de> for CountedOutput {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let _result = Result::<u32, String>::deserialize(deserializer)?;
        let count = COUNTED_DECODES.with(|count| {
            count.set(count.get() + 1);
            count.get()
        });

        Ok(Self(Rc::new(Cell::new(count))))
    }
}

#[wasm_bindgen]
pub async fn deferred_reply_check(url: String, drop_worker: bool) -> Result<bool, JsValue> {
    REJECTED_DECODES.with(|count| count.set(0));
    COUNTED_DECODES.with(|count| count.set(0));
    let mut group = AktorGroup::new();
    let completed = group.start().unwrap();
    let worker = group
        .worker::<u32>("deferred", &url, options())
        .await
        .unwrap();
    let mut counted = worker::WorkerRequest::<u32, CountedOutput>::new(
        &worker,
        operation::NAME,
        worker::encode(&(0_u32,)),
    )
    .send()
    .await;
    let mut reply = bad_output(&worker).send().await;
    let retained = if drop_worker {
        None
    } else {
        Some(worker.new_handle())
    };
    drop(worker);

    let published = group.shutdown().await;
    assert!(reply.try_take().is_none());
    assert!(reply.try_take().is_none());
    if !published.failed() {
        return Err(JsValue::from_str(&format!(
            "deferred decode escaped group completion: {published}"
        )));
    }
    assert!(!published.timed_out);
    assert!(
        published
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("output refused")
    );
    assert_eq!(completed.wait().await.to_string(), published.to_string());
    assert_eq!(REJECTED_DECODES.with(Cell::get), 1);
    assert_eq!(COUNTED_DECODES.with(Cell::get), 1);
    assert_eq!(counted.try_take().unwrap().0.get(), 1);
    assert_eq!(COUNTED_DECODES.with(Cell::get), 1);
    drop(retained);
    Ok(true)
}
