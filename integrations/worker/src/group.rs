use aktor::{
    AktorError, AktorGroup, AktorServe,
    dispatch::SerdeCodec,
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
export function crash_initializer() {
    setTimeout(() => { throw new Error('entered worker initializer'); }, 0);
}

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

export function fail_transport() {
    globalThis.proofWorkers.at(-1).dispatchEvent(new ErrorEvent('error', { message: 'transport probe' }));
}

export function retained_owner() {
    const worker = globalThis.proofWorkers.at(-1);
    return worker.proofTerminated !== true && !worker.proofEvents.includes('retained_drop');
}

export function note_retained_drop() {
    postMessage({ proof: 'retained_drop' });
}

export function shutdown_started() {
    const worker = globalThis.proofWorkers.at(-1);
    return worker.proofTerminated === true || worker.proofShutdowns > 0;
}

export function release_worker() {
    globalThis.proofWorkers.at(-1).postMessage('release');
}

let originalPost;
export function break_post() {
    originalPost = Worker.prototype.postMessage;
    Worker.prototype.postMessage = function(buffer) {
        originalPost.call(this, buffer, [buffer, buffer]);
    };
}

export function restore_post() {
    Worker.prototype.postMessage = originalPost;
}

export function break_worker_post_once() {
    const post = globalThis.postMessage;
    globalThis.postMessage = function() {
        globalThis.postMessage = post;
        throw new Error('cleanup notification send failed');
    };
}
"#)]
extern "C" {
    fn crash_initializer();
    fn gate() -> js_sys::Promise;
    fn fail_gate() -> js_sys::Promise;
    fn release_gate();
    fn cleanup_gate() -> js_sys::Promise;
    fn note_cleanup();
    pub fn note_configured();
    pub fn configured() -> js_sys::Promise;
    fn fail_transport();
    fn release_worker();
    fn retained_owner() -> bool;
    fn note_retained_drop();
    fn shutdown_started() -> bool;
    fn break_post();
    fn restore_post();
    fn break_worker_post_once();
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
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    assert_eq!(worker.outstanding(), (0, 0));
    CLOSING.with(|stored| *stored.borrow_mut() = Some(worker.clone()));

    let request = <&worker::Worker<u32> as Transport<
        u32,
        ClosingInput,
        Result<u32, String>,
        (),
        SerdeCodec,
    >>::request(
        &worker,
        Operation {
            name: operation::NAME,
            caller: std::panic::Location::caller(),
        },
        ClosingInput,
    );

    let closed = request.send().now_or_never().is_none();

    assert_eq!(worker.outstanding(), (0, 0));
    let (failed, _) = worker
        .completion()
        .report()
        .await
        .expect_err("refused managed call");
    assert_eq!(failed.outcome, aktor::message::CallError::NotAdmitted);
    assert_eq!(failed.cause, worker::WorkerCause::Closed);
    let report = actors.completion().wait().await;
    assert!(report.failed());
    assert!(!report.ran_past_grace);
    assert!(report.actors.iter().all(|actor| !actor.ran_past_grace));

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .worker::<u32>("posting failure", &url, options())
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    break_post();
    let committed = operation(&worker, 0).send().now_or_never();
    restore_post();
    let reply = committed.expect("posting failure happens after admission");
    let (failure, _) = reply.receive_settled().await.unwrap_err();
    assert_eq!(failure.outcome, aktor::message::CallError::Discarded);

    let (failed, _) = worker
        .completion()
        .report()
        .await
        .expect_err("posting failure");
    assert!(matches!(failed.cause, worker::WorkerCause::Transport(_)));
    assert!(actors.completion().wait().await.failed());

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .worker::<u32>("explicit termination", &url, options())
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    worker.terminate();

    let report = actors.completion().wait().await;
    assert!(report.failed());
    assert!(!report.ran_past_grace);
    assert!(report.actors.iter().all(|actor| !actor.ran_past_grace));

    Ok(closed)
}

#[aktor::aktor(serde)]
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

#[aktor::aktor(serde)]
async fn bad_output(_: &u32) -> RejectedOutput {
    RejectedOutput
}

struct BrokenData;
impl Serialize for BrokenData {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("cleanup data refused"))
    }
}

#[aktor::aktor(serde)]
async fn roundtrip(_: &u32, value: Portable) -> Portable {
    value
}

#[derive(Serialize, Deserialize)]
struct CleanupData {
    code: u32,
    option: Option<Option<u8>>,
}

struct Other;

#[aktor::aktor(role = Other)]
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

    let retired = matches!(result, worker::Retirement::Confirmed(Ok(())));
    assert_eq!(retired, !crash);
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
            Ok::<_, String>(state)
        };
        AktorServe {
            start,
            end: (),
            hooks: Default::default(),
        }
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub async fn cancel_configured_check(url: String) -> Result<bool, JsValue> {
    use futures_util::FutureExt;

    let worker = worker::Worker::<u32>::with_config(&url, options(), &7u32)
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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
        .report()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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
                Err(AktorError {
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
                Err(AktorError {
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
                    Err(AktorError {
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
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub fn start_role_worker() -> Result<(), JsValue> {
    let server = worker::serve_for::<Other, _, _, _, ()>(17u32, async |_| Ok(()), options())
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub async fn registration_check(url: String, other: bool) -> Result<String, JsValue> {
    if other {
        let actor = worker::Worker::<u32, Other>::open(&url, options())
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;
        let output = other_operation(&actor)
            .settle()
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;

        actor
            .shutdown()
            .report()
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;
        return serde_json::to_string(&output)
            .map_err(|error| JsValue::from_str(&error.to_string()));
    }

    let actor = worker::Worker::<u32>::with_options(&url, options())
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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
    let mut group = AktorGroup::with_grace(Duration::from_millis(300));
    let kill = group.killswitch();
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let ordinary_error = Rc::new(Cell::new(false));
    let query_error = ordinary_error.clone();
    let retained = Rc::new(RefCell::new(None));
    let capture = retained.clone();
    let deferred = Rc::new(RefCell::new(None));
    let saved_reply = deferred.clone();
    let completed = group
        .start_with(move |_| hook_count.set(hook_count.get() + 1))
        .unwrap();
    let killswitch = group.killswitch();
    let app = &mut group;

    killswitch
        .run(async move {
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

                    let (error, _) = storage.shutdown().report().await.unwrap_err();
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
        })
        .await;
    killswitch.stop();

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
        let (end, _) = completion.wait_cleanup().await.unwrap_err();
        assert!(matches!(end, worker::WorkerCleanupError::Failed(Some(data)) if data.code == 17));
        assert_eq!(
            completion.cleanup_status(),
            aktor::AktorCleanupStatus::Failed
        );

        let (error, report) = completion.report().await.unwrap_err();
        assert!(report.contains("backup folder is read only"));

        error
            .data
            .is_some_and(|data| data.code == 17 && data.option == Some(None))
    } else if mode == 6 || mode == 7 {
        let completion = retained.borrow_mut().take().unwrap();
        let (error, diagnostic) = completion.report().await.unwrap_err();

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
            .filter(|actor| actor.ran_past_grace)
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
        timed_out: report.ran_past_grace,
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
            Ok::<_, String>(())
        })
        .map_err(|(_, report)| JsValue::from_str(&report))?;

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
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    let audio = actors
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let _audio = plain
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let shutdown = plain.shutdown();

    assert!(plain.killswitch().is_stopping());
    assert!(!(&closing).await.failed());
    assert_eq!(closing.await.actors.len(), shutdown.await.actors.len());

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
            .any(|actor| actor.actor == "audio" && !actor.ran_past_grace))
}

#[wasm_bindgen]
pub async fn group_startup_check(url: String, mode: u32) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::with_grace(Duration::from_millis(500));
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook_count.set(hook_count.get() + 1);
            Ok::<_, String>(())
        })
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    let audio = actors
        .worker_for::<Other, u32>("audio", &format!("{url}?other"), options())
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
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

    let (failed, _) = actors
        .worker_for_data::<(), u32, CleanupData>("storage", &storage_url, storage_options)
        .await
        .err()
        .unwrap();

    assert_eq!(hooks.get(), 1);

    let typed_data = mode != 1
        || matches!(failed.init, aktor::AktorInitOutcome::Failed(data) if data.code == 23);
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
            .any(|actor| actor.actor == "audio" && !actor.ran_past_grace))
}

struct Empty;
struct Signature;
#[aktor::aktor(role = Signature)]
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
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

#[wasm_bindgen]
pub fn typed_setup_failed() -> Result<(), JsValue> {
    worker::setup_failed(aktor::AktorError {
        diagnostics: "could not open storage".into(),
        data: CleanupData {
            code: 23,
            option: Some(None),
        },
    })
    .map_err(|(_, report)| JsValue::from_str(&report))
}

#[wasm_bindgen]
pub async fn typed_setup_check(url: String) -> Result<bool, JsValue> {
    let actor = worker::Worker::<u32, (), CleanupData>::with_options(&url, options())
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let (error, report) = actor.ready().await.unwrap_err();

    assert!(report.contains("could not open storage"));
    assert!(
        matches!(error.init, aktor::AktorInitOutcome::Failed(data) if data.code == 23 && data.option == Some(None))
    );

    let completion = actor.completion();
    assert!(matches!(
        completion.wait_cleanup().await,
        Err((worker::WorkerCleanupError::Unconfirmed, _))
    ));
    assert_eq!(
        completion.cleanup_status(),
        aktor::AktorCleanupStatus::Unconfirmed
    );

    let (error, report) = completion.report().await.unwrap_err();
    assert!(report.contains("could not open storage"));

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
pub async fn start_group_setup_worker(mode: String) -> Result<(), JsValue> {
    let mut transport = options();
    if mode == "limits" {
        transport.capacity = 0;
    }

    let server = worker::serve_setup((), transport, move |()| {
        let mut hooks = aktor::AktorHooks::default();
        if mode == "interval" {
            hooks.intervals.push(aktor::AktorInterval {
                every: Duration::ZERO,
                run: (async |_: &mut u32| {}).into(),
            });
        }

        AktorServe {
            start: async move || {
                if mode == "crash" {
                    crash_initializer();
                    core::future::pending::<()>().await;
                }
                aktor::aktor_ready(0_u32)
            },
            end: (),
            hooks,
        }
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

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
                name: name.into(),
                kind: aktor::AktorKind::BrowserWebWorker(format!("{url}?configured")),
                config: (),
                options: aktor::AktorWorkerOptions {
                    transport: options(),
                    ..Default::default()
                },
                stuck: aktor::AktorStuck::Kill,
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

    let mut group = AktorGroup::with_grace(Duration::from_millis(100));
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
        Some(worker.clone())
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
    assert!(!published.ran_past_grace);
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

#[wasm_bindgen]
pub async fn optional_worker_check(url: String, crash: bool) -> Result<bool, JsValue> {
    use worker::{OptionalWorker, Retirement};

    let worker = OptionalWorker::<u32>::with_options(&url, options())
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    worker
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    if crash {
        let result = operation(&worker, 1).await;

        assert_eq!(
            result.unwrap_err().0.outcome,
            aktor::message::CallError::OutcomeUnknown
        );
        worker.terminate();
        assert!(matches!(
            worker.completion().wait_retired().await,
            Retirement::Terminated(_)
        ));
    } else {
        assert_eq!(
            operation(&worker, 0).await.unwrap(),
            Err("ordinary operation error".into())
        );
        let confirmed = match worker.shutdown().await {
            Retirement::Confirmed(result) => result.is_err() == url.contains("fail_cleanup=true"),
            Retirement::Unconfirmed(_) | Retirement::Terminated(_) => false,
        };

        assert!(confirmed);
    }

    Ok(true)
}

struct RetainedGuard;
impl Drop for RetainedGuard {
    fn drop(&mut self) {
        note_retained_drop();
    }
}

const ROLLBACK_REPORT: &str = "Setup rollback finished\n`- storage rollback diagnostic";

#[wasm_bindgen]
pub async fn start_unconfirmed_worker(mode: u8) -> Result<(), JsValue> {
    if mode == 0 {
        let guard = RetainedGuard;
        let result =
            worker::retain_setup((u32::from(mode), String::from("early setup still owns work")))
                .await;
        drop(guard);
        return result.map_err(|(_, report)| JsValue::from_str(&report));
    }
    let server = worker::serve_setup((), options(), move |(): ()| AktorServe {
        start: async move || {
            if mode == 5 {
                worker::report_setup_pending("rollback pending").unwrap();
                wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();

                return Err((85_u32, ROLLBACK_REPORT.to_owned()));
            }

            if mode != 1 {
                Ok(0_u32)
            } else {
                let guard = RetainedGuard;
                let result =
                    worker::retain_setup((u32::from(mode), String::from("setup still owns work")))
                        .await;
                drop(guard);
                result
            }
        },
        end: async move |_: u32| {
            let _guard = RetainedGuard;
            if mode == 4 {
                break_worker_post_once();
                let (error, _) =
                    worker::report_cleanup_pending("cleanup still owns work").unwrap_err();
                assert!(matches!(error.cause, WorkerCause::Transport(_)));

                worker::report_cleanup_pending("cleanup still owns work").unwrap();
            }

            if mode >= 3 {
                wasm_bindgen_futures::JsFuture::from(cleanup_gate())
                    .await
                    .unwrap();
                return;
            }

            worker::retain_cleanup((mode, String::from("cleanup still owns work"))).await
        },
        hooks: Default::default(),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

async fn unconfirmed_owner(url: &str, mode: u8) {
    use aktor::*;

    let mut group = AktorGroup::with_grace(Duration::from_millis(2));
    group.start().unwrap();
    let program = if mode >= 3 {
        format!(
            "{}?unconfirmed={mode}&cleanup_gate",
            url.replace("group-worker.js", "gate-worker.js")
        )
    } else {
        format!("{url}?unconfirmed={mode}")
    };
    let starting = aktor_start_in(
        &group,
        AktorWorkerNew::<u32, _, u32> {
            name: "unconfirmed owner".into(),
            kind: AktorKind::BrowserWebWorker::new(program),
            config: (),
            options: AktorWorkerOptions {
                transport: options(),
                ..Default::default()
            },
            stuck: if mode >= 3 {
                AktorStuck::Leave
            } else {
                AktorStuck::Kill
            },
        },
    )
    .await;

    if mode == 4 {
        let worker = starting.unwrap();
        let completion = worker.completion();
        assert!(matches!(
            worker.shutdown().await,
            worker::Retirement::Unconfirmed(_)
        ));

        release_worker();
        assert!(matches!(
            completion.wait_retired().await,
            worker::Retirement::Confirmed(Err(_))
        ));
        completion.wait_cleanup().await.unwrap();
        return;
    }

    if mode == 3 {
        let worker = starting.unwrap();
        let reply = operation(&worker, 4).admit().await.unwrap();
        while !worker.executing() {
            gloo_timers::future::TimeoutFuture::new(1).await;
        }

        fail_transport();
        let (failure, _) = reply.receive_settled().await.unwrap_err();
        assert_eq!(failure.outcome, message::CallError::OutcomeUnknown);

        worker.terminate();
        assert!(retained_owner());
        assert!(worker.outstanding().1 > 0);
        release_worker();
        match worker.completion().wait_retired().await {
            worker::Retirement::Confirmed(Err(_)) => {}
            other => panic!("late owner retirement: {other:?}"),
        }
        worker.completion().wait_cleanup().await.unwrap();
        assert_eq!(worker.outstanding().1, 0);
        return;
    }

    if mode == 2 {
        let worker = starting.unwrap();
        let error = match worker.shutdown().await {
            worker::Retirement::Unconfirmed((error, _))
            | worker::Retirement::Terminated((error, _)) => error,
            worker::Retirement::Confirmed(_) => panic!("cleanup did not finish"),
        };
        assert_eq!(error.cause, worker::WorkerCause::Cleanup);
        assert!(error.data.is_none());
        assert_eq!(
            worker.completion().wait_cleanup().await.unwrap_err().0,
            worker::WorkerCleanupError::Unconfirmed
        );

        worker.terminate();
        let _report = group.shutdown().await;
        assert!(matches!(
            worker.completion().wait_retired().await,
            worker::Retirement::Terminated(_)
        ));
    } else {
        let (failure, report) = starting.err().expect("unconfirmed setup returned a worker");
        assert_eq!(
            failure.init,
            AktorInitOutcome::Failed(u32::from(mode)),
            "{report}"
        );
    }

    // The group's report says it, a `Kill` worker that still owned work got terminated.
    let report = group.completion().wait().await;
    let owner = report
        .actors
        .iter()
        .find(|actor| actor.actor == "unconfirmed owner")
        .unwrap();
    assert_eq!(owner.retirement, AktorRetireStatus::Terminated, "{report}");
    assert!(!retained_owner());
}

async fn pending_setup(url: &str) {
    use aktor::{AktorInitOutcome, AktorKind, AktorRetireStatus, AktorStuck, AktorWorkerNew};
    use worker::{Retirement, Worker};

    let program = url.replace("group-worker.js", "gate-worker.js");
    let failed = format!("{program}?unconfirmed=5");
    let worker = Worker::<u32, (), u32>::with_config(&failed, options(), &()).unwrap();
    let completion = worker.completion();
    let (early, report) = worker.ready().await.unwrap_err();
    assert_eq!(
        (early.init, report.as_str()),
        (AktorInitOutcome::Unconfirmed, "rollback pending")
    );
    assert!(matches!(
        completion.wait().await,
        Retirement::Unconfirmed(_)
    ));

    release_worker();
    let Retirement::Confirmed(Err((error, report))) = completion.wait_retired().await else {
        panic!("rollback did not confirm retirement");
    };
    assert_eq!((error.data, report.as_str()), (Some(85), ROLLBACK_REPORT));
    assert_eq!(
        worker.ready().await.unwrap_err().0.init,
        AktorInitOutcome::Failed(85)
    );

    let mut group = AktorGroup::with_grace(Duration::from_millis(2));
    group.start().unwrap();
    let completion = group.completion();
    let start = AktorWorkerNew::<u32, _, u32> {
        name: "pending setup".into(),
        kind: AktorKind::BrowserWebWorker::new(failed),
        config: (),
        options: aktor::AktorWorkerOptions {
            transport: options(),
            ..Default::default()
        },
        stuck: AktorStuck::Leave,
    };
    aktor::aktor_start_in(&group, start).await.err().unwrap();
    assert_eq!(
        completion.wait().await.actors[0].retirement,
        AktorRetireStatus::Unconfirmed
    );

    release_worker();
    let retired = completion.wait_retired().await.unwrap();
    let diagnostics = &retired.actors[0].diagnostics;
    assert_eq!(
        diagnostics
            .iter()
            .filter(|entry| entry.diagnostics == ROLLBACK_REPORT)
            .count(),
        1
    );
}

async fn leave_queued(url: &str, fail_output: bool) {
    use aktor::*;

    let mut group = AktorGroup::with_grace(Duration::from_millis(5));
    group.start().unwrap();
    let (release, joining) = tokio::sync::oneshot::channel();

    let worker = group
        .spawn(AktorWorkerNew::<u32, _, ()> {
            name: "storage".into(),
            kind: AktorKind::BrowserWebWorker::new(format!(
                "{}?configured",
                url.replace("group-worker.js", "gate-worker.js")
            )),
            config: (),
            options: AktorWorkerOptions {
                transport: options(),
                before_shutdown: Some(worker::join(async move { joining.await.unwrap() })),
            },
            stuck: AktorStuck::Leave,
        })
        .await
        .unwrap();
    let running = operation(&worker, 4).admit().await.unwrap();
    let rejected = if fail_output {
        Some(bad_output(&worker).admit().await.unwrap())
    } else {
        None
    };
    let queued = operation(&worker, 7).admit().await.unwrap();

    let stopped = group.shutdown();
    assert!(!stopped.wait().await.retired());
    assert_eq!(
        stopped.retirement_status(),
        aktor::AktorGroupRetirement::Unconfirmed
    );
    release_worker();
    release.send(()).unwrap();

    assert_eq!(running.receive_settled().await.unwrap(), Ok(4));
    if let Some(rejected) = rejected {
        assert!(rejected.receive_settled().await.is_err());
    }
    assert_eq!(queued.receive_settled().await.unwrap(), Ok(7));

    let report = stopped.wait_retired().await.unwrap();
    assert_eq!(
        stopped.retirement_status(),
        aktor::AktorGroupRetirement::AllConfirmed
    );
    assert_eq!(report.to_string().contains("output refused"), fail_output);
}

#[derive(er::Er)]
struct CloseErr;

struct OwnedJoin(Rc<Cell<u32>>);
impl Drop for OwnedJoin {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[wasm_bindgen]
pub async fn optional_group_check(url: String) -> Result<bool, JsValue> {
    use aktor::*;
    use er::ErContextExt;

    let mut group = AktorGroup::with_grace(Duration::from_millis(100));
    group.start().unwrap();

    let setup = |name: &str, program: &str| AktorWorkerNew {
        name: name.into(),
        kind: AktorKind::BrowserWebWorker(program),
        config: (),
        options: AktorWorkerOptions {
            transport: options(),
            ..Default::default()
        },
        stuck: AktorStuck::Kill,
    };
    let configured = format!("{url}?configured");
    for (query, expected) in [
        ("interval", AktorInitOutcome::NotRun),
        ("limits", AktorInitOutcome::NotRun),
        ("crash", AktorInitOutcome::Unconfirmed),
    ] {
        let program = format!("{configured}&mode={query}");
        let optional = group
            .spawn(setup(query, &program).optional())
            .await
            .unwrap();
        let (error, _) = optional.ready().await.unwrap_err();
        assert_eq!(error.init, expected);

        let (failure, _) = aktor_start(AktorSetup {
            actors: setup(query, &program),
            shutdown: |_| {},
            grace: Duration::from_millis(100),
        })
        .await
        .err()
        .unwrap();
        assert_eq!(failure.init, expected);
    }

    break_post();
    let optional = group
        .spawn(setup("initialize post", &configured).optional())
        .await
        .unwrap();
    let error = optional.ready().await.unwrap_err().0;
    restore_post();
    assert_eq!(error.init, AktorInitOutcome::NotRun);

    let (preferences, unavailable) = aktor_start_in(
        &group,
        (
            setup("preferences", &configured),
            setup("unavailable", "/missing-worker.js").optional(),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        operation(&unavailable, 42).await.unwrap_err().0.outcome,
        aktor::message::CallError::NotAdmitted
    );

    let mut invalid = setup("invalid", &configured);
    let drops = Rc::new(Cell::new(0));
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    invalid.options.transport.capacity = 0;
    invalid.options.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
    }));

    let invalid = aktor_start_in(&group, invalid.optional()).await.unwrap();
    let error = operation(&invalid, 42).await.unwrap_err();
    let completion = invalid.completion();
    drop(invalid);

    assert_eq!(drops.get(), 0);
    assert!(matches!(
        completion.wait().await,
        worker::Retirement::Unconfirmed(_)
    ));

    release.send(()).unwrap();
    match completion.wait_retired().await {
        worker::Retirement::Confirmed(Err(failure)) => assert_eq!(failure, error),
        other => panic!("construction retirement: {other:?}"),
    }
    assert_eq!(drops.get(), 1);

    let mut refused = setup("unavailable refused join", &configured);
    refused.options.transport.capacity = 0;
    let refused = aktor_start_in(&group, refused.optional()).await.unwrap();
    refused_join(refused, 3).await;

    let archive = aktor_start_in(&group, setup("archive", &configured).optional())
        .await
        .unwrap();
    let _failure = operation(&archive, 1).await;

    let rejected = format!("{url}?rollback");
    let (release, joining) = tokio::sync::oneshot::channel();
    let mut rejected = setup("rejected", &rejected);
    rejected.options.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
    }));
    let rejected = aktor_start_in(&group, rejected.optional()).await.unwrap();
    let error = operation(&rejected, 42)
        .await
        .er(|| std::fmt::Error)
        .unwrap_err();
    let completion = rejected.completion();

    assert!(matches!(
        completion.wait().await,
        worker::Retirement::Unconfirmed(_)
    ));
    assert_eq!(operation(&preferences, 42).await, Ok(42));
    group.killswitch().stop();
    gloo_timers::future::TimeoutFuture::new(0).await;
    release.send(()).unwrap();

    let retired = completion.wait_retired().await;

    assert!(error.er_report().contains("storage rollback diagnostic"));
    assert!(matches!(retired, worker::Retirement::Confirmed(Err(_))));

    let report = group.completion().await;
    let rejected = report
        .actors
        .iter()
        .find(|actor| actor.actor == "rejected")
        .unwrap();
    assert_eq!(rejected.retirement, AktorRetireStatus::Confirmed);

    let mut required = AktorGroup::new();
    required.start().unwrap();
    let mut invalid = setup("required", &configured);
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();
    invalid.options.transport.capacity = 0;
    invalid.options.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
        finished.send(()).unwrap();
        Err(er::er_new!(CloseErr, [std::fmt::Error]))
    }));

    let (error, _) = aktor_start_in(&required, invalid).await.err().unwrap();

    assert_eq!(
        error.report.unwrap().actors[0].retirement,
        AktorRetireStatus::Unconfirmed
    );
    assert_eq!(drops.get(), 1);
    release.send(()).unwrap();
    joined.await.unwrap();
    assert_eq!(drops.get(), 2);

    let retired = required.completion().wait_retired().await.unwrap();
    let failure = retired.actors[0]
        .diagnostics
        .iter()
        .find(|error| error.diagnostics.contains("CloseErr"))
        .unwrap();
    assert!(failure.diagnostics.contains(&std::fmt::Error.to_string()));

    for mode in 0..5 {
        unconfirmed_owner(&url, mode).await;
    }
    pending_setup(&url).await;
    leave_queued(&url, false).await;
    leave_queued(&url, true).await;

    for mode in 0..4 {
        unstarted_join(&configured, mode).await;
    }

    for mode in 0..3 {
        let worker =
            worker::OptionalWorker::<u32>::with_config(&configured, options(), &()).unwrap();
        refused_join(worker, mode).await;
    }

    Ok(true)
}

async fn refused_join(worker: worker::OptionalWorker<u32>, mode: u8) {
    use futures_util::FutureExt;
    use worker::Retirement;

    let (first_release, first_join) = tokio::sync::oneshot::channel();
    let worker = if mode == 1 {
        worker
            .before_shutdown(async move {
                first_join.await.unwrap();
            })
            .unwrap()
    } else {
        worker
    };

    if mode == 0 || mode == 1 {
        worker.ready().await.unwrap();
    }

    let retained = (mode == 2).then(|| worker.clone());
    let completion = worker.completion();
    let drops = Rc::new(Cell::new(0));
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    let (started, starting) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();

    assert!(
        worker
            .before_shutdown(async move {
                started.send(()).unwrap();
                joining.await.unwrap();
                drop(guard);
                finished.send(()).unwrap();
                Err(String::from("refused join finished with an error"))
            })
            .is_err()
    );

    starting.await.unwrap();

    if mode == 0 {
        gloo_timers::future::TimeoutFuture::new(0).await;
        assert!(!shutdown_started());
    }

    if let Some(worker) = retained {
        worker.ready().await.unwrap();
        match worker.shutdown().await {
            Retirement::Confirmed(Ok(())) => {}
            other => panic!("shared worker retirement: {other:?}"),
        }
    }

    if mode == 3 {
        match completion.wait().await {
            Retirement::Confirmed(Err(_)) => {}
            other => panic!("unavailable worker retirement: {other:?}"),
        }
    }

    assert_eq!(drops.get(), 0);
    release.send(()).unwrap();
    joined.await.unwrap();

    if mode == 1 {
        assert!(completion.wait().now_or_never().is_none());
        first_release.send(()).unwrap();
    }

    match completion.wait_retired().await {
        Retirement::Confirmed(result) => assert_eq!(result.is_err(), mode == 3),
        other => panic!("refused join retirement: {other:?}"),
    }

    assert_eq!(drops.get(), 1);
    assert_eq!(
        completion
            .wait_diagnostics()
            .await
            .iter()
            .filter(|error| error.diagnostics == "refused join finished with an error")
            .count(),
        1
    );
}

async fn unstarted_join(url: &str, mode: u8) {
    use aktor::*;
    use futures_util::FutureExt;

    let setup = |name: &str| AktorWorkerNew::<u32, _> {
        name: name.into(),
        kind: AktorKind::BrowserWebWorker(url),
        config: (),
        options: AktorWorkerOptions {
            transport: options(),
            ..Default::default()
        },
        stuck: AktorStuck::Kill,
    };

    let mut group = AktorGroup::new();
    let mut unstarted = setup("unstarted dependency");
    let drops = Rc::new(Cell::new(0));
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();
    unstarted.options.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
        finished.send(()).unwrap();
    }));

    match mode {
        0 => {
            // Preflight rejects the setup before start_in.
            assert!(aktor_start_in(&group, unstarted.optional()).await.is_err());
        }
        1 => {
            group.start().unwrap();
            drop(aktor_start_in(&group, unstarted.optional()));
        }
        2 => {
            group.start().unwrap();
            let mut invalid = setup("first setup fails");
            invalid.options.transport.capacity = 0;

            let entries = (invalid, (setup("skipped"), unstarted.optional()));
            assert!(aktor_start_in(&group, entries).await.is_err());
        }
        _ => {
            group.start().unwrap();
            let mut opening = Box::pin(aktor_start_in(&group, unstarted));
            assert!(opening.as_mut().now_or_never().is_none());
            drop(opening);
        }
    }

    assert_eq!(drops.get(), 0);
    release.send(()).unwrap();
    joined.await.unwrap();
    assert_eq!(drops.get(), 1);

    if mode != 0 {
        let report = group.shutdown().await;
        if mode == 3 {
            assert!(!report.failed(), "{report}");
        }
    }
}

#[wasm_bindgen]
pub async fn late_retirement_check(url: String, startup: bool) -> Result<bool, JsValue> {
    use worker::OptionalWorker;

    let completion = if startup {
        let worker = OptionalWorker::<u32>::with_config(&url, options(), &7u32).unwrap();
        wasm_bindgen_futures::JsFuture::from(configured()).await?;
        let completion = worker.completion();

        drop(worker);
        completion
    } else {
        let worker = OptionalWorker::<u32>::with_options(&url, options()).unwrap();
        worker.ready().await.unwrap();

        let reply = operation(&worker, 4).send().await.unwrap();
        fail_transport();
        let _result = reply.await;
        let completion = worker.completion();

        drop(worker);
        completion
    };

    release_worker();

    Ok(matches!(
        completion.wait_retired().await,
        worker::Retirement::Confirmed(Err(_))
    ))
}

#[wasm_bindgen]
pub async fn shutdown_dependency_check(url: String, mode: u8) -> Result<bool, JsValue> {
    use worker::OptionalWorker;

    let (release, joining) = tokio::sync::oneshot::channel();
    let url = if mode == 3 {
        format!("{url}?fail_cleanup=true")
    } else {
        url
    };
    let worker = OptionalWorker::<u32>::with_options(&url, options())
        .unwrap()
        .before_shutdown(async move {
            joining.await.unwrap();

            if mode == 2 {
                Err((
                    mode,
                    String::from("external cleanup finished with an error"),
                ))
            } else {
                Ok(())
            }
        })
        .unwrap();
    worker.ready().await.unwrap();

    let completion = worker.completion();
    let mut first = None;
    if mode == 1 {
        worker.terminate();
    } else if mode == 3 {
        fail_transport();
        first = Some(completion.report().await.unwrap_err());
    } else {
        drop(worker.shutdown());
    }

    gloo_timers::future::TimeoutFuture::new(1).await;
    let waited = !shutdown_started();
    release.send(()).unwrap();

    while !shutdown_started() {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    let retirement = completion.wait_retired().await;
    let cleanup = completion.wait_cleanup().await;

    match mode {
        1 => assert_eq!(
            cleanup.unwrap_err().0,
            worker::WorkerCleanupError::Unconfirmed
        ),
        3 => {
            let (failure, report) = cleanup.unwrap_err();
            assert!(matches!(failure, worker::WorkerCleanupError::Failed(_)));
            assert!(report.contains("backup folder is read only"));
        }
        _ => cleanup.unwrap(),
    }

    if let Some(first) = first {
        let worker::Retirement::Confirmed(Err(late)) = &retirement else {
            panic!("worker did not confirm cleanup: {retirement:?}");
        };
        assert_eq!(late, &first);
        assert!(
            completion
                .wait_diagnostics()
                .await
                .iter()
                .any(|error| error.diagnostics.contains("backup folder is read only"))
        );
    }

    if mode == 2 {
        let diagnostics = completion.wait_diagnostics().await;
        assert_eq!(
            diagnostics[0].diagnostics,
            "external cleanup finished with an error"
        );
    }

    Ok(waited
        && match retirement {
            worker::Retirement::Confirmed(Ok(())) => mode == 0,
            worker::Retirement::Confirmed(Err(_)) => mode == 2 || mode == 3,
            worker::Retirement::Terminated(_) => mode == 1,
            worker::Retirement::Unconfirmed(_) => false,
        })
}

#[wasm_bindgen]
pub async fn incompatible_owner_check(url: String, startup: bool) -> Result<bool, JsValue> {
    use worker::{OptionalWorker, Retirement};

    let mut options = options();
    options.build.push_str("-incompatible");
    let worker = OptionalWorker::<u32>::with_options(&url, options).unwrap();
    if startup {
        wasm_bindgen_futures::JsFuture::from(configured()).await?;
    } else {
        let error = worker.ready().await.unwrap_err().0;
        assert_eq!(error.init, aktor::AktorInitOutcome::Unconfirmed);
    }
    let completion = worker.completion();
    drop(worker);
    let uncertain = matches!(completion.wait().await, Retirement::Unconfirmed(_));

    release_worker();

    Ok(uncertain
        && match completion.wait_retired().await {
            Retirement::Confirmed(Err((error, _))) => {
                error.cause
                    == if startup {
                        WorkerCause::Closed
                    } else {
                        WorkerCause::Protocol
                    }
            }
            _ => false,
        })
}

#[wasm_bindgen]
pub async fn start_rejected_worker() -> Result<(), JsValue> {
    let _result = worker::serve_setup((), options(), |_: ()| AktorServe {
        start: async || {
            Err::<u32, _>(String::from(
                "setup rollback finished\n`- storage rollback diagnostic",
            ))
        },
        end: (),
        hooks: Default::default(),
    })
    .await;

    Ok(())
}

#[wasm_bindgen]
pub async fn start_raw_unready_worker() -> Result<(), JsValue> {
    note_configured();
    wasm_bindgen_futures::JsFuture::from(gate()).await?;
    start_group_worker(false, false, false)
}
