use aktor::{
    AktorData, AktorGroup, AktorHooks, AktorReady, AktorServe, AktorWorkerContract,
    data::{DataError, Reader, Writer},
    dispatch::DataCodec,
    worker::{self, Options, WorkerCause},
};
use er::*;
use serde::Serialize;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::Duration,
};
use wasm_bindgen::prelude::*;

thread_local! { static CLOSING: RefCell<Option<worker::Worker<GroupWorker>>> = const { RefCell::new(None) }; }

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

struct ClosingInput;
impl AktorData for ClosingInput {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        let worker = CLOSING.with(|worker| worker.borrow_mut().take()).unwrap();
        drop(worker.shutdown());
        writer.write(&7u32)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let _: u32 = reader.read()?;
        Ok(Self)
    }
}

#[wasm_bindgen]
pub async fn serialization_shutdown_check(url: String) -> Result<bool, JsValue> {
    use aktor::{dispatch::Transport, operation::Operation};

    use futures_util::FutureExt;
    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "serialization shutdown",
            &url,
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    assert_eq!(worker.outstanding(), (0, 0));
    CLOSING.with(|stored| *stored.borrow_mut() = Some(worker.clone()));

    let request = <&worker::Worker<GroupWorker> as Transport<
        u32,
        ClosingInput,
        Result<u32, String>,
        (),
        DataCodec,
    >>::request(
        &worker,
        Operation {
            name: operation::NAME,
            caller: std::panic::Location::caller(),
        },
        ClosingInput,
    );

    let closed = request.send().await.is_err();

    assert_eq!(worker.outstanding(), (0, 0));
    worker.completion().report().await.unwrap();
    let report = actors.stop().await;
    assert!(!report.failed());

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "posting failure",
            &url,
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    break_post();
    let committed = operation(&worker, 0).send().now_or_never();
    restore_post();
    let reply = committed
        .expect("posting failure happens after admission")
        .unwrap();
    let failure = reply.await.unwrap_err();
    assert_eq!(
        failure.outcome,
        aktor::message::AktorDeliveryOutcome::Discarded
    );

    let (failed, _) = worker
        .completion()
        .report()
        .await
        .expect_err("posting failure");
    assert!(matches!(failed.cause, worker::WorkerCause::Transport));
    assert!(actors.completion().wait().await.unwrap().failed());

    let mut actors = AktorGroup::new();
    actors.start().unwrap();
    let worker = actors
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "explicit termination",
            &url,
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    worker.terminate();
    worker.terminate();

    let report = actors.completion().wait().await.unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(!report.ran_past_grace());
    assert!(report.actors.iter().all(|actor| !actor.ran_past_grace()));

    Ok(closed)
}

#[aktor::aktor(latest)]
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

#[derive(Debug, Clone, PartialEq, AktorData)]
enum Choice {
    Text { text: String },
    Number(u64),
}

#[derive(Debug, Clone, PartialEq, AktorData)]
struct Portable {
    options: Vec<Option<Option<u8>>>,
    map: std::collections::BTreeMap<(u32, String), u128>,
    choice: Vec<Choice>,
    limits: (i128, u128),
    invalid: Option<RejectedInput>,
}

#[derive(Debug, Clone, PartialEq)]
struct RejectedInput;
impl AktorData for RejectedInput {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Ok(())
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Err(DataError::new("input refused"))
    }
}

struct RejectedOutput;
impl AktorData for RejectedOutput {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Ok(())
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        REJECTED_DECODES.with(|count| count.set(count.get() + 1));
        Err(DataError::new("output refused"))
    }
}

#[aktor::aktor]
async fn bad_output(_: &u32) -> RejectedOutput {
    RejectedOutput
}

#[derive(Debug)]
struct BrokenData;
impl AktorData for BrokenData {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Err(DataError::new("cleanup data refused"))
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(Self)
    }
}

#[aktor::aktor]
async fn roundtrip(_: &u32, value: Portable) -> Portable {
    value
}

#[derive(Debug, PartialEq, aktor::AktorData)]
pub(super) struct CleanupData {
    code: u32,
    option: Option<Option<u8>>,
}

struct Other;

#[aktor::aktor(role = Other)]
async fn other_operation(state: &u32) -> u32 {
    *state
}

fn options() -> Options {
    crate::options("group-v1")
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
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "gated",
            &url,
            (),
            Options {
                capacity: core::num::NonZeroUsize::new(if bytes { 2 } else { 1 }).unwrap(),
                max_outstanding_bytes: if bytes {
                    core::num::NonZeroUsize::MIN
                } else {
                    options().max_outstanding_bytes
                },
                ..options()
            },
            aktor::Required,
        ))
        .ready()
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
        .await
        .unwrap(),
    );

    if cleanup {
        assert_eq!(reply.take().unwrap().await.unwrap(), Ok(7));
    } else {
        while !worker.executing() {
            gloo_timers::future::TimeoutFuture::new(0).await;
        }
    }

    let mut blocked = Box::pin(operation(&worker, 9).into_future());

    if !cleanup {
        assert!(blocked.as_mut().now_or_never().is_none());
    }

    let closing = worker.shutdown();

    actors.killswitch().stop();
    assert!(blocked.as_mut().await.is_err());

    let prematurely_finished = worker.completion().wait().now_or_never().is_some();

    drop(blocked);
    release_gate();

    let result = closing.await;

    if !cleanup && !crash {
        assert_eq!(reply.take().unwrap().await.unwrap(), Ok(4));
    }

    drop(reply);

    let report = actors.completion().await.unwrap();

    if prematurely_finished {
        return Err(JsValue::from_str("blocked admission terminated the worker"));
    }

    let retired = matches!(result, Ok(aktor::AktorRetired::Released(Ok(()))));
    assert_eq!(retired, !crash);
    assert!(!report.failed(), "{report}");
    Ok(true)
}

#[wasm_bindgen]
pub fn cancel_startup_check(url: String) -> bool {
    use futures_util::FutureExt;

    let group = crate::group();
    let spawned = group.spawn(group_setup::<GroupWorker>("cancel startup", &url));
    let mut opening = Box::pin(spawned.ready());
    let pending = opening.as_mut().now_or_never().is_none();

    drop(opening);
    pending
}

struct GatedWorker;
impl AktorWorkerContract for GatedWorker {
    type State = u32;
    type Role = ();
    type Config = u32;
    type Ready = ();
    type Error = CleanupData;
}

#[wasm_bindgen]
pub async fn start_unready_worker() -> Result<(), JsValue> {
    worker::serve_setup::<GatedWorker>(options(), |state| {
        let start = async move || {
            note_configured();
            wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();
            Ok::<_, (CleanupData, String)>(state)
        };
        AktorServe {
            start: aktor::AktorInit::Try(start),
            end: (),
            hooks: AktorHooks::NONE,
        }
    })
    .await
    .er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn cancel_configured_check(url: String) -> Result<bool, JsValue> {
    use futures_util::FutureExt;

    let group = crate::group();
    let (_, worker) = group
        .spawn(crate::worker_setup::<GatedWorker, _>(
            "gated",
            &url,
            7,
            options(),
            aktor::Required,
        ))
        .into_parts()
        .unwrap();
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

    let group = crate::group();
    let worker = group
        .spawn(group_setup::<GroupWorker>("sessions", &url))
        .ready()
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

struct RoleWorker;
impl AktorWorkerContract for RoleWorker {
    type State = u32;
    type Role = Other;
    type Config = ();
    type Ready = ();
    type Error = ();
}

struct BrokenWorker;
impl AktorWorkerContract for BrokenWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = BrokenData;
}

struct WrongDataWorker;
impl AktorWorkerContract for WrongDataWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = String;
}

#[wasm_bindgen]
pub async fn start_group_worker(
    fail_cleanup: bool,
    wrong_data: bool,
    fail_data: bool,
) -> Result<(), JsValue> {
    let result = if fail_data {
        worker::serve_setup::<BrokenWorker>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || 0u32),
            end: async |_| Err((BrokenData, String::from("backup folder is read only"))),
            hooks: AktorHooks::NONE,
        })
        .await
    } else if wrong_data {
        worker::serve_setup::<WrongDataWorker>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || 0u32),
            end: async |_| {
                Err((
                    String::from("unexpected data type"),
                    String::from("backup folder is read only"),
                ))
            },
            hooks: AktorHooks::NONE,
        })
        .await
    } else {
        worker::serve_setup::<GroupWorker>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || 0u32),
            end: async move |_| {
                note_cleanup();
                wasm_bindgen_futures::JsFuture::from(cleanup_gate())
                    .await
                    .unwrap();
                if fail_cleanup {
                    Err((
                        CleanupData {
                            code: 17,
                            option: Some(None),
                        },
                        String::from("Flush settings\n  backup folder is read only"),
                    ))
                } else {
                    Ok(())
                }
            },
            hooks: AktorHooks::NONE,
        })
        .await
    };
    result.er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn start_role_worker() -> Result<(), JsValue> {
    worker::serve_setup::<RoleWorker>(options(), |()| AktorServe {
        start: aktor::AktorInit::Ready(async || 17u32),
        end: (),
        hooks: AktorHooks::NONE,
    })
    .await
    .er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn registration_check(url: String, other: bool) -> Result<String, JsValue> {
    if other {
        let group = crate::group();
        let actor = group
            .spawn(crate::worker_setup::<RoleWorker, _>(
                "other",
                &url,
                (),
                options(),
                aktor::Required,
            ))
            .ready()
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;
        let output = other_operation(&actor)
            .await
            .map_err(|error| JsValue::from_str(&error.report))?;

        actor
            .shutdown()
            .report()
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;
        return serde_json::to_string(&output)
            .map_err(|error| JsValue::from_str(&error.to_string()));
    }

    let group = crate::group();
    let (_, actor) = group
        .spawn(group_setup::<GroupWorker>("registration", &url))
        .into_parts()
        .unwrap();
    let result = crate::browser::startup(actor.ready().await);

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
                .spawn(crate::worker_setup::<GroupWorker, _>(
                    "storage",
                    &storage_url,
                    (),
                    options(),
                    aktor::Required,
                ))
                .ready()
                .await
                .unwrap();

            *capture.borrow_mut() = Some(storage.completion());

            let audio = app
                .spawn(crate::worker_setup::<RoleWorker, _>(
                    "audio",
                    &format!("{url}?other"),
                    (),
                    options(),
                    aktor::Required,
                ))
                .ready()
                .await
                .unwrap();

            assert_eq!(other_operation(&audio).await.unwrap(), 17);

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

                    assert_eq!(roundtrip(&storage, value.clone()).await.unwrap(), value);
                    query_error.set(operation(&storage, 0).await.unwrap().is_err());
                }
                1 => {
                    *saved_reply.borrow_mut() = Some(operation(&storage, 1).send().await.unwrap());
                    core::future::pending::<()>().await;
                }
                2 | 3 => {
                    let reply = operation(&storage, mode).send().await.unwrap();

                    while !storage.executing() {
                        gloo_timers::future::TimeoutFuture::new(1).await;
                    }

                    kill.stop();
                    kill.stop();
                    assert!(reply.await.is_err());
                }
                8 => {
                    let mut reply = bad_output(&storage).send().await.unwrap();

                    let (error, _) = storage.shutdown().report().await.unwrap_err();
                    assert!(matches!(error.cause, WorkerCause::Codec));
                    assert!(kill.is_stopping());
                    assert!(reply.try_take().unwrap().is_err());
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

                    drop(roundtrip(&storage, value).send().await.unwrap());
                    core::future::pending::<()>().await;
                }
                _ => {}
            }
        })
        .await;
    killswitch.stop();

    let report = completed.wait().await.unwrap();
    assert_eq!(
        report
            .to_string()
            .matches("backup folder is read only")
            .count(),
        usize::from(mode == 4 || mode == 6 || mode == 7)
    );
    let typed_cleanup = if mode == 4 {
        let completion = retained.borrow_mut().take().unwrap();
        assert!(matches!(
            completion.cleanup(),
            aktor::AktorCleanup::Returned(_)
        ));
        assert!(!report.failures[0].delivered());
        let aktor::AktorCleanup::Returned(Err(end)) = completion.cleanup() else {
            panic!("missing cleanup failure")
        };
        assert!(matches!(end.take().data, Some(data) if data.code == 17));

        assert!(report.failures[0].delivered());
        let (error, report) = completion.report().await.unwrap_err();
        assert!(report.contains("backup folder is read only"));

        error
            .data
            .is_some_and(|data| data.code == 17 && data.option == Some(None))
    } else if mode == 6 || mode == 7 {
        let completion = retained.borrow_mut().take().unwrap();
        let (error, diagnostic) = completion.report().await.unwrap_err();

        let cause = if mode == 6 {
            "invalid option tag"
        } else {
            "cleanup data refused"
        };
        assert!(diagnostic.contains(cause), "{diagnostic}");

        error.data.is_none() && diagnostic.contains("backup folder is read only")
    } else {
        true
    };

    let deferred_reply_safe = if let Some(mut reply) = deferred.borrow_mut().take() {
        reply.try_take().is_some_and(|result| result.is_err())
    } else {
        true
    };

    serde_json::to_string(&Check {
        failure_message: report.failures.first().map(|failure| failure.error.clone()),
        failure: report
            .failures
            .first()
            .and_then(|failure| match &failure.source {
                aktor::AktorFailureSource::Actor { name, .. } => Some(name.clone()),
                _ => None,
            }),
        timed_out_actors: report
            .actors
            .iter()
            .filter(|actor| actor.ran_past_grace())
            .map(|actor| actor.name.clone())
            .collect(),
        actors: report.actors.len(),
        diagnostics: report
            .actors
            .iter()
            .map(|attempt| {
                let errors = report
                    .actor_failures(attempt.id)
                    .filter(|failure| {
                        report
                            .failures
                            .first()
                            .is_none_or(|first| first.id != failure.id)
                    })
                    .map(|failure| failure.error.clone())
                    .collect();
                (attempt.name.clone(), errors)
            })
            .collect(),
        hook_count: hooks.get(),
        typed_cleanup,
        deferred_reply_safe,
        ordinary_error: ordinary_error.get(),
        timed_out: report.ran_past_grace(),
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
        })
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    let _closing = closing;

    let storage = actors
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "storage",
            &url,
            (),
            Options {
                capacity: core::num::NonZeroUsize::MIN,
                ..options()
            },
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    let audio = actors
        .spawn(crate::worker_setup::<RoleWorker, _>(
            "audio",
            &format!("{url}?other"),
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let mut reply = operation(&storage, 0).send().await.unwrap();
    let ordinary_error = loop {
        if let Some(result) = reply.try_take() {
            break result.unwrap().is_err();
        }

        gloo_timers::future::TimeoutFuture::new(1).await;
    };

    assert!(reply.try_take().is_none());

    let mut pending = operation(&storage, 0).send().await.unwrap();

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
    assert!(pending.try_take().unwrap().unwrap().is_err());
    assert!(pending.try_take().is_none());
    assert!(!kill.is_stopping());
    assert_eq!(other_operation(&audio).await.unwrap(), 17);

    let occupied = operation(&storage, 2).send().await.unwrap();
    let mut sending = Box::pin(operation(&storage, 0).send());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    assert!(sending.as_mut().poll(&mut context).is_pending());
    storage.terminate();

    assert!(sending.as_mut().await.is_err());

    drop(sending);
    drop(occupied);

    kill.wait_stopping().await;

    let report = (&completed).await.unwrap();

    assert_eq!(completed.await.unwrap().actors.len(), report.actors.len());

    let mut plain = AktorGroup::new();
    let closing = plain
        .start()
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let _audio = plain
        .spawn(crate::worker_setup::<RoleWorker, _>(
            "audio",
            &format!("{url}?other"),
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let shutdown = plain.stop();

    assert!(plain.killswitch().is_stopping());
    assert!(!(&closing).await.unwrap().failed());
    assert_eq!(
        closing.await.unwrap().actors.len(),
        shutdown.await.actors.len()
    );

    Ok(ordinary_error
        && hooks.get() == 1
        && report.actors.len() == 2
        && report
            .failures.first()
            .is_some_and(|failure| matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "storage"))
        && report
            .actors
            .iter()
            .any(|actor| actor.name == "audio" && !actor.ran_past_grace()))
}

#[wasm_bindgen]
pub async fn group_startup_check(url: String, mode: u32) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::with_grace(Duration::from_millis(500));
    let hooks = Rc::new(Cell::new(0));
    let hook_count = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook_count.set(hook_count.get() + 1);
        })
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    let audio = actors
        .spawn(crate::worker_setup::<RoleWorker, _>(
            "audio",
            &format!("{url}?other"),
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    let mut storage_options = options();

    if mode == 0 {
        storage_options.capacity = core::num::NonZeroUsize::MAX;
    } else if mode == 2 {
        storage_options.build = "wrong build".into();
    }

    let storage_url = if mode == 1 {
        format!("{url}?fail_setup")
    } else {
        url
    };

    let spawned = actors.spawn(crate::worker_setup::<GroupWorker, _>(
        "storage",
        &storage_url,
        (),
        storage_options,
        aktor::Required,
    ));
    let (failed, _) = spawned.ready().await.err().unwrap();

    assert_eq!(hooks.get(), 1);

    let typed_data = mode != 1
        || matches!(&failed, aktor::AktorStartError::Initializer { error: data, .. } if data.code == 23);
    let report = closing.wait().await.unwrap();

    Ok(typed_data
        && matches!(report.stop_cause, aktor::StopCause::StartupFailure(_))
        && report
            .failures.first()
            .is_some_and(|failure| matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "storage"))
        && audio.completion().report().await.is_ok()
        && report
            .actors
            .iter()
            .any(|actor| actor.name == "audio" && !actor.ran_past_grace()))
}

struct Empty;
struct Signature;
#[aktor::aktor(role = Signature)]
async fn signature(_: &u32, _: u64) -> u32 {
    17
}

impl AktorWorkerContract for Empty {
    type State = u32;
    type Role = Empty;
    type Config = ();
    type Ready = ();
    type Error = ();
}

impl AktorWorkerContract for Signature {
    type State = u32;
    type Role = Signature;
    type Config = ();
    type Ready = ();
    type Error = ();
}

#[wasm_bindgen]
pub async fn start_registration_worker(signature: bool) -> Result<(), JsValue> {
    async fn serve<
        C: AktorWorkerContract<State = u32, Role = R, Ready = (), Config = (), Error = ()>,
        R,
    >() -> Result<(), (worker::WorkerError, String)> {
        worker::serve_setup::<C>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || 0u32),
            end: (),
            hooks: AktorHooks::NONE,
        })
        .await
    }
    let result = if signature {
        serve::<Signature, _>().await
    } else {
        serve::<Empty, _>().await
    };
    result.er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn typed_setup_failed() -> Result<(), JsValue> {
    worker::serve_setup::<GroupWorker>(options(), |()| AktorServe {
        start: aktor::AktorInit::Try(async || {
            Err::<u32, _>((
                CleanupData {
                    code: 23,
                    option: Some(None),
                },
                String::from("could not open storage"),
            ))
        }),
        end: (),
        hooks: AktorHooks::NONE,
    })
    .await
    .er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn typed_setup_check(url: String) -> Result<bool, JsValue> {
    let group = crate::group();
    let (_, actor) = group
        .spawn(group_setup::<GroupWorker>("registration", &url))
        .into_parts()
        .unwrap();
    let (error, report) = actor.ready().await.unwrap_err();

    assert!(report.contains("could not open storage"));
    assert!(
        matches!(error, worker::WorkerStartError::Initializer { error: data, .. } if data.code == 23 && data.option == Some(None))
    );

    let completion = actor.completion();
    assert!(matches!(
        completion.wait_cleanup().await,
        Err((worker::WorkerCleanupError::Unconfirmed(_), _))
    ));

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

pub(super) struct GroupWorker;
impl AktorWorkerContract for GroupWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = CleanupData;
}

#[wasm_bindgen]
pub async fn start_group_setup_worker(mode: String) -> Result<(), JsValue> {
    let mut transport = options();
    if mode == "limits" {
        transport.capacity = core::num::NonZeroUsize::MAX;
    }

    worker::serve_setup::<GroupWorker>(transport, move |()| AktorServe {
        start: aktor::AktorInit::Try(async move || {
            if mode == "crash" {
                crash_initializer();
                core::future::pending::<()>().await;
            }
            if mode == "typed_error" {
                return Err((
                    CleanupData {
                        code: 42,
                        option: Some(None),
                    },
                    String::from("wrong storage key"),
                ));
            }
            Ok::<u32, (CleanupData, String)>(0_u32)
        }),
        end: (),
        hooks: aktor::AktorHooks::NONE,
    })
    .await
    .er_report()?;

    Ok(())
}

async fn managed_worker(
    group: &mut AktorGroup,
    name: &str,
    url: &str,
    prepared: bool,
) -> worker::Worker<GroupWorker> {
    if prepared {
        let spawned = group.spawn(group_setup::<GroupWorker>(
            name,
            &format!("{url}?configured"),
        ));

        spawned.ready().await.unwrap()
    } else {
        group
            .spawn(crate::worker_setup::<GroupWorker, _>(
                name,
                url,
                (),
                options(),
                aktor::Required,
            ))
            .ready()
            .await
            .unwrap()
    }
}

#[wasm_bindgen]
pub async fn worker_failure_check(
    url: String,
    prepared: bool,
    same_worker: bool,
) -> Result<bool, JsValue> {
    let mut group = AktorGroup::with_grace(Duration::from_millis(100));
    group.start().unwrap();
    let primary = managed_worker(&mut group, "primary", &url, prepared).await;
    let secondary = if same_worker {
        None
    } else {
        Some(managed_worker(&mut group, "secondary", &url, prepared).await)
    };
    let draining = secondary.as_ref().unwrap_or(&primary);
    let mut reply = operation(draining, 10).send().await.unwrap();

    while !draining.executing() {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    let rejection = aktor::dispatch::Transport::<u32, BadInput, ()>::request(
        &primary,
        aktor::operation::Operation {
            name: operation::NAME,
            caller: core::panic::Location::caller(),
        },
        BadInput("primary worker sentinel"),
    );
    assert!(rejection.await.is_err());
    release_gate();

    let report = group.stop().await;
    assert!(
        matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "primary")
    );
    assert_eq!(
        report
            .to_string()
            .matches("primary worker sentinel")
            .count(),
        1,
        "{report}"
    );
    let label = if same_worker { "primary" } else { "secondary" };
    let [_, secondary] = report.failures.as_slice() else {
        return Err(JsValue::from_str(&report.to_string()));
    };
    assert!(
        matches!(&secondary.source, aktor::AktorFailureSource::Actor { name, .. } if name == label)
    );
    assert!(
        secondary.error.contains("secondary worker sentinel"),
        "{report}"
    );
    assert_eq!(
        report
            .to_string()
            .matches("secondary worker sentinel")
            .count(),
        1
    );
    assert!(reply.try_take().unwrap().is_err());
    assert!(reply.try_take().is_none());
    assert!(operation(draining, 0).await.is_err());
    assert_eq!(
        group.completion().wait().await.unwrap().to_string(),
        report.to_string()
    );
    drop(reply);
    Ok(true)
}

thread_local! {
    static REJECTED_DECODES: Cell<usize> = const { Cell::new(0) };
    static COUNTED_DECODES: Cell<usize> = const { Cell::new(0) };
}

pub(crate) struct BadInput(pub &'static str);
impl AktorData for BadInput {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Err(DataError::new(self.0))
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Err(DataError::new("invalid input"))
    }
}

struct CountedOutput(Rc<Cell<usize>>);
impl AktorData for CountedOutput {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        Err(DataError::new("decode only probe"))
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        let _result: Result<u32, String> = reader.read()?;
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
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "deferred",
            &url,
            (),
            options(),
            aktor::Required,
        ))
        .ready()
        .await
        .unwrap();
    let mut counted = aktor::dispatch::Transport::<u32, (u32,), CountedOutput>::request(
        &worker,
        aktor::operation::Operation {
            name: operation::NAME,
            caller: core::panic::Location::caller(),
        },
        (0_u32,),
    )
    .send()
    .await
    .unwrap();
    let mut reply = bad_output(&worker).send().await.unwrap();
    let retained = if drop_worker {
        None
    } else {
        Some(worker.clone())
    };
    drop(worker);

    let published = group.stop().await;
    assert!(published.failed(), "{published}");
    let error = reply.try_take().unwrap().err().unwrap();
    assert!(
        published
            .failure(error.failure_id.unwrap())
            .unwrap()
            .delivered()
    );
    assert!(reply.try_take().is_none());
    assert!(!published.ran_past_grace());
    assert!(published.failures[0].error.contains("output refused"));
    assert_eq!(
        completed.wait().await.unwrap().to_string(),
        published.to_string()
    );
    assert_eq!(REJECTED_DECODES.with(Cell::get), 1);
    assert_eq!(COUNTED_DECODES.with(Cell::get), 1);
    assert_eq!(counted.try_take().unwrap().unwrap().0.get(), 1);
    assert_eq!(COUNTED_DECODES.with(Cell::get), 1);
    drop(retained);
    Ok(true)
}

#[wasm_bindgen]
pub async fn optional_worker_check(url: String, crash: bool) -> Result<bool, JsValue> {
    let group = crate::group();
    let (_, worker) = group
        .spawn(optional_setup::<GroupWorker>("archive", &url))
        .into_parts()
        .unwrap();

    worker
        .ready()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;

    if crash {
        let result = operation(&worker, 1).await;

        assert_eq!(
            result.unwrap_err().outcome,
            aktor::message::AktorDeliveryOutcome::OutcomeUnknown
        );
        worker.terminate();
        assert!(matches!(
            worker.completion().wait_retired().await,
            Ok(aktor::AktorRetired::Terminated(_))
        ));
    } else {
        assert_eq!(
            operation(&worker, 0).await.unwrap(),
            Err("ordinary operation error".into())
        );
        let confirmed = match worker.shutdown().await {
            Ok(aktor::AktorRetired::Released(result)) => {
                result.is_err() == url.contains("fail_cleanup=true")
            }
            Err(_) | Ok(aktor::AktorRetired::Terminated(_)) => false,
        };

        assert!(confirmed);
    }

    Ok(true)
}

#[derive(AktorData)]
struct TrapOnDrop;
struct TrapWorker;
impl AktorWorkerContract for TrapWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = TrapOnDrop;
}

impl Drop for TrapOnDrop {
    fn drop(&mut self) {
        panic!("cleanup error destructor");
    }
}

struct RetainedGuard;
impl Drop for RetainedGuard {
    fn drop(&mut self) {
        note_retained_drop();
    }
}

const ROLLBACK_REPORT: &str = "Setup rollback finished\n`- storage rollback diagnostic";

struct BrokenReadyWorker;
impl AktorWorkerContract for BrokenReadyWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = BrokenData;
    type Error = core::convert::Infallible;
}

struct UnconfirmedWorker;
impl AktorWorkerContract for UnconfirmedWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = u32;
}

#[wasm_bindgen]
pub async fn start_unconfirmed_worker(mode: u8) -> Result<(), JsValue> {
    if mode == 7 {
        worker::serve_setup::<TrapWorker>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || 0u32),
            end: async |_| Err((TrapOnDrop, String::from("database flush failed"))),
            hooks: AktorHooks::NONE,
        })
        .await
        .er_report()?;

        return Ok(());
    }
    if mode == 6 {
        worker::serve_setup::<BrokenReadyWorker>(options(), |()| AktorServe {
            start: aktor::AktorInit::Ready(async || AktorReady {
                state: 0u32,
                ready: BrokenData,
            }),
            end: async |_: u32| {
                wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();
            },
            hooks: AktorHooks::NONE,
        })
        .await
        .er_report()?;

        return Ok(());
    }

    worker::serve_setup::<UnconfirmedWorker>(options(), move |()| AktorServe {
        start: aktor::AktorInit::Try(async move || {
            if matches!(mode, 0 | 1 | 5) {
                let guard = RetainedGuard;
                return Err(worker::ServeFailure::Pending {
                    error: (mode != 5).then_some(u32::from(mode)),
                    report: String::from(if mode == 5 {
                        "rollback pending"
                    } else {
                        "setup still owns work"
                    }),
                    finish: Box::pin(async move {
                        let _guard = guard;
                        if mode != 5 {
                            core::future::pending::<()>().await;
                        }
                        wasm_bindgen_futures::JsFuture::from(gate()).await.unwrap();
                        Err((85_u32, ROLLBACK_REPORT.to_owned()))
                    }),
                });
            }
            Ok(0_u32)
        }),
        end: async move |_: u32| {
            if mode == 4 || mode < 3 {
                if mode == 4 {
                    break_worker_post_once();
                }
                let guard = RetainedGuard;
                return Err(worker::ServeFailure::Pending {
                    error: None,
                    report: String::from("cleanup still owns work"),
                    finish: Box::pin(async move {
                        let _guard = guard;
                        if mode < 3 {
                            core::future::pending::<()>().await;
                        }
                        wasm_bindgen_futures::JsFuture::from(cleanup_gate())
                            .await
                            .unwrap();
                        Ok(())
                    }),
                });
            }
            wasm_bindgen_futures::JsFuture::from(cleanup_gate())
                .await
                .unwrap();
            Ok(())
        },
        hooks: AktorHooks::NONE,
    })
    .await
    .er_report()?;

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
    let spawned = group.spawn(AktorWorkerNew {
        name: "unconfirmed owner".into(),
        kind: AktorKind::BrowserWebWorker::<UnconfirmedWorker>(program),
        config: (),
        options: options(),
        availability: aktor::Required,
        before_shutdown: None,
        stuck: if mode >= 3 {
            AktorStuck::Leave
        } else {
            AktorStuck::Kill
        },
    });
    let starting = spawned.ready().await;

    if mode == 4 {
        let worker = starting.unwrap();
        let completion = worker.completion();
        assert!(worker.shutdown().await.is_err());

        release_worker();
        assert!(matches!(
            completion.wait_retired().await,
            Ok(aktor::AktorRetired::Released(Err(_)))
        ));
        completion.wait_cleanup().await.unwrap();
        return;
    }

    if mode == 3 {
        let worker = starting.unwrap();
        let reply = operation(&worker, 4).send().await.unwrap();
        while !worker.executing() {
            gloo_timers::future::TimeoutFuture::new(1).await;
        }

        fail_transport();
        let failure = reply.await.unwrap_err();
        assert_eq!(
            failure.outcome,
            message::AktorDeliveryOutcome::OutcomeUnknown
        );

        worker.terminate();
        assert!(retained_owner());
        assert!(worker.outstanding().1 > 0);
        release_worker();
        match worker.completion().wait_retired().await {
            Ok(aktor::AktorRetired::Released(Err(_))) => {}
            other => panic!("late owner retirement: {other:?}"),
        }
        worker.completion().wait_cleanup().await.unwrap();
        assert_eq!(worker.outstanding().1, 0);
        return;
    }

    if mode == 2 {
        let worker = starting.unwrap();
        let error = match worker.shutdown().await {
            Err((error, _)) | Ok(aktor::AktorRetired::Terminated(Err((error, _)))) => error,
            _ => panic!("cleanup did not finish"),
        };
        assert_eq!(error.cause, worker::WorkerCause::Cleanup);
        assert!(error.data.is_none());
        assert_eq!(
            worker.completion().wait_cleanup().await.unwrap_err().0,
            worker::WorkerCleanupError::Unconfirmed(error.failure_id)
        );

        worker.terminate();
        let _report = group.stop().await;
        assert!(matches!(
            worker.completion().wait_retired().await,
            Ok(aktor::AktorRetired::Terminated(_))
        ));
    } else {
        let (failure, report) = starting.err().expect("unconfirmed setup was ready");
        assert!(
            matches!(failure, aktor::AktorStartError::Initializer { error, .. } if error == u32::from(mode)),
            "{report}"
        );
    }

    // The group's report says it, a `Kill` worker that still owned work got terminated.
    let report = group.completion().wait().await.unwrap();
    let owner = report
        .actors
        .iter()
        .find(|actor| actor.name == "unconfirmed owner")
        .unwrap();
    assert_eq!(
        owner.retirement(),
        Some(AktorRetirement::Terminated),
        "{report}"
    );
    assert!(!retained_owner());
}

async fn pending_setup(url: &str) {
    use aktor::{AktorKind, AktorRetirement, AktorStuck, AktorWorkerNew};

    let program = url.replace("group-worker.js", "gate-worker.js");
    let invalid = format!("{program}?unconfirmed=6");
    let (release, joining) = tokio::sync::oneshot::channel();
    let group = crate::group();
    let (_, worker) = group
        .spawn(AktorWorkerNew {
            before_shutdown: Some(worker::join(async move {
                joining.await.unwrap();
                worker::WorkerJoinOutcome::Stopped(Ok(()))
            })),
            ..optional_setup::<BrokenReadyWorker>("broken receipt", &invalid)
        })
        .into_parts()
        .unwrap();
    assert_eq!(
        worker.ready().await.unwrap_err().0.failure().cause,
        WorkerCause::Codec
    );
    assert!(matches!(
        worker.completion().cleanup(),
        aktor::AktorCleanup::Pending
    ));

    release.send(()).unwrap();
    release_worker();
    assert!(matches!(
        worker.completion().wait_retired().await,
        Ok(aktor::AktorRetired::Released(Err(_)))
    ));

    worker.completion().wait_cleanup().await.unwrap();

    let mut trapped = AktorGroup::with_grace(Duration::from_millis(100));
    trapped.start().unwrap();
    let setup =
        optional_setup::<UnconfirmedWorker>("cleanup trap", &format!("{program}?unconfirmed=7"));
    let worker = trapped.spawn(setup).ready().await.unwrap();
    worker.ready().await.unwrap();
    let ended = worker.shutdown();
    assert!(
        ended
            .wait_cleanup()
            .await
            .unwrap_err()
            .1
            .contains("database flush failed")
    );
    let (crash, _) = ended.report().await.unwrap_err();
    assert_eq!(crash.cause, WorkerCause::Crashed);
    let _stopped = trapped.stop().await;

    let failed = format!("{program}?unconfirmed=5");
    let mut optional = AktorGroup::with_grace(Duration::from_millis(100));
    optional.start().unwrap();
    let worker = optional
        .spawn(optional_setup::<UnconfirmedWorker>("rollback", &failed))
        .ready()
        .await
        .unwrap();
    let completion = worker.completion();
    let (early, _) = worker.ready().await.unwrap_err();
    assert!(
        matches!(early, worker::WorkerStartError::Failed(failure) if failure.evidence == aktor::AktorInitEvidence::Entered)
    );
    assert!(completion.wait().await.is_err());

    fail_transport();
    release_worker();
    let Ok(aktor::AktorRetired::Released(Err((error, _)))) = completion.wait_retired().await else {
        panic!("rollback did not confirm retirement");
    };
    assert_eq!(error.data, Some(85));
    assert_ne!(early.failure().failure_id, error.failure_id);
    assert!(core::ptr::eq(early, worker.ready().await.unwrap_err().0));

    let report = optional.stop().await;
    assert!(
        report
            .failure(error.failure_id.unwrap())
            .unwrap()
            .error
            .contains("storage rollback diagnostic")
    );
    assert!(
        report
            .failures
            .iter()
            .any(|row| !row.delivered() && row.error.contains("transport probe"))
    );

    let mut group = AktorGroup::with_grace(Duration::from_millis(2));
    group.start().unwrap();
    let completion = group.completion();
    let start = AktorWorkerNew {
        name: "pending setup".into(),
        kind: AktorKind::BrowserWebWorker::<UnconfirmedWorker>(failed),
        config: (),
        options: options(),
        availability: aktor::Required,
        before_shutdown: None,
        stuck: AktorStuck::Leave,
    };
    group.spawn(start).ready().await.err().unwrap();
    assert_eq!(
        completion.wait().await.unwrap().actors[0].retirement(),
        Some(AktorRetirement::StillRunning)
    );

    release_worker();
    let retired = completion.wait_retired().await.unwrap().into_report();
    let diagnostics = &retired.failures;
    assert_eq!(
        diagnostics
            .iter()
            .filter(|entry| entry.error.contains("storage rollback diagnostic"))
            .count(),
        1
    );
}

async fn leave_queued(url: &str, fail_output: bool) {
    use aktor::*;

    let mut group = AktorGroup::with_grace(Duration::from_millis(5));
    group.start().unwrap();
    let (release, joining) = tokio::sync::oneshot::channel();

    let spawned = group.spawn(AktorWorkerNew {
        name: "storage".into(),
        kind: AktorKind::BrowserWebWorker::<GroupWorker>(format!(
            "{}?configured",
            url.replace("group-worker.js", "gate-worker.js")
        )),
        config: (),
        options: options(),
        availability: aktor::Required,
        before_shutdown: Some(worker::join(async move {
            joining.await.unwrap();
            worker::WorkerJoinOutcome::Stopped(Ok(()))
        })),
        stuck: AktorStuck::Leave,
    });
    let worker = spawned.ready().await.unwrap();
    let running = operation(&worker, 4).send().await.unwrap();
    let rejected = if fail_output {
        Some(bad_output(&worker).send().await.unwrap())
    } else {
        None
    };
    let queued = operation(&worker, 7).send().await.unwrap();

    let stopped = group.stop().await;
    assert!(!stopped.released());
    release_worker();
    release.send(()).unwrap();

    assert_eq!(running.await.unwrap(), Ok(4));
    if let Some(rejected) = rejected {
        assert!(rejected.await.is_err());
    }
    assert_eq!(queued.await.unwrap(), Ok(7));

    let report = group.stop_released().await.unwrap().into_report();
    assert!(report.released());
    assert_eq!(report.to_string().contains("output refused"), fail_output);
}

#[derive(Er)]
struct CloseErr;

struct OwnedJoin(Rc<Cell<u32>>);
impl Drop for OwnedJoin {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

fn group_setup<C>(name: &str, program: &str) -> aktor::AktorWorkerNew<C>
where
    C: AktorWorkerContract<Config = ()>,
{
    crate::worker_setup::<C, _>(name, program, (), options(), aktor::Required)
}

fn optional_setup<C: AktorWorkerContract<Config = ()>>(
    name: &str,
    program: &str,
) -> aktor::AktorWorkerNew<C, aktor::Optional> {
    crate::worker_setup::<C, _>(name, program, (), options(), aktor::Optional)
}

#[wasm_bindgen]
pub async fn optional_group_check(url: String) -> Result<bool, JsValue> {
    use aktor::*;

    let mut group = AktorGroup::with_grace(Duration::from_millis(100));
    group.start().unwrap();

    let setup = group_setup::<GroupWorker>;
    let configured = format!("{url}?configured");
    for during_call in [false, true] {
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: optional_setup::<GroupWorker>("archive", &configured),
            shutdown: |_| {},
            grace: Duration::from_millis(100),
        });
        started.ready().await.unwrap();
        started.handles().ready().await.unwrap();

        let received = if during_call {
            let error = operation(started.handles(), 1).await.unwrap_err();
            Some(error.failure_id.unwrap())
        } else {
            fail_transport();
            None
        };

        let report = started.stop().await;
        assert_eq!(report.stop_cause, StopCause::Requested);
        if let Some(id) = received {
            assert!(report.failure(id).unwrap().delivered());
            report.into_result().unwrap();
        } else {
            assert!(report.into_result().is_err());
        }
    }

    for (query, expected_ran) in [("limits", false), ("crash", true)] {
        let program = format!("{configured}&mode={query}");
        let optional = group
            .spawn(optional_setup::<GroupWorker>(query, &program))
            .ready()
            .await
            .unwrap();
        let (error, _) = optional.ready().await.unwrap_err();
        assert!(matches!(
            (expected_ran, error),
            (true, worker::WorkerStartError::Failed(_))
                | (false, worker::WorkerStartError::Refused(_))
        ));

        let attempt = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: setup(query, &program),
            shutdown: |_| {},
            grace: Duration::from_millis(100),
        });
        let (failure, _) = attempt.ready().await.err().unwrap();
        let expected = if expected_ran {
            AktorStartReason::Init
        } else {
            AktorStartReason::InvalidSetup
        };
        assert_eq!(failure.reason(), expected);
        let actor = &failure.startup().unwrap().actors[0];
        assert_eq!(Some(actor.id), attempt.handles().actor_id());
        assert_eq!(
            actor.init,
            if expected_ran {
                aktor::AktorInitEvidence::Unknown
            } else {
                aktor::AktorInitEvidence::NeverEntered
            }
        );
    }

    break_post();
    let optional = group
        .spawn(optional_setup::<GroupWorker>(
            "initialize post",
            &configured,
        ))
        .ready()
        .await
        .unwrap();
    let error = optional.ready().await.unwrap_err().0;
    restore_post();
    assert!(matches!(error, worker::WorkerStartError::Refused(_)));

    let spawned = group.spawn((
        setup("preferences", &configured),
        optional_setup::<GroupWorker>("unavailable", "/missing-worker.js"),
    ));
    let (preferences, unavailable) = spawned.ready().await.unwrap();

    assert_eq!(
        operation(&unavailable, 42).await.unwrap_err().outcome,
        aktor::message::AktorDeliveryOutcome::NotAdmitted
    );

    let rollback = format!("{url}?rollback");
    let mut invalid = optional_setup::<RejectedWorker>("invalid", &rollback);
    let drops = Rc::new(Cell::new(0));
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    invalid.options.capacity = core::num::NonZeroUsize::MAX;
    invalid.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
        worker::WorkerJoinOutcome::Stopped(Ok(()))
    }));

    let invalid = group.spawn(invalid).ready().await.unwrap();
    let error = operation(&invalid, 42).await.unwrap_err();
    let completion = invalid.completion();
    drop(invalid);

    assert_eq!(drops.get(), 0);
    assert!(completion.wait().await.is_err());

    release.send(()).unwrap();
    match completion.wait_retired().await {
        Ok(aktor::AktorRetired::Released(Err(failure))) => {
            assert_eq!(failure.0.failure_id, error.failure_id)
        }
        other => panic!("construction retirement: {other:?}"),
    }
    assert_eq!(drops.get(), 1);

    let archive = group
        .spawn(optional_setup::<GroupWorker>("archive", &configured))
        .ready()
        .await
        .unwrap();
    let _failure = operation(&archive, 1).await;

    let (release, joining) = tokio::sync::oneshot::channel();
    let mut rejected = optional_setup::<RejectedWorker>("rejected", &rollback);
    rejected.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        worker::WorkerJoinOutcome::Stopped(Ok(()))
    }));
    let rejected = group.spawn(rejected).ready().await.unwrap();
    let error = operation(&rejected, 42)
        .await
        .er(|| std::fmt::Error)
        .unwrap_err();
    let completion = rejected.completion();

    assert!(completion.wait().await.is_err());
    assert_eq!(operation(&preferences, 42).await.unwrap(), Ok(42));
    group.killswitch().stop();
    gloo_timers::future::TimeoutFuture::new(0).await;
    release.send(()).unwrap();

    let retired = completion.wait_retired().await;

    assert!(error.er_report().contains("storage rollback diagnostic"));
    assert!(matches!(retired, Ok(aktor::AktorRetired::Released(Err(_)))));

    let report = group.completion().await.unwrap();
    let rejected = report
        .actors
        .iter()
        .find(|actor| actor.name == "rejected")
        .unwrap();
    assert_eq!(rejected.retirement(), Some(AktorRetirement::Released));

    let mut required = AktorGroup::new();
    required.start().unwrap();
    let mut invalid = setup("required", &configured);
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();
    invalid.options.capacity = core::num::NonZeroUsize::MAX;
    invalid.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
        finished.send(()).unwrap();
        worker::WorkerJoinOutcome::Stopped(Err(er_new!(
            aktor::AktorCleanupError,
            er_new!(CloseErr, std::fmt::Error)
        )
        .er_split()))
    }));

    let attempt = required.spawn(invalid);
    let (error, _) = attempt.ready().await.err().unwrap();

    assert_eq!(
        error.startup().unwrap().report.as_ref().unwrap().actors[0].retirement(),
        Some(AktorRetirement::StillRunning)
    );
    assert_eq!(drops.get(), 1);
    release.send(()).unwrap();
    joined.await.unwrap();
    assert_eq!(drops.get(), 2);

    let retired = required
        .completion()
        .wait_retired()
        .await
        .unwrap()
        .into_report();
    let failure = retired
        .failures
        .iter()
        .map(|failure| &failure.error)
        .find(|error| error.contains("CloseErr"))
        .unwrap();
    assert!(failure.contains(&std::fmt::Error.to_string()));

    for mode in 0..5 {
        unconfirmed_owner(&url, mode).await;
    }
    pending_setup(&url).await;

    let typed_program = format!("{configured}&mode=typed_error");
    let attempt = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorWorkerNew {
            stuck: AktorStuck::Leave,
            ..setup("turso", &typed_program)
        },
        shutdown: |_| {},
        grace: Duration::from_millis(100),
    });
    let (failure, report) = attempt.ready().await.err().unwrap();
    assert!(
        matches!(
            failure,
            aktor::AktorStartError::Initializer { error: data, .. } if data.code == 42 && data.option == Some(None)
        ),
        "{report}"
    );
    assert_eq!(
        failure.startup().unwrap().report.as_ref().unwrap().actors[0].retirement(),
        Some(AktorRetirement::Released),
        "{report}"
    );
    assert!(report.contains("wrong storage key"));

    leave_queued(&url, false).await;
    leave_queued(&url, true).await;

    for mode in 0..2 {
        unstarted_join(&configured, mode).await;
    }

    Ok(true)
}

async fn unstarted_join(url: &str, mode: u8) {
    let setup = |name| group_setup::<GroupWorker>(name, url);
    let mut group = AktorGroup::new();
    let mut unstarted = optional_setup::<GroupWorker>("unstarted dependency", url);
    let drops = Rc::new(Cell::new(0));
    let guard = OwnedJoin(drops.clone());
    let (release, joining) = tokio::sync::oneshot::channel();
    let (finished, joined) = tokio::sync::oneshot::channel();
    unstarted.before_shutdown = Some(worker::join(async move {
        joining.await.unwrap();
        drop(guard);
        finished.send(()).unwrap();
        worker::WorkerJoinOutcome::Stopped(Ok(()))
    }));

    if mode == 0 {
        // The group never started, so spawn refuses it before launching anything.
        assert!(group.spawn(unstarted).ready().await.is_err());
    } else {
        group.start().unwrap();
        let mut invalid = setup("first setup fails");
        invalid.options.capacity = core::num::NonZeroUsize::MAX;

        let entries = (invalid, (setup("skipped"), unstarted));
        assert!(group.spawn(entries).ready().await.is_err());
    }

    assert_eq!(drops.get(), 0);
    release.send(()).unwrap();
    joined.await.unwrap();
    assert_eq!(drops.get(), 1);

    if mode != 0 {
        let _report = group.stop().await;
    }
}

#[wasm_bindgen]
pub async fn late_retirement_check(url: String, startup: bool) -> Result<bool, JsValue> {
    let group = crate::group();
    let completion = if startup {
        let (_, worker) = group
            .spawn(crate::worker_setup::<GatedWorker, _>(
                "gated",
                &url,
                7,
                options(),
                aktor::Optional,
            ))
            .into_parts()
            .unwrap();
        wasm_bindgen_futures::JsFuture::from(configured()).await?;
        let completion = worker.completion();

        drop(group.stop());
        completion
    } else {
        let (_, worker) = group
            .spawn(optional_setup::<GroupWorker>("archive", &url))
            .into_parts()
            .unwrap();
        worker.ready().await.unwrap();

        let reply = operation(&worker, 4).send().await.unwrap();
        fail_transport();
        let _result = reply.await;
        let completion = worker.completion();

        drop(group.stop());
        completion
    };

    release_worker();

    Ok(matches!(
        completion.wait_retired().await,
        Ok(aktor::AktorRetired::Released(Err(_)))
    ))
}

#[wasm_bindgen]
pub async fn shutdown_dependency_check(url: String, mode: u8) -> Result<bool, JsValue> {
    let (release, joining) = tokio::sync::oneshot::channel();
    let url = if mode == 3 {
        format!("{url}?fail_cleanup=true")
    } else {
        url
    };
    let group = crate::group();
    let worker = group
        .spawn(aktor::AktorWorkerNew {
            before_shutdown: Some(worker::join(async move {
                joining.await.unwrap();

                if mode == 4 {
                    return worker::WorkerJoinOutcome::Unconfirmed(String::from(
                        "archive still running",
                    ));
                }
                worker::WorkerJoinOutcome::Stopped(if mode == 2 {
                    Err((
                        aktor::AktorCleanupError,
                        String::from("external cleanup finished with an error"),
                    ))
                } else {
                    Ok(())
                })
            })),
            ..optional_setup::<GroupWorker>("storage", &url)
        })
        .ready()
        .await
        .unwrap();
    worker.ready().await.unwrap();

    let completion = worker.completion();
    assert!(matches!(completion.cleanup(), aktor::AktorCleanup::Pending));

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

    if mode == 4 {
        assert!(completion.wait_retired().await.is_err());
        assert!(matches!(
            completion.retired(),
            aktor::AktorRetiredStatus::Unconfirmed(_)
        ));
        worker.terminate();
        let snapshot = group.stop().await;
        return Ok(waited && !shutdown_started() && !snapshot.released());
    }

    while !shutdown_started() {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    let retirement = completion.wait_retired().await;
    match (mode, completion.cleanup()) {
        (1, aktor::AktorCleanup::Unconfirmed(_))
        | (3, aktor::AktorCleanup::Returned(Err(_)))
        | (0 | 2, aktor::AktorCleanup::Returned(Ok(()))) => {}
        other => panic!("unexpected cleanup {other:?}"),
    }

    if let Some(first) = first {
        let Ok(aktor::AktorRetired::Released(Err(late))) = &retirement else {
            panic!("worker did not confirm cleanup: {retirement:?}");
        };
        assert_eq!(late, &first);
        assert!(
            group
                .stop()
                .await
                .actor_failures(worker.actor_id().unwrap())
                .any(|row| row.error.contains("backup folder is read only"))
        );
    }

    if mode == 2 {
        let report = group.stop().await;
        assert!(
            report
                .actor_failures(worker.actor_id().unwrap())
                .any(|row| row
                    .error
                    .contains("external cleanup finished with an error"))
        );
    }

    Ok(waited
        && match retirement {
            Ok(aktor::AktorRetired::Released(Ok(()))) => mode == 0,
            Ok(aktor::AktorRetired::Released(Err(_))) => mode == 2 || mode == 3,
            Ok(aktor::AktorRetired::Terminated(_)) => mode == 1,
            Err(_) => false,
        })
}

#[wasm_bindgen]
pub async fn incompatible_owner_check(url: String, startup: bool) -> Result<bool, JsValue> {
    let mut options = options();
    options.build.push_str("-incompatible");
    let group = crate::group();
    let (_, worker) = group
        .spawn(crate::worker_setup::<GroupWorker, _>(
            "preflight",
            &url,
            (),
            options,
            aktor::Optional,
        ))
        .into_parts()
        .unwrap();
    if startup {
        wasm_bindgen_futures::JsFuture::from(configured()).await?;
        release_worker();
    }
    let (error, _) = worker.ready().await.unwrap_err();
    Ok(matches!(error, worker::WorkerStartError::Refused(_)))
}

struct RejectedWorker;
impl AktorWorkerContract for RejectedWorker {
    type State = u32;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = ();
}

#[wasm_bindgen]
pub async fn start_rejected_worker() -> Result<(), JsValue> {
    let _result = worker::serve_setup::<RejectedWorker>(options(), |()| AktorServe {
        start: aktor::AktorInit::Try(async || {
            Err::<u32, _>((
                (),
                String::from("setup rollback finished\n`- storage rollback diagnostic"),
            ))
        }),
        end: (),
        hooks: AktorHooks::NONE,
    })
    .await;

    Ok(())
}

#[wasm_bindgen]
pub async fn start_raw_unready_worker() -> Result<(), JsValue> {
    note_configured();
    wasm_bindgen_futures::JsFuture::from(gate()).await?;
    start_group_worker(false, false, false).await
}
