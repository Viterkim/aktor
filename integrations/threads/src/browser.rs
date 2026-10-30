use aktor::*;
use core::{
    future::{Ready, ready},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{cell::Cell, rc::Rc, sync::Arc};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = "export function clock() { return performance.now(); }")]
extern "C" {
    fn clock() -> f64;
}

#[wasm_bindgen(inline_js = r#"
let previousWorker;
let trackedWorker;

export function trackThread() {
    previousWorker = globalThis.Worker;
    globalThis.Worker = class extends previousWorker {
        constructor(...args) {
            super(...args);
            trackedWorker = this;
        }
    };
}

export function restoreWorker() {
    globalThis.Worker = previousWorker;
}

export function failThread() {
    trackedWorker.dispatchEvent(new ErrorEvent('error', { message: 'early worker error' }));
}

export function delayedProbe(memory, address) {
    const view = new Uint32Array(memory.buffer, address, 2);
    const PreviousError = Error;
    globalThis.Error = class extends PreviousError {
        constructor(message, options) {
            super(message, options);
            if (message === 'actor worker already stopped') Atomics.add(view, 1, 1);
        }
    };
    let pending = Promise.resolve();

    for (let i = 0; i < 128; i++) pending = pending.then(() => {});

    return pending.then(() => {
        Atomics.store(view, 0, 1);
    });
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = delayedProbe)]
    fn delayed_probe(memory: JsValue, address: u32) -> js_sys::Promise;
    #[wasm_bindgen(js_name = trackThread)]
    fn track_thread();
    #[wasm_bindgen(js_name = restoreWorker)]
    fn restore_worker();
    #[wasm_bindgen(js_name = failThread)]
    fn fail_thread();
}

static CLEANED: AtomicUsize = AtomicUsize::new(0);

struct State(Rc<Cell<u32>>);

struct Payload {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[aktor]
async fn echo(_: &State, input: Payload) -> Payload {
    input
}

#[aktor]
async fn instance(_: &State) -> bool {
    wasm_bindgen::instance()
        .dyn_into::<js_sys::WebAssembly::Instance>()
        .is_ok()
}

#[aktor]
async fn forward(_: &State, other: cross_core::Handle<State>, input: Payload) -> Payload {
    echo(&other, input).await
}

#[aktor]
async fn set(state: &mut State, value: u32) -> u32 {
    state.0.set(value);
    value
}

#[aktor]
async fn hold(_: &State, entered: Arc<AtomicBool>, release: Arc<AtomicBool>) {
    entered.store(true, Ordering::Release);

    while !release.load(Ordering::Acquire) {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }
}

#[aktor]
async fn stop(_: &State, kill: KillSwitch, fail: bool) {
    if fail {
        kill.fail(AktorFailureReport {
            actor: "worker control".into(),
            kind: Some(AktorExecution::BrowserThread),
            phase: "call".into(),
            message: "worker requested failure".into(),
        });
    } else {
        kill.stop();
    }

    kill.wait_stopping().await;
}

#[aktor]
async fn observe(_: &State, completion: GroupCompletion) -> (bool, bool) {
    let report = completion.wait().await;
    (report.failed(), report.timed_out)
}

#[aktor]
async fn parallel(_: &State, entered: Arc<AtomicUsize>) -> Result<(), String> {
    entered.fetch_add(1, Ordering::SeqCst);
    let deadline = js_sys::Date::now() + 1500.0;

    while entered.load(Ordering::SeqCst) != 2 {
        if js_sys::Date::now() > deadline {
            return Err("owners could not run concurrently".into());
        }

        core::hint::spin_loop();
    }

    Ok(())
}

fn open() -> Ready<Result<State, AktorSetupError>> {
    ready(Ok(State(Rc::new(Cell::new(0)))))
}

fn close(state: State) -> Result<(), AktorCleanupError> {
    CLEANED.fetch_add(1, Ordering::SeqCst);
    drop(state);
    Ok(())
}

type Setup = AktorNew<
    State,
    fn() -> Ready<Result<State, AktorSetupError>>,
    AktorKind::BrowserThread,
    (),
    fn(State) -> Result<(), AktorCleanupError>,
>;

fn setup(program: &str, name: &str) -> Setup {
    AktorNew {
        name: AktorName::new(name),
        role: AktorNoRole,
        kind: AktorKind::BrowserThread(program),
        closures: AktorClosures {
            start: open,
            end: close,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: AktorNewOptions { capacity: 1 },
    }
}

#[wasm_bindgen]
pub async fn check(program: String) -> Result<String, JsValue> {
    let before = CLEANED.load(Ordering::SeqCst);
    let actors = aktor_start(AktorSetup {
        actors: (setup(&program, "first"), setup(&program, "second")),
        shutdown: |_| {},
        options: AktorOptions::default(),
    })
    .await
    .map_err(|error| JsValue::from_str(&format!("{error:#}")))?;
    let (first, second) = &actors.handles;
    if !instance(first).await {
        return Err(JsValue::from_str("worker lost its WebAssembly.Instance"));
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let value = Payload {
        bytes: vec![19; 2 * 1024 * 1024],
        drops: drops.clone(),
    };
    let pointer = value.bytes.as_ptr();
    let mut value = forward(first, second.new_handle(), value).await;

    for _ in 0..16 {
        value = echo(first, value).await;
    }

    let started = clock();

    for _ in 0..512 {
        value = echo(first, value).await;
    }

    let micros = (clock() - started) * 1000.0 / 512.0;

    if value.bytes.as_ptr() != pointer || value.bytes.iter().any(|byte| *byte != 19) {
        return Err(JsValue::from_str("owned payload was copied or corrupted"));
    }

    drop(value);

    if drops.load(Ordering::SeqCst) != 1 {
        return Err(JsValue::from_str(
            "owned payload was not dropped exactly once",
        ));
    }

    let entered = Arc::new(AtomicUsize::new(0));
    let (left, right) =
        futures_util::future::join(parallel(first, entered.clone()), parallel(second, entered))
            .await;
    left.and(right).map_err(|error| JsValue::from_str(&error))?;

    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let mut running = hold(first, entered.clone(), release.clone()).send().await;

    while !entered.load(Ordering::Acquire) {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    let admitted = running.timeout(std::time::Duration::from_millis(1)).await;
    let queued = set(first, 77).send().await;
    let pending = set(first, 88)
        .timeout(std::time::Duration::from_millis(1))
        .await;

    if !admitted.is_err_and(|error| error.admitted) || !pending.is_err_and(|error| !error.admitted)
    {
        return Err(JsValue::from_str(
            "shared call timeout lost its admission status",
        ));
    }

    for _ in 0..1000 {
        let (sender, results) = set(first, 9000).latest();
        drop((sender, results));
    }

    let (sender, mut results) = set(first, 1).latest();

    for value in 2..=1000 {
        sender.send(value);
    }

    drop(sender);
    let closing = actors.shutdown();
    release.store(true, Ordering::Release);
    running.await;
    queued.await;

    if results.next().await != Some(1000) || results.next().await.is_some() {
        return Err(JsValue::from_str(
            "shared latest lost its final replacement during shutdown",
        ));
    }

    let report = closing.await;

    if report.failed() || CLEANED.load(Ordering::SeqCst) != before + 2 {
        return Err(JsValue::from_str(&format!("shared cleanup: {report}")));
    }

    Ok(format!(
        "2 MiB owned round trip {micros:.2} us, same allocation, worker-to-worker calls, parallel owners, latest and cleanup passed"
    ))
}

fn refused() -> Ready<Result<State, AktorSetupError>> {
    ready(Err(AktorSetupError::new("shared startup refused", ())))
}

fn bad_cleanup(state: State) -> Result<(), AktorCleanupError> {
    drop(state);
    Err(AktorCleanupError::new("shared cleanup refused", ()))
}

#[aktor]
async fn crash(_: &State) {
    panic!("shared operation panicked");
}

#[wasm_bindgen]
pub async fn failures(program: String) -> Result<(), JsValue> {
    let mut refused = setup(&program, "refused");
    refused.closures.start = self::refused;
    let startup = aktor_start(AktorSetup {
        actors: refused,
        shutdown: |_| {},
        options: AktorOptions::default(),
    })
    .await;

    match startup {
        Err(error)
            if error
                .report
                .as_ref()
                .is_some_and(|report| report.failed() && !report.timed_out) => {}
        _ => {
            return Err(JsValue::from_str(
                "shared startup did not report completed rollback",
            ));
        }
    }

    for mode in 0..4 {
        let mut owner = setup(&program, "failure");

        if mode == 0 {
            owner.closures.end = bad_cleanup;
        }

        if mode == 3 {
            track_thread();
        }

        let actors = aktor_start(AktorSetup {
            actors: owner,
            shutdown: |_| {},
            options: AktorOptions {
                shutdown_grace: std::time::Duration::from_millis(300),
            },
        })
        .await;

        if mode == 3 {
            restore_worker();
        }

        let actors = actors.map_err(|error| JsValue::from_str(&format!("{error:#}")))?;
        let mut abandoned = None;
        let release = Arc::new(AtomicBool::new(false));

        if mode == 1 {
            crash(&actors.handles).cast().await;
        } else if mode >= 2 {
            let entered = Arc::new(AtomicBool::new(false));
            abandoned = Some(
                hold(&actors.handles, entered.clone(), release.clone())
                    .send()
                    .await,
            );

            if mode == 3 {
                while !entered.load(Ordering::Acquire) {
                    gloo_timers::future::TimeoutFuture::new(1).await;
                }

                fail_thread();
            }
        }

        let report = actors.shutdown().await;
        release.store(true, Ordering::Release);

        let expected = if mode == 0 {
            RetirementStatus::Confirmed
        } else {
            RetirementStatus::Unconfirmed
        };

        if !report.failed()
            || report.timed_out != (mode == 2)
            || report.actors[0].retirement != expected
        {
            return Err(JsValue::from_str(&format!(
                "shared failure mode {mode}: {report}"
            )));
        }

        drop(abandoned);
    }

    Ok(())
}

#[cfg(feature = "non-send")]
#[aktor]
async fn invalid(_: &State, input: Rc<u32>) -> Rc<u32> {
    input
}

#[cfg(feature = "non-send")]
async fn cannot_transfer(handle: &threads::Handle<State>) {
    invalid(handle, Rc::new(1)).await;
}

#[wasm_bindgen]
pub async fn bootstrap(program: String) -> Result<(), JsValue> {
    let startup = aktor_start(AktorSetup {
        actors: setup(&program, "cancelled bootstrap"),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: std::time::Duration::from_millis(300),
        },
    });
    let kill = startup.killswitch();
    let (result, ()) = futures_util::future::join(startup, async move {
        gloo_timers::future::TimeoutFuture::new(10).await;
        kill.stop();
    })
    .await;

    match result {
        Err(error)
            if error
                .report
                .as_ref()
                .is_some_and(|report| !report.timed_out) =>
        {
            Ok(())
        }
        _ => Err(JsValue::from_str(
            "unstarted shared worker did not cancel before its deadline",
        )),
    }
}

#[wasm_bindgen]
pub async fn controls(program: String) -> Result<(), JsValue> {
    let observer = aktor_start(AktorSetup {
        actors: setup(&program, "observer"),
        shutdown: |_| {},
        options: AktorOptions::default(),
    })
    .await
    .map_err(|error| JsValue::from_str(&format!("{error:#}")))?;

    gloo_timers::future::TimeoutFuture::new(500).await;

    for (fail, delay) in [(false, 40), (true, 40), (false, 1000)] {
        let cleaned = Arc::new(AtomicBool::new(false));
        let finished = cleaned.clone();
        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("control"),
                role: AktorNoRole,
                kind: AktorKind::BrowserThread(&program),
                closures: AktorClosures {
                    start: open,
                    end: async move |state| {
                        gloo_timers::future::TimeoutFuture::new(delay).await;
                        drop(state);
                        finished.store(true, Ordering::Release);
                    },
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 1 },
            },
            shutdown: |_| {},
            options: AktorOptions {
                shutdown_grace: std::time::Duration::from_millis(300),
            },
        })
        .await
        .map_err(|error| JsValue::from_str(&format!("{error:#}")))?;
        let completion = actors.group.completion();
        let observed = observe(&observer.handles, completion.clone()).send().await;
        stop(&actors.handles, actors.group.killswitch(), fail).await;
        let report = completion.wait().await;
        let observed = observed.await;
        let overdue = delay == 1000;

        if observed != (fail || overdue, overdue) || cleaned.load(Ordering::Acquire) == overdue {
            return Err(JsValue::from_str(&format!(
                "cross-worker controls: {report}"
            )));
        }
    }

    let report = observer.shutdown().await;
    if report.failed() {
        return Err(JsValue::from_str(&format!("observer cleanup: {report}")));
    }

    Ok(())
}

#[wasm_bindgen]
pub async fn teardown_probe(program: String) -> Result<(), JsValue> {
    use core::sync::atomic::AtomicU32;

    let markers = Arc::new([AtomicU32::new(0), AtomicU32::new(0)]);
    let retained = markers.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("teardown probe"),
            role: AktorNoRole,
            kind: AktorKind::BrowserThread(&program),
            closures: AktorClosures {
                start: open,
                end: move |state| {
                    drop(state);
                    let retained = retained.clone();
                    let address = retained.as_ptr() as u32;
                    let pending = delayed_probe(wasm_bindgen::memory(), address);
                    wasm_bindgen_futures::spawn_local(async move {
                        let _ = wasm_bindgen_futures::JsFuture::from(pending).await;
                        drop(retained);
                    });
                },
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 1 },
        },
        shutdown: |_| {},
        options: AktorOptions::default(),
    })
    .await
    .map_err(|error| JsValue::from_str(&format!("{error:#}")))?;
    let report = actors.shutdown().await;
    let deadline = clock() + 1000.0;

    while (markers[0].load(Ordering::Acquire) == 0 || markers[1].load(Ordering::Acquire) == 0)
        && clock() < deadline
    {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    if report.failed()
        || markers[0].load(Ordering::SeqCst) != 1
        || markers[1].load(Ordering::SeqCst) == 0
    {
        return Err(JsValue::from_str(
            "worker teardown did not block its detached callback",
        ));
    }

    Ok(())
}
