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
async fn park(_: &State, entered: Arc<AtomicBool>) {
    entered.store(true, Ordering::Release);
    core::future::pending().await
}

#[aktor]
async fn block(_: &State, entered: Arc<AtomicBool>, release: Arc<AtomicBool>) {
    entered.store(true, Ordering::Release);

    while !release.load(Ordering::Acquire) {
        core::hint::spin_loop();
    }
}

#[aktor]
async fn stop(_: &State, kill: AktorKillSwitch, fail: bool) {
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
async fn observe(_: &State, completion: AktorGroupCompletion) -> (bool, bool) {
    let report = completion.wait().await;
    (report.failed(), report.ran_past_grace)
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

fn open() -> Ready<Result<State, std::io::Error>> {
    ready(Ok(State(Rc::new(Cell::new(0)))))
}

fn close(state: State) -> std::io::Result<()> {
    CLEANED.fetch_add(1, Ordering::SeqCst);
    drop(state);
    Ok(())
}

fn setup(
    program: &str,
    name: &str,
    start: fn() -> Ready<Result<State, std::io::Error>>,
    end: fn(State) -> std::io::Result<()>,
) -> impl AktorDefinition<cross_core::Handle<State>, std::io::Error> {
    AktorNew {
        name: name.into(),
        kind: AktorKind::BrowserThread(program),
        start,
        end,
        hooks: Default::default(),
        capacity: 1,
        stuck: AktorStuck::Kill,
    }
}

#[wasm_bindgen]
pub async fn check(program: String) -> Result<String, JsValue> {
    let before = CLEANED.load(Ordering::SeqCst);
    let actors = aktor_start(AktorSetup {
        actors: (
            setup(&program, "first", open, close),
            setup(&program, "second", open, close),
        ),
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;
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
    let mut value = forward(first, second.clone(), value).await;

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

fn refused() -> Ready<Result<State, std::io::Error>> {
    ready(Err(std::io::Error::other("shared startup refused")))
}

fn bad_cleanup(state: State) -> std::io::Result<()> {
    drop(state);
    Err(std::io::Error::other("shared cleanup refused"))
}

#[aktor]
async fn crash(_: &State) {
    panic!("shared operation panicked");
}

#[wasm_bindgen]
pub async fn failures(program: String) -> Result<(), JsValue> {
    let refused = setup(&program, "refused", refused, close);
    let startup = aktor_start(AktorSetup {
        actors: refused,
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await;

    match startup {
        Err((error, _))
            if matches!(&error.init, AktorInitOutcome::Failed(e) if e.to_string() == "shared startup refused")
                && error.report.as_ref().is_some_and(|report| {
                    !report.ran_past_grace
                        && report.to_string().matches("shared startup refused").count() == 1
                }) => {}
        _ => {
            return Err(JsValue::from_str(
                "shared startup did not report completed rollback",
            ));
        }
    }

    let bad_program = "data:text/javascript,throw new Error('shared worker import refused')";
    let startup = aktor_start(AktorSetup {
        actors: setup(bad_program, "bootstrap error", open, close),
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await;

    match startup {
        Err((error, _))
            if matches!(&error.init, AktorInitOutcome::NotRun)
                && matches!(&error.cause, AktorStartCause::Setup(error) if error.diagnostics.contains("shared worker import refused")) =>
            {}
        _ => {
            return Err(JsValue::from_str("shared worker lost its bootstrap error"));
        }
    }

    for mode in 0..4 {
        let cleaned = CLEANED.load(Ordering::SeqCst);
        let end = if mode == 0 { bad_cleanup } else { close };
        let owner = setup(&program, "failure", open, end);

        if mode == 3 {
            track_thread();
        }

        let actors = aktor_start(AktorSetup {
            actors: owner,
            shutdown: |_| {},
            grace: std::time::Duration::from_millis(300),
        })
        .await;

        if mode == 3 {
            restore_worker();
        }

        let actors = actors.map_err(|(_, report)| JsValue::from_str(&report))?;
        let mut abandoned = None;
        let release = Arc::new(AtomicBool::new(false));

        if mode == 1 {
            crash(&actors.handles).cast().await;
        } else if mode == 2 {
            let entered = Arc::new(AtomicBool::new(false));
            abandoned = Some(park(&actors.handles, entered.clone()).send().await);

            while !entered.load(Ordering::Acquire) {
                gloo_timers::future::TimeoutFuture::new(1).await;
            }
        } else if mode == 3 {
            let entered = Arc::new(AtomicBool::new(false));
            abandoned = Some(
                hold(&actors.handles, entered.clone(), release.clone())
                    .send()
                    .await,
            );

            while !entered.load(Ordering::Acquire) {
                gloo_timers::future::TimeoutFuture::new(1).await;
            }

            fail_thread();
        }

        let report = actors.shutdown().await;

        if mode == 0
            && (report.to_string().matches("shared cleanup refused").count() != 1
                || report.failure.as_ref().is_none_or(|failure| {
                    failure.phase != "cleanup" || failure.message != "shared cleanup refused"
                }))
        {
            return Err(JsValue::from_str(&format!(
                "shared cleanup returned: {report}"
            )));
        }

        let expected = match mode {
            0 | 2 => AktorRetireStatus::Confirmed,
            _ => AktorRetireStatus::Unconfirmed,
        };
        release.store(true, Ordering::Release);

        if !report.failed()
            || report.ran_past_grace != (mode == 2)
            || report.actors[0].retirement != expected
            || (mode == 2
                && (CLEANED.load(Ordering::SeqCst) != cleaned + 1 || report.failure.is_some()))
        {
            return Err(JsValue::from_str(&format!(
                "shared failure mode {mode}: {report}"
            )));
        }

        drop(abandoned);
    }

    stuck_calls(&program).await
}

async fn stuck_calls(program: &str) -> Result<(), JsValue> {
    for stuck in [AktorStuck::Kill, AktorStuck::Leave] {
        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "stuck call".into(),
                kind: AktorKind::BrowserThread(program),
                start: open,
                end: (),
                hooks: Default::default(),
                capacity: 1,
                stuck,
            },
            shutdown: |_| {},
            grace: std::time::Duration::from_millis(100),
        })
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let reply = match stuck {
            AktorStuck::Kill => {
                block(&actors.handles, entered.clone(), release.clone())
                    .send()
                    .await
            }
            AktorStuck::Leave => {
                hold(&actors.handles, entered.clone(), release.clone())
                    .send()
                    .await
            }
        };

        while !entered.load(Ordering::Acquire) {
            gloo_timers::future::TimeoutFuture::new(1).await;
        }

        let report = actors.shutdown().await;
        release.store(true, Ordering::Release);

        if report.actors[0].retirement != AktorRetireStatus::Unconfirmed {
            return Err(JsValue::from_str(&format!(
                "stuck call retired early: {report}"
            )));
        }

        if stuck == AktorStuck::Leave {
            reply.await;
        }

        actors
            .completion()
            .wait_retired()
            .await
            .map_err(|(_, report)| JsValue::from_str(&report))?;
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
        actors: setup(&program, "cancelled bootstrap", open, close),
        shutdown: |_| {},
        grace: std::time::Duration::from_millis(300),
    });
    let kill = startup.killswitch();
    let (result, ()) = futures_util::future::join(startup, async move {
        gloo_timers::future::TimeoutFuture::new(10).await;
        kill.stop();
    })
    .await;

    match result {
        Err((error, _)) if error.report.as_ref().is_some_and(|report| !report.failed()) => {
            dropped_startup(&program).await
        }
        _ => Err(JsValue::from_str(
            "unstarted shared worker did not cancel before its deadline",
        )),
    }
}

#[wasm_bindgen]
pub async fn controls(program: String) -> Result<(), JsValue> {
    let observer = aktor_start(AktorSetup {
        actors: setup(&program, "observer", open, close),
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    gloo_timers::future::TimeoutFuture::new(500).await;

    for (fail, delay) in [(false, 40), (true, 40), (false, 1000)] {
        let cleaned = Arc::new(AtomicBool::new(false));
        let finished = cleaned.clone();
        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "control".into(),
                kind: AktorKind::BrowserThread(&program),
                start: open,
                end: async move |state| {
                    gloo_timers::future::TimeoutFuture::new(delay).await;
                    drop(state);
                    finished.store(true, Ordering::Release);
                },
                hooks: Default::default(),
                capacity: 1,
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: std::time::Duration::from_millis(300),
        })
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
        let completion = actors.group.completion();
        let observed = observe(&observer.handles, completion.clone()).send().await;
        stop(&actors.handles, actors.group.killswitch(), fail).await;
        let report = completion.wait().await;
        let observed = observed.await;
        let overdue = delay == 1000;

        if overdue {
            let late = completion
                .wait_retired()
                .await
                .map_err(|(_, report)| JsValue::from_str(&report))?;
            if late.actors[0].retirement != AktorRetireStatus::Terminated {
                return Err(JsValue::from_str(&format!(
                    "overdue cleanup retired: {late}"
                )));
            }
        }

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

async fn dropped_startup(program: &str) -> Result<(), JsValue> {
    let entered = Arc::new(AtomicBool::new(false));
    let starting = entered.clone();
    let mut startup = Box::pin(aktor_start(AktorSetup {
        actors: AktorNew {
            name: "dropped startup".into(),
            kind: AktorKind::BrowserThread(program),
            start: async move || {
                starting.store(true, Ordering::Release);
                open().await
            },
            end: close,
            hooks: Default::default(),
            capacity: 1,
            stuck: AktorStuck::Leave,
        },
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    }));
    let completion = startup.completion();

    core::future::poll_fn(|cx| {
        let _ = startup.as_mut().poll(cx);
        core::task::Poll::Ready(())
    })
    .await;

    let deadline = clock() + 5000.0;
    while !entered.load(Ordering::Acquire) && clock() < deadline {
        gloo_timers::future::TimeoutFuture::new(1).await;
    }

    drop(startup);

    if !entered.load(Ordering::Acquire) {
        return Err(JsValue::from_str("shared initializer did not enter"));
    }

    let report = completion
        .wait_retired()
        .await
        .map_err(|(_, report)| JsValue::from_str(&report))?;
    if report.actors[0].retirement != AktorRetireStatus::Confirmed {
        return Err(JsValue::from_str(&format!(
            "dropped shared startup: {report}"
        )));
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
            name: "teardown probe".into(),
            kind: AktorKind::BrowserThread(&program),
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
            hooks: Default::default(),
            capacity: 1,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;
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
