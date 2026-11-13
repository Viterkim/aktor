use aktor::*;
use futures_util::FutureExt;
use std::{
    cell::Cell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use wasm_bindgen::prelude::*;

struct Counter {
    value: Rc<Cell<u32>>,
}

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Rc<Cell<u32>> {
    counter.value.set(counter.value.get() + by);
    counter.value.clone()
}

#[aktor]
async fn wait(counter: &mut Counter, millis: u32) -> u32 {
    gloo_timers::future::TimeoutFuture::new(millis).await;
    counter.value.set(counter.value.get() + 1);
    counter.value.get()
}

#[wasm_bindgen]
pub async fn local_setup_check() -> Result<bool, JsValue> {
    browser_budget().await?;
    let value = Rc::new(Cell::new(0));
    let count = value.clone();
    let before = Rc::new(Cell::new(0));
    let log = before.clone();
    let cleaned = Rc::new(Cell::new(false));
    let end = cleaned.clone();
    let interval = Rc::new(Cell::new(false));
    let tick = interval.clone();
    let (closed, closing) = tokio::sync::oneshot::channel();
    let hook_cleaned = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "browser counter".into(),
            kind: AktorKind::BrowserLocal,
            start: async move || aktor_ready(Counter { value: count }),
            end: async move |_: Counter| {
                end.set(true);
                Ok::<_, String>(())
            },
            hooks: AktorHooks {
                intervals: vec![AktorInterval {
                    every: Duration::from_millis(5),
                    run: (async move |_: &mut Counter| {
                        tick.set(true);
                    })
                    .into(),
                }],
                before_each: Some(
                    (move |_: &mut Counter, _: operation::Operation| {
                        log.set(log.get() + 1);
                    })
                    .into(),
                ),
                after_each: None,
            },
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: move |report| {
            assert!(hook_cleaned.get());
            let _sent = closed.send(report);
        },
        grace: Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    let result = add(&actors.handles, 2).await;

    if !Rc::ptr_eq(&result, &value) || result.get() != 2 {
        return Err(JsValue::from_str("local state or output changed"));
    }

    let (sender, mut results) = add::latest(&actors.handles);

    sender.send(3);
    sender.send(5);

    let result = results
        .next()
        .await
        .ok_or_else(|| JsValue::from_str("latest ended early"))?;

    if result.get() != 7 {
        return Err(JsValue::from_str("latest did not coalesce"));
    }

    let timed = wait(&actors.handles, 20)
        .timeout(Duration::from_millis(1))
        .await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(JsValue::from_str(
            "browser timeout did not expire after admission",
        ));
    }

    if add(&actors.handles, 0).await.get() != 8 || !interval.get() || before.get() < 4 {
        return Err(JsValue::from_str(
            "timed operation or owner hooks did not run",
        ));
    }

    let report = actors.shutdown().await;
    let notified = closing
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    if report.to_string() != notified.to_string()
        || report.failed()
        || !cleaned.get()
        || report.actors[0].kind != AktorExecution::BrowserLocal
    {
        return Err(JsValue::from_str("browser local cleanup"));
    }

    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "supplied browser executor".into(),
            kind: AktorKind::Local::<local::clock::Browser>(|future| {
                wasm_bindgen_futures::spawn_local(future);
                Ok(())
            }),
            start: async || {
                Ok::<_, String>(Counter {
                    value: Rc::new(Cell::new(0)),
                })
            },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    let timed = wait(&actors.handles, 10)
        .timeout(Duration::from_millis(1))
        .await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(JsValue::from_str("supplied executor timeout"));
    }

    if add(&actors.handles, 0).await.get() != 1 {
        return Err(JsValue::from_str("supplied executor lost admitted work"));
    }

    let report = actors.shutdown().await;

    if report.failed() || report.actors[0].kind != AktorExecution::Local {
        return Err(JsValue::from_str("supplied executor cleanup"));
    }

    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "custom browser executor".into(),
            kind: AktorKind::Custom(
                local::clock::Browser,
                |future| {
                    wasm_bindgen_futures::spawn_local(future);
                    Ok(())
                },
                async |mut runner: AktorRunner<'_, Counter, 0, ()>| {
                    while let Some(call) = runner.next().await {
                        call.run().await;
                    }

                    Ok(())
                },
            ),
            start: async || {
                Ok::<_, String>(Counter {
                    value: Rc::new(Cell::new(0)),
                })
            },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    if add(&actors.handles, 3).await.get() != 3 {
        return Err(JsValue::from_str("custom browser call"));
    }

    let report = actors.shutdown().await;

    Ok(!report.failed() && report.actors[0].kind == AktorExecution::Custom)
}

#[aktor]
async fn increment(counter: &mut Cell<u32>, by: u32) -> u32 {
    counter.set(counter.get() + by);
    counter.get()
}

async fn browser_budget() -> Result<(), JsValue> {
    let pool = bevy_tasks::TaskPool::new();
    let mut failures = Vec::new();

    for (mode, name) in ["BrowserLocal", "BevyLocal", "BevyTask", "Local<Browser>"]
        .into_iter()
        .enumerate()
    {
        for stop in [false, true] {
            let count = Arc::new(AtomicU32::new(0));
            let counted = count.clone();
            let cleaned = Arc::new(AtomicU32::new(0));
            let cleanup = cleaned.clone();
            let (started, entered) = tokio::sync::oneshot::channel();
            let mut started = Some(started);
            let before = move |_: &mut Cell<u32>, _: operation::Operation| {
                counted.fetch_add(1, Ordering::Relaxed);

                if let Some(started) = started.take() {
                    let _sent = started.send(());
                }
            };
            let end = async move |state: Cell<u32>| {
                cleanup.fetch_add(1, Ordering::Relaxed);

                if state.get() == 4096 {
                    Ok(())
                } else {
                    Err(String::from("busy owner lost admitted work"))
                }
            };

            macro_rules! check {
                ($kind:expr) => {{
                    let actors = aktor_start(AktorSetup {
                        actors: AktorNew {
                            name: "browser budget".into(),
                            kind: $kind,
                            start: async || aktor_ready(Cell::new(0)),
                            end,
                            hooks: AktorHooks {
                                intervals: vec![],
                                before_each: Some(before.into()),
                                after_each: None,
                            },
                            capacity: 4096,
                            stuck: AktorStuck::Kill,
                        },
                        shutdown: |_| {},
                        grace: Duration::from_secs(5),
                    })
                    .await
                    .map_err(|(_, report)| JsValue::from_str(&report))?;

                    observe_budget(actors, entered, count, cleaned, stop, |handle| {
                        increment(handle, 1).send().now_or_never().is_some()
                    })
                    .await?
                }};
            }

            let observed = match mode {
                0 => check!(AktorKind::BrowserLocal),
                1 => check!(AktorKind::BevyLocal(&pool)),
                2 => check!(AktorKind::BevyTask(&pool)),
                _ => check!(AktorKind::Local::<local::clock::Browser>(|future| {
                    wasm_bindgen_futures::spawn_local(future);
                    Ok(())
                })),
            };

            if observed == 4096 {
                failures.push(format!(
                    "{name}, stop {stop}: timer waited for all {observed} calls"
                ));
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(JsValue::from_str(&failures.join("\n")))
    }
}

async fn observe_budget<H, G: setup::group::AktorSetupGroup>(
    actors: AktorStarted<H, G>,
    entered: tokio::sync::oneshot::Receiver<()>,
    count: Arc<AtomicU32>,
    cleaned: Arc<AtomicU32>,
    stop: bool,
    send: impl Fn(&H) -> bool,
) -> Result<u32, JsValue> {
    for _ in 0..4096 {
        if !send(&actors.handles) {
            let report = actors.shutdown().await;
            return Err(JsValue::from_str(&format!("busy admission: {report}")));
        }
    }

    entered
        .await
        .map_err(|_| JsValue::from_str("busy owner did not start"))?;
    gloo_timers::future::TimeoutFuture::new(0).await;
    let observed = count.load(Ordering::Relaxed);

    if stop {
        G::stop(&actors.killswitch());
    } else {
        while count.load(Ordering::Relaxed) < 4096 {
            gloo_timers::future::TimeoutFuture::new(0).await;
        }
    }

    let report = actors.shutdown().await;

    if report.failed() || cleaned.load(Ordering::Relaxed) != 1 {
        return Err(JsValue::from_str(&format!("busy cleanup: {report}")));
    }

    Ok(observed)
}

#[aktor]
async fn gated(counter: &mut Cell<u32>, gate: tokio::sync::oneshot::Receiver<()>) -> u32 {
    let _released = gate.await;
    counter.set(counter.get() + 1);
    counter.get()
}

#[wasm_bindgen]
pub async fn bevy_setup_check() -> Result<bool, JsValue> {
    let pool = bevy_tasks::TaskPool::new();
    let local_setup = AktorNew {
        name: "Bevy browser local".into(),
        kind: AktorKind::BevyLocal(&pool),
        start: async || {
            Ok::<_, String>(Counter {
                value: Rc::new(Cell::new(0)),
            })
        },
        end: (),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let task_setup = AktorNew {
        name: "Bevy browser task".into(),
        kind: AktorKind::BevyTask(&pool),
        start: async || aktor_ready(Cell::new(0)),
        end: async |counter: Cell<u32>| {
            if counter.get() == 7 {
                Ok::<_, String>(())
            } else {
                Err(String::from("Bevy task lost admitted work"))
            }
        },
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        actors: (local_setup, task_setup),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| JsValue::from_str(&report))?;

    if add(&actors.handles.0, 2).await.get() != 2 || increment(&actors.handles.1, 2).await != 2 {
        return Err(JsValue::from_str("Bevy ordinary calls"));
    }

    let (release, gate) = tokio::sync::oneshot::channel();
    let mut reply = gated(&actors.handles.1, gate).send().await;
    let timed = reply.timeout(Duration::from_millis(1)).await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(JsValue::from_str("Bevy browser timeout"));
    }

    drop(reply);
    release
        .send(())
        .map_err(|_| JsValue::from_str("Bevy operation was cancelled"))?;

    let (input, mut output) = increment::latest(&actors.handles.1);

    input.send(4);

    let report = actors.shutdown().await;

    if report.failed() || output.next().await != Some(7) || output.next().await.is_some() {
        return Err(JsValue::from_str("Bevy browser cleanup/latest"));
    }

    Ok(report
        .actors
        .iter()
        .any(|actor| actor.kind == AktorExecution::BevyLocal)
        && report
            .actors
            .iter()
            .any(|actor| actor.kind == AktorExecution::BevyTask))
}
