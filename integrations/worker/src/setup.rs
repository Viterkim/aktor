use aktor::*;
use std::{cell::Cell, rc::Rc, time::Duration};
use wasm_bindgen::prelude::*;

struct Counter {
    value: Rc<Cell<u32>>,
}

mod aktors {
    pub struct Counter;
}

#[aktor(role = aktors::Counter)]
async fn add(counter: &mut Counter, by: u32) -> Rc<Cell<u32>> {
    counter.value.set(counter.value.get() + by);
    counter.value.clone()
}

#[aktor(role = aktors::Counter)]
async fn wait(counter: &mut Counter, millis: u32) -> u32 {
    gloo_timers::future::TimeoutFuture::new(millis).await;
    counter.value.set(counter.value.get() + 1);
    counter.value.get()
}

#[wasm_bindgen]
pub async fn local_setup_check() -> Result<bool, JsValue> {
    let value = Rc::new(Cell::new(0));
    let count = value.clone();
    let before = Rc::new(Cell::new(0));
    let log = before.clone();
    let cleaned = Rc::new(Cell::new(false));
    let end = cleaned.clone();
    let interval = Rc::new(Cell::new(false));
    let tick = interval.clone();
    let actors = start(AktorSetup {
        name: AktorName::new("browser counter"),
        role: aktors::Counter,
        kind: AktorKind::BrowserLocal,
        closures: AktorClosures {
            start: async move || Ok(Counter { value: count }),
            end: Some(
                (async move |_: Counter| {
                    end.set(true);
                    Ok(())
                })
                .into(),
            ),
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
        options: None,
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

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

    if report.failed()
        || !cleaned.get()
        || report.actors[0].kind != Some(AktorExecution::BrowserLocal)
    {
        return Err(JsValue::from_str("browser local cleanup"));
    }

    let actors = start(AktorSetup {
        name: AktorName::new("supplied browser executor"),
        role: aktors::Counter,
        kind: AktorKind::Local::<local::clock::Browser>(|future| {
            wasm_bindgen_futures::spawn_local(future);
            Ok(())
        }),
        closures: AktorClosures {
            start: async || {
                Ok(Counter {
                    value: Rc::new(Cell::new(0)),
                })
            },
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

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

    if report.failed() || report.actors[0].kind != Some(AktorExecution::Local) {
        return Err(JsValue::from_str("supplied executor cleanup"));
    }

    let actors = start(AktorSetup {
        name: AktorName::new("custom browser executor"),
        role: aktors::Counter,
        kind: AktorKind::Custom(
            local::clock::Browser,
            |future| {
                wasm_bindgen_futures::spawn_local(future);
                Ok(())
            },
            async |mut runner: AktorRunner<'_, Counter>| {
                while let Some(call) = runner.next().await {
                    call.run().await;
                }

                Ok(())
            },
        ),
        closures: AktorClosures {
            start: async || {
                Ok(Counter {
                    value: Rc::new(Cell::new(0)),
                })
            },
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    })
    .await
    .map_err(|error| JsValue::from_str(&error.to_string()))?;

    if add(&actors.handles, 3).await.get() != 3 {
        return Err(JsValue::from_str("custom browser call"));
    }

    let report = actors.shutdown().await;

    Ok(!report.failed() && report.actors[0].kind == Some(AktorExecution::Custom))
}

#[aktor]
async fn increment(counter: &mut Cell<u32>, by: u32) -> u32 {
    counter.set(counter.get() + by);
    counter.get()
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
    let local_setup = AktorSetup {
        name: AktorName::new("Bevy browser local"),
        role: aktors::Counter,
        kind: AktorKind::BevyLocal(&pool),
        closures: AktorClosures {
            start: async || {
                Ok(Counter {
                    value: Rc::new(Cell::new(0)),
                })
            },
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let task_setup = AktorSetup {
        name: AktorName::new("Bevy browser task"),
        role: AktorNoRole,
        kind: AktorKind::BevyTask(&pool),
        closures: AktorClosures {
            start: async || Ok(Cell::new(0)),
            end: Some(
                (async |counter: Cell<u32>| {
                    if counter.get() == 7 {
                        Ok(())
                    } else {
                        Err(AktorError::new("Bevy task lost admitted work"))
                    }
                })
                .into(),
            ),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let actors = start(aktor_setups! { local: local_setup, task: task_setup })
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;

    if add(&actors.handles.local, 2).await.get() != 2
        || increment(&actors.handles.task, 2).await != 2
    {
        return Err(JsValue::from_str("Bevy ordinary calls"));
    }

    let (release, gate) = tokio::sync::oneshot::channel();
    let mut reply = gated(&actors.handles.task, gate).send().await;
    let timed = reply.timeout(Duration::from_millis(1)).await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(JsValue::from_str("Bevy browser timeout"));
    }

    drop(reply);
    release
        .send(())
        .map_err(|_| JsValue::from_str("Bevy operation was cancelled"))?;

    let (input, mut output) = increment::latest(&actors.handles.task);

    input.send(4);

    let report = actors.shutdown().await;

    if report.failed() || output.next().await != Some(7) || output.next().await.is_some() {
        return Err(JsValue::from_str("Bevy browser cleanup/latest"));
    }

    Ok(report
        .actors
        .iter()
        .any(|actor| actor.kind == Some(AktorExecution::BevyLocal))
        && report
            .actors
            .iter()
            .any(|actor| actor.kind == Some(AktorExecution::BevyTask)))
}
