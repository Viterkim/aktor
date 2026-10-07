use aktor::*;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{cell::Cell, rc::Rc, time::Duration};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = "
export function note_setup() {
    postMessage({ proof: 'setup' });
}

export function note_end() {
    postMessage({ proof: 'end' });
}
")]
extern "C" {
    fn note_setup();
    fn note_end();
}

#[derive(Serialize, Deserialize)]
struct Config {
    initial: u32,
    fail_start: bool,
    fail_end: bool,
    setup_delay: u32,
}

struct Counter {
    value: Rc<Cell<u32>>,
    before: usize,
    after: usize,
    interval: bool,
}

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, bool> {
    if by == 0 {
        return Err(false);
    }

    counter.value.set(counter.value.get() + by);
    Ok(counter.value.get())
}

#[aktor]
async fn inspect(counter: &Counter) -> (u32, usize, usize, bool) {
    (
        counter.value.get(),
        counter.before,
        counter.after,
        counter.interval,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Buffer {
    #[serde(with = "aktor::worker::bytes")]
    data: Vec<u8>,
}

#[aktor]
async fn echo(_: &Counter, buffer: Buffer) -> Buffer {
    buffer
}

#[derive(AktorData, Debug, Clone, PartialEq, Eq)]
struct DataBuffer {
    data: Vec<u8>,
    #[aktor(skip)]
    cache: String,
}

#[aktor(data)]
async fn echo_data(_: &Counter, mut buffer: DataBuffer) -> Result<DataBuffer, String> {
    if !buffer.cache.is_empty() {
        return Err("local cache crossed the worker boundary".into());
    }

    buffer.cache = "worker cache".into();
    Ok(buffer)
}

#[aktor]
async fn bytes(counter: &Counter, size: u32) -> Vec<u8> {
    vec![counter.value.get() as u8; size as usize]
}

#[aktor(data)]
async fn checksum_data(_: &Counter, buffer: DataBuffer) -> u64 {
    buffer.data.iter().map(|byte| *byte as u64).sum()
}

#[aktor]
async fn wait(counter: &mut Counter, millis: u32) -> u32 {
    gloo_timers::future::TimeoutFuture::new(millis).await;
    counter.value.set(counter.value.get() + 1);
    counter.value.get()
}

#[wasm_bindgen]
pub async fn start_legacy_setup_worker() -> Result<(), JsValue> {
    worker::serve(Counter {
        value: Rc::new(Cell::new(0)),
        before: 0,
        after: 0,
        interval: false,
    })
    .map_err(error)?
    .wait()
    .await
    .map_err(error)
}

#[wasm_bindgen]
pub async fn start_setup_worker(build: String) -> Result<(), JsValue> {
    worker::serve_setup(
        AktorNoRole,
        worker::Options {
            build,
            ..Default::default()
        },
        |config: Config| {
            note_setup();
            let start = async move || {
                gloo_timers::future::TimeoutFuture::new(config.setup_delay).await;

                if config.fail_start {
                    return Err(AktorSetupError::new("counter setup failed"));
                }

                Ok(Counter {
                    value: Rc::new(Cell::new(config.initial)),
                    before: 0,
                    after: 0,
                    interval: false,
                })
            };
            AktorClosures {
                start,
                end: Some(
                    (async move |counter: Counter| {
                        note_end();

                        if counter.before != counter.after || config.fail_end {
                            return Err(AktorCleanupError::new("counter cleanup failed"));
                        }

                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![AktorInterval {
                    every: Duration::from_millis(5),
                    run: (async |counter: &mut Counter| {
                        counter.interval = true;
                    })
                    .into(),
                }],
                before_each: Some(
                    (|counter: &mut Counter, _: operation::Operation| {
                        counter.before += 1;
                    })
                    .into(),
                ),
                after_each: Some(
                    (|counter: &mut Counter, _: operation::Operation| {
                        counter.after += 1;
                    })
                    .into(),
                ),
            }
        },
    )
    .await
    .map_err(error)?
    .wait()
    .await
    .map_err(error)
}

#[wasm_bindgen]
pub async fn remote_preflight_check(program: String) -> Result<bool, JsValue> {
    let config = Config {
        initial: 7,
        fail_start: false,
        fail_end: false,
        setup_delay: 0,
    };

    for mode in 0..4 {
        let mut options = worker::Options {
            build: "config-v1".into(),
            ..Default::default()
        };

        match mode {
            0 => options.build = "config-v2".into(),
            1 => options.capacity += 1,
            2 => options.max_outstanding_bytes += 1,
            _ => {}
        }

        if mode == 3 {
            let client =
                worker::Worker::<u32>::with_config(&program, options, &config).map_err(error)?;

            if client.ready().await.is_ok() {
                return Err(error("mismatched operations passed preflight"));
            }
        } else {
            let client = worker::Worker::<Counter>::with_config(&program, options, &config)
                .map_err(error)?;

            if client.ready().await.is_ok() {
                return Err(error("mismatched worker passed preflight"));
            }
        }
    }

    let client = worker::Worker::<Counter>::with_config(
        &program,
        worker::Options {
            build: "config-v1".into(),
            ..Default::default()
        },
        &config,
    )
    .map_err(error)?;

    client.ready().await.map_err(error)?;
    assert_eq!(add(&client, 1).await, Ok(8));
    client.shutdown().await.map_err(error)?;
    Ok(true)
}

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("{error:#}"))
}

fn setup(program: &str, config: Config) -> AktorWorkerNew<Counter, Config> {
    AktorWorkerNew {
        name: AktorName::new("remote counter"),
        role: AktorNoRole,
        kind: AktorKind::BrowserWebWorker(program),
        config,
        options: Default::default(),
    }
}

#[wasm_bindgen]
pub async fn remote_setup_check(program: String) -> Result<bool, JsValue> {
    let mut group = AktorGroup::new();
    let completed = group.start().map_err(error)?;
    let handles = aktor_start_in(
        &group,
        aktor_setups! {
            first: setup(&program, Config {
                initial: 17,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            }),
            second: setup(&program, Config {
                initial: 23,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            }),
        },
    )
    .await
    .map_err(error)?;

    assert!(!group.killswitch().is_stopping());
    assert_eq!(add(&handles.first, 3).await, Ok(20));
    assert_eq!(add(&handles.second, 4).await, Ok(27));

    let report = group.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors.len(), 2);
    assert_eq!(completed.await.actors.len(), 2);

    let terminated = aktor_start(AktorSetup {
        actors: setup(
            &program,
            Config {
                initial: 0,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .map_err(error)?;
    terminated.handles.terminate();
    let report = terminated.completion().wait().await;

    assert!(!report.startup);

    if !report.failed() || report.timed_out || report.actors[0].timed_out {
        return Err(error(format!(
            "explicit termination was marked as a timeout: {report}"
        )));
    }
    assert!(!report.actors[0].diagnostics.is_empty());

    let actors = aktor_start(AktorSetup {
        actors: setup(
            &program,
            Config {
                initial: 2,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    })
    .await
    .map_err(error)?;

    if add(&actors.handles, 3).await != Ok(5) || add(&actors.handles, 0).await != Err(false) {
        return Err(error("remote calls changed their domain results"));
    }

    let timed = wait(&actors.handles, 20)
        .timeout(Duration::from_millis(1))
        .await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(error("remote timeout did not expire after admission"));
    }

    let (value, before, after, interval) = inspect(&actors.handles).await;

    if value != 6 || before != after + 1 || !interval {
        return Err(error("remote state, hooks or interval did not run"));
    }

    assert_eq!(
        bytes(&actors.handles, 256 * 1024).await,
        vec![6; 256 * 1024]
    );

    for size in [17, 768 * 1024, 0, 5 * 1024 * 1024, 31, 256 * 1024] {
        assert_eq!(bytes(&actors.handles, size).await, vec![6; size as usize]);
    }

    let (large_input, mut large_output) = bytes::latest(&actors.handles);

    large_input.send(256 * 1024);
    assert_eq!(large_output.next().await, Some(vec![6; 256 * 1024]));
    drop((large_input, large_output));

    let buffer = Buffer {
        data: (0..256 * 1024).map(|value| value as u8).collect(),
    };

    assert_eq!(echo(&actors.handles, buffer.clone()).await, buffer);

    let input = DataBuffer {
        data: buffer.data.clone(),
        cache: "parent cache".into(),
    };
    let expected = DataBuffer {
        data: input.data.clone(),
        cache: String::new(),
    };

    assert_eq!(
        echo_data(&actors.handles, input.clone()).await,
        Ok(expected.clone())
    );

    let reply = echo_data(&actors.handles, input.clone()).send().await;
    assert_eq!(reply.await, Ok(expected.clone()));

    let reply = echo_data(&actors.handles, input.clone()).send().await;
    assert_eq!(reply.await, Ok(expected.clone()));

    let (data_sender, mut data_results) = echo_data(&actors.handles, input).latest();
    assert_eq!(data_results.next().await, Some(Ok(expected.clone())));

    data_sender.send(DataBuffer {
        data: vec![37; 32],
        cache: "another cache".into(),
    });
    assert_eq!(
        data_results.next().await,
        Some(Ok(DataBuffer {
            data: vec![37; 32],
            cache: String::new()
        }))
    );
    for size in [0, 768 * 1024, 13, 256 * 1024] {
        let data: Vec<_> = (0..size).map(|index| index as u8).collect();

        data_sender.send(DataBuffer {
            data: data.clone(),
            cache: String::new(),
        });
        assert_eq!(
            data_results.next().await,
            Some(Ok(DataBuffer {
                data,
                cache: String::new(),
            }))
        );
    }

    drop(data_sender);
    assert_eq!(data_results.next().await, None);

    for size in [768 * 1024, 0, 1_048_576, 12] {
        let data: Vec<_> = (0..size).map(|index| index as u8).collect();
        let expected = data.iter().map(|byte| *byte as u64).sum::<u64>();

        assert_eq!(
            checksum_data(
                &actors.handles,
                DataBuffer {
                    data,
                    cache: String::new(),
                },
            )
            .await,
            expected
        );
    }

    let (input, mut output) = echo::latest(&actors.handles);

    input.send(buffer.clone());
    assert_eq!(output.next().await, Some(buffer));
    drop((input, output));

    let (input, mut output) = add::latest(&actors.handles);

    input.send(4);

    let report = actors.shutdown().await;

    if report.failed()
        || report.actors[0].kind != Some(AktorExecution::BrowserWebWorker)
        || output.next().await != Some(Ok(10))
        || output.next().await.is_some()
    {
        return Err(error(format!("remote shutdown failed: {report}")));
    }

    drop(input);

    let actors = aktor_start(AktorSetup {
        actors: setup(
            &program,
            Config {
                initial: 0,
                fail_start: false,
                fail_end: true,
                setup_delay: 0,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    })
    .await
    .map_err(error)?;

    let completion = actors.completion();
    let (input, mut output) = add::latest(&actors.handles);

    input.send(3);

    let report = actors.shutdown().await;

    if output.next().now_or_never() != Some(Some(Ok(3))) {
        return Err(error(
            "remote cleanup failure hid a completed latest result",
        ));
    }

    drop(input);

    if !report.failed()
        || !format!("{report}").contains("counter cleanup failed")
        || format!("{}", completion.wait().await) != format!("{report}")
    {
        return Err(error("remote cleanup report was lost"));
    }

    let failed = aktor_start(AktorSetup {
        actors: (
            setup(
                &program,
                Config {
                    initial: 0,
                    fail_start: false,
                    fail_end: true,
                    setup_delay: 0,
                },
            ),
            setup(
                &program,
                Config {
                    initial: 0,
                    fail_start: true,
                    fail_end: false,
                    setup_delay: 0,
                },
            ),
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    })
    .await
    .err()
    .ok_or_else(|| error("remote setup failure was lost"))?;

    let report = failed
        .report
        .ok_or_else(|| error("remote rollback report was lost"))?;

    if report.timed_out
        || !report.to_string().contains("counter setup failed")
        || !report.to_string().contains("counter cleanup failed")
    {
        return Err(error(format!("remote rollback failed: {report}")));
    }

    let legacy = program.replace("setup_worker.js", "setup_legacy_worker.js");

    if aktor_start(AktorSetup {
        actors: setup(
            &legacy,
            Config {
                initial: 2,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    })
    .await
    .is_ok()
    {
        return Err(error(
            "configuration was silently ignored by a legacy worker",
        ));
    }

    let mut startup = Box::pin(aktor_start(AktorSetup {
        actors: setup(
            &program,
            Config {
                initial: 0,
                fail_start: false,
                fail_end: false,
                setup_delay: 10_000,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    }));

    let completion = startup.completion();

    if startup.as_mut().now_or_never().is_some() {
        return Err(error("remote setup finished before its delay"));
    }

    let kill = startup.killswitch();

    kill.stop();

    if startup.await.is_ok() {
        return Err(error("stopping remote startup returned a handle"));
    }

    let report = completion.wait().await;

    if !report.timed_out
        || !report.actors[0].timed_out
        || report.actors[0].kind != Some(AktorExecution::BrowserWebWorker)
    {
        return Err(error("stopping remote startup lost its cleanup report"));
    }

    let mut startup = Box::pin(aktor_start(AktorSetup {
        actors: setup(
            &program,
            Config {
                initial: 0,
                fail_start: false,
                fail_end: false,
                setup_delay: 10_000,
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(300),
        },
    }));

    let completion = startup.completion();

    assert!(startup.as_mut().now_or_never().is_none());
    drop(startup);

    let report = completion.wait().await;

    Ok(report.timed_out
        && report.actors[0].timed_out
        && report.actors[0].kind == Some(AktorExecution::BrowserWebWorker))
}
