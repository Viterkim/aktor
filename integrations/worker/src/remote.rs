use aktor::{
    data::{DataError, Reader, Writer},
    dispatch::DataCodec,
    latest::{SendLatest, Session},
    operation::Operation,
    *,
};
use er::*;
use futures_util::FutureExt;
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

#[derive(Clone, AktorData)]
struct Config {
    initial: u32,
    fail_start: bool,
    fail_end: bool,
    setup_delay: u32,
}

struct RejectedInput(bool);
impl AktorData for RejectedInput {
    fn encode_data(&self, _: &mut Writer) -> Result<(), DataError> {
        if self.0 {
            Ok(())
        } else {
            Err(DataError::new("invalid input"))
        }
    }

    fn decode_data(_: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(Self(true))
    }
}

struct Counter {
    value: Rc<Cell<u32>>,
    before: usize,
    after: usize,
    interval: bool,
}
impl AktorWorkerContract for Counter {
    type State = Self;
    type Role = ();
    type Config = Config;
    type Ready = u32;
    type Error = CounterErr;
}

#[aktor(latest)]
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

#[derive(Debug, Clone, PartialEq, Eq, AktorData)]
struct Buffer {
    data: Vec<u8>,
}

#[aktor(latest)]
async fn echo(_: &Counter, buffer: Buffer) -> Buffer {
    buffer
}

#[derive(AktorData, Debug, Clone, PartialEq, Eq)]
struct DataBuffer {
    data: Vec<u8>,
    #[aktor(skip)]
    cache: String,
}

#[derive(AktorData, Clone, PartialEq)]
struct Packed {
    flags: Vec<bool>,
    values: Vec<f64>,
    bytes: [u8; 16],
    switches: [bool; 9],
}

#[aktor]
async fn packed(_: &Counter, value: Packed) -> Packed {
    value
}

#[aktor(latest)]
async fn echo_data(_: &Counter, mut buffer: DataBuffer) -> Result<DataBuffer, String> {
    if !buffer.cache.is_empty() {
        return Err("local cache crossed the worker boundary".into());
    }

    buffer.cache = "worker cache".into();
    Ok(buffer)
}

#[aktor(latest)]
async fn bytes(counter: &Counter, size: u32) -> Vec<u8> {
    vec![counter.value.get() as u8; size as usize]
}

#[aktor]
async fn checksum_data(_: &Counter, buffer: DataBuffer) -> u64 {
    buffer.data.iter().map(|byte| *byte as u64).sum()
}

#[aktor]
async fn wait(counter: &mut Counter, millis: u32) -> u32 {
    gloo_timers::future::TimeoutFuture::new(millis).await;
    counter.value.set(counter.value.get() + 1);
    counter.value.get()
}

#[derive(Er, AktorData)]
enum CounterErr {
    SetupErr { count: u32 },
    CloseErr { count: u32 },
}

#[wasm_bindgen]
pub async fn start_setup_worker(build: String) -> Result<(), JsValue> {
    worker::serve_setup::<Counter>(crate::options(&build), |config| {
        note_setup();
        let start = async move || -> ErResult<_, CounterErr> {
            if config.setup_delay != 0 {
                crate::group::note_configured();
            }

            gloo_timers::future::TimeoutFuture::new(config.setup_delay).await;

            if config.fail_start {
                er_bail!(CounterErr::SetupErr {
                    count: config.initial
                });
            }

            let state = Counter {
                value: Rc::new(Cell::new(config.initial)),
                before: 0,
                after: 0,
                interval: false,
            };

            Ok(AktorReady {
                state,
                ready: config.initial,
            })
        };

        AktorServe {
            start: aktor::AktorInit::Try(start),
            end: move |counter: Counter| -> Result<(), (CounterErr, String)> {
                note_end();

                if counter.before != counter.after || config.fail_end {
                    er_bail!(
                        CounterErr::CloseErr {
                            count: counter.value.get()
                        },
                        [std::fmt::Error]
                    );
                }

                Ok(())
            },
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(Duration::from_millis(5), async |counter: &mut Counter| {
                        counter.interval = true;
                    })
                    .unwrap(),
                ],
                before_each: |counter: &mut Counter, _: operation::Operation| {
                    counter.before += 1;
                },
                after_each: |counter: &mut Counter, _: operation::Operation| {
                    counter.after += 1;
                },
            },
        }
    })
    .await
    .er_report()?;

    Ok(())
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
        let mut options = crate::options("config-v1");

        match mode {
            0 => options.build = "config-v2".into(),
            1 => options.capacity = options.capacity.checked_add(1).unwrap(),
            2 => {
                options.max_outstanding_bytes =
                    options.max_outstanding_bytes.checked_add(1).unwrap()
            }
            _ => {}
        }

        if mode == 3 {
            struct WrongState;
            impl AktorWorkerContract for WrongState {
                type State = u32;
                type Role = ();
                type Config = Config;
                type Ready = u32;
                type Error = CounterErr;
            }

            let mut group = AktorGroup::new();
            group.start().map_err(|(_, report)| error(report))?;
            let client = group.spawn(AktorWorkerNew {
                name: "wrong worker".into(),
                kind: AktorKind::BrowserWebWorker::<WrongState>(&program),
                config: config.clone(),
                options,
                availability: aktor::Required,
                before_shutdown: None,
                stuck: AktorStuck::Kill,
            });

            if client.ready().await.is_ok() {
                return Err(error(format!(
                    "mismatched operations passed preflight: counter={:?}, u32={:?}, registrations={}",
                    worker::Operations::for_actor::<Counter, ()>(),
                    worker::Operations::for_actor::<u32, ()>(),
                    worker::inventory::iter::<worker::Registration>
                        .into_iter()
                        .count(),
                )));
            }
        } else {
            let group = crate::group();
            let client = group.spawn(AktorWorkerNew {
                options,
                ..setup(&program, config.clone())
            });

            if client.ready().await.is_ok() {
                return Err(error("mismatched worker passed preflight"));
            }
        }
    }

    let group = crate::group();
    let client = group
        .spawn(AktorWorkerNew {
            options: crate::options("config-v1"),
            ..setup(&program, config)
        })
        .ready()
        .await
        .map_err(|(_, report)| error(report))?;
    assert_eq!(add(&client, 1).await.unwrap(), Ok(8));
    client
        .shutdown()
        .report()
        .await
        .map_err(|(_, report)| error(report))?;
    Ok(true)
}

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("{error:#}"))
}

fn setup(program: &str, config: Config) -> AktorWorkerNew<Counter> {
    AktorWorkerNew {
        name: "remote counter".into(),
        kind: AktorKind::BrowserWebWorker::<Counter>(program),
        config,
        options: crate::options(""),
        availability: aktor::Required,
        before_shutdown: None,
        stuck: AktorStuck::Kill,
    }
}

#[wasm_bindgen]
pub async fn remote_setup_check(program: String) -> Result<bool, JsValue> {
    let mut group = AktorGroup::new();
    let completed = group.start().map_err(|(_, report)| error(report))?;
    let spawned = group.spawn((
        setup(
            &program,
            Config {
                initial: 17,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            },
        ),
        setup(
            &program,
            Config {
                initial: 23,
                fail_start: false,
                fail_end: false,
                setup_delay: 0,
            },
        ),
    ));
    let handles = spawned.ready().await.map_err(|(_, report)| error(report))?;

    assert!(!group.killswitch().is_stopping());
    assert_eq!(handles.0.ready().await.unwrap(), &17);
    assert_eq!(add(&handles.0, 3).await.unwrap(), Ok(20));
    assert_eq!(add(&handles.1, 4).await.unwrap(), Ok(27));

    let report = group.stop().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors.len(), 2);
    assert_eq!(completed.await.unwrap().actors.len(), 2);

    let terminated = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_secs(5),
    });
    terminated
        .ready()
        .await
        .map_err(|(_, report)| error(report))?;
    terminated.handles().terminate();
    let report = terminated.completion().wait().await.unwrap();

    assert!(!matches!(
        report.stop_cause,
        aktor::StopCause::StartupFailure(_)
    ));

    if !report.failed() || report.ran_past_grace() || report.actors[0].ran_past_grace() {
        return Err(error(format!(
            "explicit termination was marked as a timeout: {report}"
        )));
    }
    assert_eq!(report.failures.len(), 1);

    for latest in [false, true] {
        for remote in [false, true] {
            for fail_end in [false, true] {
                let rejected = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: setup(
                        &program,
                        Config {
                            initial: 0,
                            fail_start: false,
                            fail_end,
                            setup_delay: 0,
                        },
                    ),
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                });
                rejected
                    .ready()
                    .await
                    .map_err(|(_, report)| error(report))?;
                let mut running = wait(rejected.handles(), 20).send().await.unwrap();
                let mut queued = add(rejected.handles(), 3).send().await.unwrap();
                let request = async {
                    if latest {
                        let (sender, mut output) = <&worker::Worker<Counter> as Session<
                            Counter,
                            RejectedInput,
                            u32,
                            (),
                            DataCodec,
                        >>::session(
                            rejected.handles(),
                            Operation {
                                name: add::NAME,
                                caller: std::panic::Location::caller(),
                            },
                            async |_: &mut Counter, _: RejectedInput| 0,
                        );

                        sender.send(RejectedInput(remote));
                        output.next().await.unwrap().unwrap_err()
                    } else {
                        aktor::dispatch::Transport::<Counter, RejectedInput, u32>::request(
                            rejected.handles(),
                            aktor::operation::Operation {
                                name: add::NAME,
                                caller: core::panic::Location::caller(),
                            },
                            RejectedInput(remote),
                        )
                        .await
                        .unwrap_err()
                    }
                };
                let delivery = request.await;
                let report = rejected.stop().await;
                let failure = report.failure(delivery.failure_id.unwrap()).unwrap();
                let first_line = failure.error.lines().next().unwrap();
                assert_eq!(report.to_string().matches(first_line).count(), 1);
                assert_eq!(running.try_take(), Some(Ok(1)));
                assert_eq!(queued.try_take(), Some(Ok(Ok(4))));
                assert!(!report.ran_past_grace());
                assert_eq!(report.failures.len(), 1 + usize::from(fail_end));

                if fail_end {
                    assert!(report.to_string().contains("CloseErr"));
                }
            }
        }
    }

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    });
    actors.ready().await.map_err(|(_, report)| error(report))?;

    if add(actors.handles(), 3).await.unwrap() != Ok(5)
        || add(actors.handles(), 0).await.unwrap() != Err(false)
    {
        return Err(error("remote calls changed their domain results"));
    }

    let timed = wait(actors.handles(), 20)
        .timeout(Duration::from_millis(1))
        .await;

    if !matches!(timed, Err(AktorTimeoutError { admitted: true, .. })) {
        return Err(error("remote timeout did not expire after admission"));
    }

    let (value, before, after, interval) = inspect(actors.handles()).await.unwrap();

    if value != 6 || before != after + 1 || !interval {
        return Err(error("remote state, hooks or interval did not run"));
    }

    assert_eq!(
        bytes(actors.handles(), 256 * 1024).await.unwrap(),
        vec![6; 256 * 1024]
    );

    for size in [17, 768 * 1024, 0, 5 * 1024 * 1024, 31, 256 * 1024] {
        assert_eq!(
            bytes(actors.handles(), size).await.unwrap(),
            vec![6; size as usize]
        );
    }

    let (large_input, mut large_output) = bytes::latest(actors.handles());

    large_input.send(256 * 1024);
    assert_eq!(
        large_output.next().await.transpose().unwrap(),
        Some(vec![6; 256 * 1024])
    );
    drop((large_input, large_output));

    let buffer = Buffer {
        data: (0..256 * 1024).map(|value| value as u8).collect(),
    };

    assert_eq!(
        echo(actors.handles(), buffer.clone()).await.unwrap(),
        buffer
    );

    let input = Packed {
        flags: (0..19).map(|value| value % 3 == 0).collect(),
        values: vec![0.0, -0.0, 0.75, f64::INFINITY],
        bytes: core::array::from_fn(|index| index as u8),
        switches: [true, false, true, false, true, false, true, false, true],
    };

    if packed(actors.handles(), input.clone()).await.unwrap() != input {
        return Err(error("packed data changed during the worker call"));
    }

    let input = DataBuffer {
        data: buffer.data.clone(),
        cache: "parent cache".into(),
    };
    let expected = DataBuffer {
        data: input.data.clone(),
        cache: String::new(),
    };

    assert_eq!(
        echo_data(actors.handles(), input.clone()).await.unwrap(),
        Ok(expected.clone())
    );

    let reply = echo_data(actors.handles(), input.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(reply.await.unwrap(), Ok(expected.clone()));

    let reply = echo_data(actors.handles(), input.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(reply.await.unwrap(), Ok(expected.clone()));

    let (data_sender, mut data_results) = echo_data(actors.handles(), input).latest();
    assert_eq!(
        data_results.next().await.transpose().unwrap(),
        Some(Ok(expected.clone()))
    );

    data_sender.send(DataBuffer {
        data: vec![37; 32],
        cache: "another cache".into(),
    });
    assert_eq!(
        data_results.next().await.transpose().unwrap(),
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
            data_results.next().await.transpose().unwrap(),
            Some(Ok(DataBuffer {
                data,
                cache: String::new(),
            }))
        );
    }

    drop(data_sender);
    assert_eq!(data_results.next().await.transpose().unwrap(), None);

    for size in [768 * 1024, 0, 1_048_576, 12] {
        let data: Vec<_> = (0..size).map(|index| index as u8).collect();
        let expected = data.iter().map(|byte| *byte as u64).sum::<u64>();

        assert_eq!(
            checksum_data(
                actors.handles(),
                DataBuffer {
                    data,
                    cache: String::new(),
                },
            )
            .await
            .unwrap(),
            expected
        );
    }

    let (input, mut output) = echo::latest(actors.handles());

    input.send(buffer.clone());
    assert_eq!(output.next().await.transpose().unwrap(), Some(buffer));
    drop((input, output));

    let (input, mut output) = add::latest(actors.handles());

    input.send(4);

    let report = actors.stop().await;

    if report.failed()
        || report.actors[0].kind != AktorExecution::BrowserWebWorker
        || output.next().await.transpose().unwrap() != Some(Ok(10))
        || output.next().await.is_some()
    {
        return Err(error(format!("remote shutdown failed: {report}")));
    }

    drop(input);

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    });
    actors.ready().await.map_err(|(_, report)| error(report))?;

    let completion = actors.completion();
    let worker = actors.handles().completion();
    let (input, mut output) = add::latest(actors.handles());

    input.send(3);

    let report = actors.stop().await;

    if output.next().now_or_never() != Some(Some(Ok(Ok(3)))) {
        return Err(error(
            "remote cleanup failure hid a completed latest result",
        ));
    }

    drop(input);

    if !report.failed()
        || !format!("{report}").contains("CloseErr")
        || format!("{}", completion.wait().await.unwrap()) != format!("{report}")
    {
        return Err(error("remote cleanup report was lost"));
    }
    let Ok(AktorRetired::Released(Err((failure, text)))) = worker.wait().await else {
        return Err(error("worker cleanup did not complete"));
    };
    assert!(matches!(failure.data, Some(CounterErr::CloseErr { .. })));
    assert!(text.contains(&std::fmt::Error.to_string()));
    assert_eq!(report.to_string().matches("CloseErr").count(), 1);

    let attempt = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    });
    let failed = attempt
        .ready()
        .await
        .err()
        .ok_or_else(|| error("remote setup failure was lost"))?;

    if !matches!(
        failed.0,
        aktor::AktorStartError::Initializer {
            error: AktorBranch::Second(CounterErr::SetupErr { count: 0 }),
            ..
        }
    ) {
        return Err(error("the second worker's startup error was lost"));
    }

    assert_eq!(failed.1.matches("SetupErr").count(), 1);

    let report = failed
        .0
        .startup()
        .and_then(|s| s.report.as_ref().ok())
        .ok_or_else(|| error("remote rollback report was lost"))?;

    // The first worker may still be starting when the second fails, then it never gets to `end`.
    if report.ran_past_grace() || !report.to_string().contains("SetupErr") {
        return Err(error(format!("remote rollback failed: {report}")));
    }

    let legacy = program.replace("setup_worker.js", "setup_legacy_worker.js");

    if aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    })
    .ready()
    .await
    .is_ok()
    {
        return Err(error(
            "configuration was silently ignored by a legacy worker",
        ));
    }

    Ok(true)
}

#[wasm_bindgen]
pub async fn remote_cancel_check(program: String) -> Result<bool, JsValue> {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    });

    let completion = started.completion();
    let kill = started.killswitch();
    let mut ready = Box::pin(started.ready());

    if ready.as_mut().now_or_never().is_some() {
        return Err(error("remote setup finished before its delay"));
    }

    kill.stop();

    let (failed, _) = ready
        .await
        .err()
        .ok_or_else(|| error("stopping remote startup returned a handle"))?;

    let report = completion.wait().await.unwrap();

    if failed.reason() != AktorStartReason::Stopping || report.ran_past_grace() {
        return Err(error(
            "stopping before configuration should skip initialization",
        ));
    }

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
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
        grace: Duration::from_millis(300),
    });

    let completion = started.completion();

    wasm_bindgen_futures::JsFuture::from(crate::group::configured()).await?;
    drop(started);

    let report = completion.wait().await.unwrap();

    Ok(report.ran_past_grace()
        && report.actors[0].ran_past_grace()
        && report.actors[0].kind == AktorExecution::BrowserWebWorker)
}
