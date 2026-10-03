use aktor::{
    AktorCleanupError, AktorError, AktorGroup,
    worker::{self, Options, Server},
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
pub fn start_group_worker(fail_cleanup: bool, wrong_data: bool, fail_data: bool) -> Result<(), JsValue> {
    let server = if fail_data {
        worker::serve_with(0u32, async |_| Err(AktorCleanupError {
            diagnostics: "backup folder is read only".into(),
            data: BrokenData,
        }), options())
    } else if wrong_data {
        worker::serve_with(0u32, async |_| Err(AktorCleanupError {
            diagnostics: "backup folder is read only".into(),
            data: String::from("unexpected data type"),
        }), options())
    } else {
        worker::serve_with(
        0u32,
        async move |_| {
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
                    5 => {
                        let value = Portable {
                            options: Vec::new(), map: Default::default(), choice: Vec::new(),
                            limits: (0, 0), invalid: Some(RejectedInput),
                        };
                        drop(roundtrip(&storage, value).send().await);
                        core::future::pending::<()>().await;
                    }
                    _ => {}
                }
                Ok::<_, AktorError>(())
            },
            async move |_| {
                hook_count.set(hook_count.get() + 1);
                Ok::<_, AktorError>(())
            },
        )
        .await;
    let report = completed.wait().await;
    let typed_cleanup = if mode == 4 {
        let completion = retained.borrow_mut().take().unwrap();
        let error = completion.wait().await.unwrap_err();
        error
            .data
            .is_some_and(|data| data.code == 17 && data.option == Some(None))
    } else if mode == 6 || mode == 7 {
        let completion = retained.borrow_mut().take().unwrap();
        let error = completion.wait().await.unwrap_err();
        error.data.is_none()
            && error.to_string().contains("backup folder is read only")
            && error.to_string().contains("Optional lifecycle data")
    } else {
        true
    };
    let deferred_reply_safe = if let Some(mut reply) = deferred.borrow_mut().take() {
        (0..2).all(|_| std::pin::Pin::new(&mut reply)
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending())
    } else {
        true
    };
    serde_json::to_string(&Check {
        failure_message: report.failure.as_ref().map(|failure| failure.message.clone()),
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
    assert!(error.to_string().contains("could not open storage"));
    assert!(
        error
            .data
            .is_some_and(|data| data.code == 23 && data.option == Some(None))
    );
    let error = actor.completion().wait().await.unwrap_err();
    Ok(error
        .data
        .is_some_and(|data| data.code == 23 && data.option == Some(None)))
}
