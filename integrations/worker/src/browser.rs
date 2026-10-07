use crate::preferences::{read::read, write::write};
use aktor::message::CallError;
use aktor::*;
use futures_util::FutureExt;
use rusqlite::Connection;
use std::{
    cell::{Cell, RefCell},
    fmt::Display,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Waker},
};
use wasm_bindgen::prelude::*;
use worker::{Options, Worker, WorkerCause, WorkerError};

#[aktor]
async fn occupy(_: &Connection, millis: u32) -> u32 {
    let until = js_sys::Date::now() + f64::from(millis);

    while js_sys::Date::now() < until {
        std::hint::spin_loop();
    }

    millis
}

#[aktor]
async fn pause(db: &Connection, millis: u32) -> u32 {
    timer(db, Rc::new(millis)).await
}

#[aktor]
async fn timer(_: &Connection, millis: Rc<u32>) -> u32 {
    gloo_timers::future::TimeoutFuture::new(*millis).await;

    *millis
}

#[derive(serde::Deserialize)]
struct CountInput {
    millis: u32,
    #[serde(skip)]
    encoded: Rc<Cell<usize>>,
    #[serde(skip)]
    dropped: Rc<Cell<usize>>,
}
impl serde::Serialize for CountInput {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.encoded.set(self.encoded.get() + 1);
        serde::Serialize::serialize(&(self.millis,), serializer)
    }
}
impl Drop for CountInput {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}

thread_local! {
    static LATEST: RefCell<Option<worker::LatestSender<(String,)>>> = const { RefCell::new(None) };
}

struct LatestWake;
// The destructor is what this check needs.
#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for LatestWake {
    fn wake(self: std::sync::Arc<Self>) {}
}
impl Drop for LatestWake {
    fn drop(&mut self) {
        let sender = LATEST.with(|sender| sender.borrow_mut().take());
        drop(sender);
    }
}

fn js_error(error: impl Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[wasm_bindgen]
pub async fn start_worker() -> Result<(), JsValue> {
    let pool = sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(
        &sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfg::default(),
        true,
    )
    .await
    .map_err(|error| {
        if let sqlite_wasm_vfs::sahpool::OpfsSAHError::CreateSyncAccessHandle(value) = &error {
            let name = js_sys::Reflect::get(value, &JsValue::from_str("name"))
                .ok()
                .and_then(|name| name.as_string());

            if name.as_deref() == Some("NoModificationAllowedError") {
                return JsValue::from_str("preferences storage is already open in another tab");
            }
        }

        js_error(error)
    })?;

    let db = Connection::open("preferences.sqlite").map_err(js_error)?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS preferences(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .map_err(js_error)?;

    let cleanup = async move |db: Connection| {
        db.close()
            .map_err(|(_, error)| AktorCleanupError::new(error.to_string()))?;

        pool.pause_vfs()
            .map_err(|error| AktorCleanupError::new(error.to_string()))?;

        Ok(())
    };

    let options = Options {
        build: "preferences-v1".into(),
        ..Options::default()
    };

    worker::serve_with(db, cleanup, options)
        .map_err(js_error)?
        .wait()
        .await
        .map_err(js_error)?;

    Ok(())
}

#[wasm_bindgen]
pub async fn sqlite_listener_check(url: String) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::new();
    let kill = actors.killswitch();
    let after = async |_| Ok::<_, AktorCleanupError>(());
    let closing = actors.start_with(after).map_err(js_error)?;
    let options = Options {
        build: "preferences-v1".into(),
        ..Options::default()
    };
    let database = actors
        .worker::<Connection>("preferences", &url, options)
        .await
        .map_err(js_error)?;

    let saved = write(&database, "listener-proof".into(), "0.8".into()).await;
    let found = read(&database, "listener-proof".into()).await;
    let missing = read(&database, "listener-missing".into()).await;
    let stayed_open = !kill.is_stopping();

    drop(actors);

    let report = closing.wait().await;

    Ok(saved.as_deref() == Ok("0.8")
        && found.as_deref() == Ok("0.8")
        && missing.is_err()
        && stayed_open
        && !report.failed()
        && report.actors.len() == 1)
}

#[wasm_bindgen]
pub struct Client {
    worker: Worker<Connection>,
    url: String,
}
#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(url: &str) -> Result<Client, JsValue> {
        Ok(Self {
            url: url.into(),
            worker: Worker::with_options(
                url,
                Options {
                    build: "preferences-v1".into(),
                    ..Options::default()
                },
            )
            .map_err(js_error)?,
        })
    }

    pub async fn ready(&self) -> Result<String, JsValue> {
        serde_json::to_string(&self.worker.ready().await).map_err(js_error)
    }

    pub fn limited(url: &str, capacity: usize, bytes: usize) -> Result<Client, JsValue> {
        Ok(Self {
            url: url.into(),
            worker: Worker::with_options(
                url,
                Options {
                    build: "preferences-v1".into(),
                    capacity,
                    max_outstanding_bytes: bytes,
                },
            )
            .map_err(js_error)?,
        })
    }

    pub fn executing(&self) -> bool {
        self.worker.executing()
    }

    pub fn outstanding(&self) -> Result<String, JsValue> {
        serde_json::to_string(&self.worker.outstanding()).map_err(js_error)
    }

    pub fn abandon(&self, value: String) -> bool {
        write::request(&self.worker, "abandoned".into(), value)
            .send()
            .now_or_never()
            .map(drop)
            .is_some()
    }

    pub async fn queue_pause(&self, millis: u32) {
        drop(pause::request(&self.worker, millis).send().await);
    }

    pub fn observer(&self) -> Observer {
        Observer {
            completion: self.worker.completion(),
        }
    }

    pub fn begin_shutdown(&self) {
        drop(self.worker.shutdown());
    }

    pub async fn finished(&self) -> Result<String, JsValue> {
        serde_json::to_string(&self.worker.completion().wait().await).map_err(js_error)
    }

    pub async fn latest_sequence(&self) -> Result<String, JsValue> {
        let running = pause(&self.worker, 150).send().await;
        let write = write(&self.worker, "volume".into(), "0.9".into())
            .send()
            .await;
        let (panel, mut results) = read(&self.worker, "absent".into()).latest();
        let (other, mut other_results) = read(&self.worker, "absent".into()).latest();

        panel.send("absent".into());

        for _ in 0..2000 {
            panel.send("volume".into());
        }

        other.send("absent".into());

        let bounded = self.worker.outstanding().0;

        for _ in 0..20_000 {
            let (sender, results) = read(&self.worker, "absent".into()).latest();
            drop(results);
            drop(sender);
        }

        running.await;
        write
            .await
            .map_err(|error| js_error(format!("{error:?}")))?;

        let current = results.next().await;
        let independent = other_results.next().await;

        LATEST.with(|sender| *sender.borrow_mut() = Some(panel.inner.clone()));

        let waker = std::task::Waker::from(std::sync::Arc::new(LatestWake));
        let mut next = Box::pin(results.next());

        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        drop(next);

        panel.send("volume".into());
        drop(panel);

        let final_result = results.next().await;
        let ended = results.next().await.is_none();

        serde_json::to_string(&(current, independent, final_result, ended, bounded))
            .map_err(js_error)
    }

    pub async fn reservation(&self) -> Result<String, JsValue> {
        let running = pause(&self.worker, 150).send().await;
        let writing = write(&self.worker, "oversized".into(), "x".repeat(4096)).send();
        let (reply, ()) = futures_util::future::join(writing, async {
            running.await;
        })
        .await;
        let counted = self.worker.outstanding();
        let timed = write(&self.worker, "other".into(), "y".repeat(4096))
            .timeout(core::time::Duration::from_millis(5))
            .await;
        let not_admitted = timed.err().is_some_and(|error| !error.admitted);
        let _ = reply
            .await
            .map_err(|error| js_error(format!("{error:?}")))?;

        serde_json::to_string(&(counted, not_admitted)).map_err(js_error)
    }

    pub async fn rejection(&self) -> Result<String, JsValue> {
        let mut group = AktorGroup::new();
        let (closed, closing) = tokio::sync::oneshot::channel();
        let notified = Rc::new(Cell::new(false));
        let notification = notified.clone();
        group
            .on_shutdown(move |report| {
                notification.set(true);
                let _sent = closed.send(report);
            })
            .map_err(js_error)?;
        group.start().map_err(js_error)?;
        let worker = group
            .worker::<u32>(
                "codec failure",
                &self.url.replace("/worker.js", "/group-worker.js"),
                Options {
                    build: "group-v1".into(),
                    ..Options::default()
                },
            )
            .await
            .map_err(js_error)?;
        let completed = worker.completion();
        let continued = Rc::new(Cell::new(false));
        let after = continued.clone();
        let failed = async {
            worker::WorkerRequest::<u32, u32>::new(
                &worker,
                pause::NAME,
                Err(WorkerError::new(
                    CallError::NotAdmitted,
                    WorkerCause::Codec("invalid input".into()),
                )),
            )
            .await;
            after.set(true);
        };
        futures_util::pin_mut!(failed);
        let report = match futures_util::future::select(failed, closing).await {
            futures_util::future::Either::Right((report, _)) => report.map_err(js_error)?,
            futures_util::future::Either::Left(_) => return Err(js_error("failed call continued")),
        };
        assert!(!continued.get());
        assert!(notified.get());
        let codec = completed.wait().await.unwrap_err();
        assert!(report.failed());
        assert!(report.failure.unwrap().message.contains("invalid input"));
        assert_eq!(codec.outcome, CallError::NotAdmitted);

        let mut admitted = Vec::new();
        for _ in 0..4 {
            admitted.push(pause(&self.worker, 0).send().await);
        }
        use aktor::{dispatch::Transport, operation::Operation};
        let encoded = Rc::new(Cell::new(0));
        let dropped = Rc::new(Cell::new(0));
        let mut waiting = <&Worker<Connection> as Transport<Connection, CountInput, u32>>::request(
            &self.worker,
            Operation {
                name: pause::NAME,
                caller: std::panic::Location::caller(),
            },
            CountInput {
                millis: 0,
                encoded: encoded.clone(),
                dropped: dropped.clone(),
            },
        );
        for _ in 0..2 {
            assert!((&mut waiting).now_or_never().is_none());
        }
        assert_eq!(encoded.get(), 1);
        assert_eq!(dropped.get(), 1);
        for reply in admitted {
            assert_eq!(reply.await, 0);
        }
        assert_eq!(waiting.send().await.await, 0);
        assert_eq!(encoded.get(), 1);

        let full_bytes = write(&self.worker, "diagnostic-budget".into(), "x".repeat(1024))
            .send()
            .await;
        let mut waiting = pause::request(&self.worker, 0);
        assert!((&mut waiting).now_or_never().is_none());
        assert_eq!(self.worker.outstanding(), (1, 1024));
        assert_eq!(
            full_bytes
                .await
                .map_err(|error| js_error(format!("{error:?}")))?
                .len(),
            1024
        );
        assert_eq!(waiting.send().await.await, 0);
        assert_eq!(self.worker.outstanding(), (0, 0));

        let healthy = read(&self.worker, "volume".into()).await;

        serde_json::to_string(&(codec, healthy)).map_err(js_error)
    }

    pub async fn large(&self) -> Result<usize, JsValue> {
        let text = "ø".repeat(4 * 1024 * 1024);
        let expected = text.len();

        assert_eq!(
            write(&self.worker, "large".into(), text.clone())
                .await
                .map_err(|error| js_error(format!("{error:?}")))?,
            text
        );
        assert_eq!(
            read(&self.worker, "large".into())
                .await
                .map_err(|error| js_error(format!("{error:?}")))?,
            text
        );
        Ok(expected)
    }

    pub async fn timed_pause(&self, millis: u32, timeout: u32) -> Result<String, JsValue> {
        let result = pause(&self.worker, millis)
            .timeout(core::time::Duration::from_millis(u64::from(timeout)))
            .await
            .map_err(|error| error.admitted);

        serde_json::to_string(&result).map_err(js_error)
    }

    pub async fn submitted(&self) -> Result<String, JsValue> {
        let (_idle, mut results) = read(&self.worker, "absent".into()).latest();

        assert!(results.next().await.unwrap().is_err());

        let mut request = pause::request(&self.worker, 0);
        let pending = Pin::new(&mut request)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();

        let (ended, output) =
            futures_util::future::join(async { results.next().await.is_none() }, async {
                drop(self.worker.shutdown());

                let reply = request.send().await;
                let output = reply.await;

                self.worker.completion().wait().await.map_err(js_error)?;
                Ok::<_, JsValue>(output)
            })
            .await;

        serde_json::to_string(&(pending, output?, ended)).map_err(js_error)
    }

    pub async fn write(&self, key: String, value: String) -> Result<String, JsValue> {
        serde_json::to_string(&write(&self.worker, key, value).await).map_err(js_error)
    }

    pub async fn read(&self, key: String) -> Result<String, JsValue> {
        serde_json::to_string(&read(&self.worker, key).await).map_err(js_error)
    }

    pub async fn occupy(&self, millis: u32) -> Result<String, JsValue> {
        serde_json::to_string(&occupy(&self.worker, millis).await).map_err(js_error)
    }

    pub async fn pause(&self, millis: u32) -> Result<String, JsValue> {
        serde_json::to_string(&pause(&self.worker, millis).await).map_err(js_error)
    }

    pub fn terminate(&self) {
        self.worker.terminate();
    }
}

#[wasm_bindgen]
pub fn startup_failed(error: String) -> Result<(), JsValue> {
    worker::setup_failed(aktor::AktorSetupError::new(error)).map_err(js_error)
}

#[wasm_bindgen]
pub struct Observer {
    completion: worker::Completion,
}
#[wasm_bindgen]
impl Observer {
    pub async fn finished(&mut self) -> Result<String, JsValue> {
        serde_json::to_string(&self.completion.wait().await).map_err(js_error)
    }
}
