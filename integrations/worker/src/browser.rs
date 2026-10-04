use crate::preferences::{read::read, write::write};
use aktor::message::CallError;
use aktor::*;
use rusqlite::Connection;
use std::{
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
}
#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(url: &str) -> Result<Client, JsValue> {
        Ok(Self {
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
            .try_send()
            .map(drop)
            .is_ok()
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
        running.await;
        write
            .await
            .map_err(|error| js_error(format!("{error:?}")))?;
        let current = results.next().await;
        let independent = other_results.next().await;
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
        let codec = WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Codec("invalid input".into()),
        );
        let request =
            worker::WorkerRequest::<Connection, u32>::new(&self.worker, pause::NAME, Err(codec));
        let codec = match request.try_send() {
            Err(worker::TrySendError::Rejected(_, error)) => error,
            _ => return Err(js_error("expected codec rejection")),
        };
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
                let reply = request.try_send().map_err(js_error)?;
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
