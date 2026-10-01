use crate::preferences::{read::read, write::write};
use aktor::message::CallError;
use aktor::*;
use rusqlite::Connection;
use std::{
    cell::RefCell,
    fmt::Display,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Waker},
};
use wasm_bindgen::prelude::*;
use worker::{Options, Server, Worker, WorkerCause, WorkerError};

thread_local! {
    static SERVER: RefCell<Option<Server>> = const { RefCell::new(None) };
}

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

worker_routes!(
    routes,
    Connection,
    [
        crate::preferences::read::read,
        crate::preferences::write::write,
        occupy,
        pause
    ]
);

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

    let server = worker::serve_with(
        db,
        routes,
        async move |db| {
            db.close().map_err(|(_, error)| {
                WorkerError::new(
                    CallError::OutcomeUnknown,
                    WorkerCause::Cleanup(error.to_string()),
                )
            })?;

            pool.pause_vfs().map_err(|error| {
                WorkerError::new(
                    CallError::OutcomeUnknown,
                    WorkerCause::Cleanup(error.to_string()),
                )
            })?;

            Ok(())
        },
        Options {
            build: "preferences-v1".into(),
            ..Options::default()
        },
    )
    .map_err(js_error)?;

    SERVER.with(|slot| *slot.borrow_mut() = Some(server));

    Ok(())
}

#[wasm_bindgen]
pub struct Client {
    worker: Worker<Connection>,
}
#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(url: &str, timeout_ms: u32) -> Result<Client, JsValue> {
        Ok(Self {
            worker: Worker::with_options(
                url,
                Options {
                    timeout_ms,
                    build: "preferences-v1".into(),
                    ..Options::default()
                },
            )
            .map_err(js_error)?,
        })
    }

    pub async fn ready(&self) -> Result<String, JsValue> {
        worker::encode(&self.worker.ready().await).map_err(js_error)
    }

    pub fn limited(url: &str, capacity: usize, bytes: usize) -> Result<Client, JsValue> {
        Ok(Self {
            worker: Worker::with_options(
                url,
                Options {
                    build: "preferences-v1".into(),
                    capacity,
                    max_payload_bytes: bytes,
                    max_outstanding_bytes: bytes,
                    ..Options::default()
                },
            )
            .map_err(js_error)?,
        })
    }

    pub fn executing(&self) -> bool {
        self.worker.executing()
    }

    pub fn outstanding(&self) -> Result<String, JsValue> {
        worker::encode(&self.worker.outstanding()).map_err(js_error)
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
        worker::encode(&self.worker.completion().wait().await).map_err(js_error)
    }

    pub async fn latest_sequence(&self) -> Result<String, JsValue> {
        let running = pause::request(&self.worker, 150).send().await;

        let old = read::request(&self.worker, "volume".into())
            .latest("panel")
            .checked_send()
            .await
            .map_err(js_error)?;

        let write = write::request(&self.worker, "volume".into(), "0.9".into())
            .send()
            .await;

        let panel = read::request(&self.worker, "volume".into())
            .latest("other panel")
            .send()
            .await;

        let oversized = read::request(&self.worker, "x".repeat(2048))
            .latest("panel")
            .checked()
            .await;

        let new = read::request(&self.worker, "volume".into())
            .latest("panel")
            .try_send()
            .map_err(js_error)?;

        let rejected = read::request(&self.worker, "volume".into())
            .latest("third panel")
            .try_send()
            .is_err();

        let old = old.await;
        running.await;
        write
            .await
            .map_err(|error| JsValue::from_str(&format!("{error:?}")))?;
        panel
            .await
            .map_err(|error| JsValue::from_str(&format!("{error:?}")))?;
        let new = new.await;

        worker::encode(&(old, new, rejected, oversized)).map_err(js_error)
    }

    pub async fn reservation(&self) -> Result<String, JsValue> {
        let key = |millis, bytes: usize, character: char| -> Result<String, JsValue> {
            let base = pause::NAME.len() + worker::encode(&(millis,)).map_err(js_error)?.len();

            Ok(character.to_string().repeat(bytes.saturating_sub(base)))
        };

        let first = pause::request(&self.worker, 200)
            .latest(key(200u32, 600, 'a')?)
            .checked_send()
            .await
            .map_err(js_error)?;

        let second = pause::request(&self.worker, 200)
            .latest(key(200u32, 400, 'b')?)
            .checked_send()
            .await
            .map_err(js_error)?;

        let mut waiting = pause::request(&self.worker, 0).latest(key(0u32, 100, 'c')?);

        let pending = Pin::new(&mut waiting)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();

        let waiting = waiting.latest(key(0u32, 600, 'd')?);
        let third = waiting.checked_send().await.map_err(js_error)?;
        let reserved = self.worker.outstanding();
        let rejected = matches!(
            pause::request(&self.worker, 0)
                .latest(key(0u32, 500, 'e')?)
                .try_send(),
            Err(worker::TrySendError::Full(_))
        );

        first.await.map_err(js_error)?;
        second.await.map_err(js_error)?;
        third.await.map_err(js_error)?;

        worker::encode(&(pending, reserved, rejected)).map_err(js_error)
    }

    pub async fn rejection(&self) -> Result<String, JsValue> {
        let oversized = match read::request(&self.worker, "x".repeat(2048)).try_send() {
            Err(worker::TrySendError::Rejected(_, error)) => error,
            _ => return Err(js_error("expected payload rejection")),
        };

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

        let healthy = read::request(&self.worker, "volume".into()).checked().await;

        worker::encode(&(oversized, codec, healthy)).map_err(js_error)
    }

    pub async fn submitted(&self) -> Result<String, JsValue> {
        let mut request = pause::request(&self.worker, 0);
        let pending = Pin::new(&mut request)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();

        drop(self.worker.shutdown());
        let reply = request.try_send().map_err(js_error)?;
        let output = reply.checked().await.map_err(js_error)?;
        self.worker.completion().wait().await.map_err(js_error)?;

        worker::encode(&(pending, output)).map_err(js_error)
    }

    pub async fn write(&self, key: String, value: String) -> Result<String, JsValue> {
        worker::encode(&write(&self.worker, key, value).await).map_err(js_error)
    }

    pub async fn read(&self, key: String) -> Result<String, JsValue> {
        worker::encode(&read(&self.worker, key).await).map_err(js_error)
    }

    pub async fn occupy(&self, millis: u32) -> Result<String, JsValue> {
        worker::encode(&occupy::request(&self.worker, millis).checked().await).map_err(js_error)
    }

    pub async fn pause(&self, millis: u32) -> Result<String, JsValue> {
        worker::encode(&pause::request(&self.worker, millis).checked().await).map_err(js_error)
    }

    pub fn terminate(&self) {
        self.worker.terminate();
    }
}

#[wasm_bindgen]
pub fn startup_failed(error: String) -> Result<(), JsValue> {
    worker::setup_failed(error).map_err(js_error)
}

#[wasm_bindgen]
pub struct Observer {
    completion: worker::Completion,
}
#[wasm_bindgen]
impl Observer {
    pub async fn finished(&mut self) -> Result<String, JsValue> {
        worker::encode(&self.completion.wait().await).map_err(js_error)
    }
}
