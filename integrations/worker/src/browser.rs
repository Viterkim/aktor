use crate::preferences::{read::read, write::write};
use aktor::*;
use aktor::{
    data::{DataError, Reader, Writer},
    message::AktorDeliveryOutcome,
};
use core::num::NonZeroUsize;
use er::*;
use futures_util::FutureExt;
use rusqlite::Connection;
use sqlite_wasm_vfs::sahpool::{OpfsSAHError, OpfsSAHPoolUtil};
use std::{
    cell::{Cell, RefCell},
    fmt::Display,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Waker},
};
use wasm_bindgen::prelude::*;
use worker::{Options, Worker, WorkerCause};

fn json(value: &impl serde::Serialize) -> Result<String, JsValue> {
    #[derive(Er)]
    struct JsonErr;

    Ok(serde_json::to_string(value).er(|| JsonErr).er_report()?)
}

fn outcome(outcome: AktorDeliveryOutcome) -> &'static str {
    match outcome {
        AktorDeliveryOutcome::NotAdmitted => "NotAdmitted",
        AktorDeliveryOutcome::Discarded => "Discarded",
        AktorDeliveryOutcome::OutcomeUnknown => "OutcomeUnknown",
    }
}

fn failure(delivery: AktorDeliveryOutcome, cause: WorkerCause) -> serde_json::Value {
    let outcome = outcome(delivery);
    let cause = match cause {
        WorkerCause::Operations { expected, actual } => {
            serde_json::json!({ "Operations": { "expected": expected, "actual": actual } })
        }
        WorkerCause::UnknownOperation(name) => serde_json::json!({ "UnknownOperation": name }),
        WorkerCause::Closed => "Closed".into(),
        WorkerCause::Full => "Full".into(),
        WorkerCause::Codec => "Codec".into(),
        WorkerCause::Setup => "Setup".into(),
        WorkerCause::Cleanup => "Cleanup".into(),
        WorkerCause::Crashed => "Crashed".into(),
        WorkerCause::Protocol => "Protocol".into(),
        WorkerCause::Transport => "Transport".into(),
    };

    serde_json::json!({ "outcome": outcome, "cause": cause })
}

pub(super) fn startup(
    result: Result<&(), (&worker::WorkerStartError<impl Sized>, String)>,
) -> Result<(), (serde_json::Value, String)> {
    match result {
        Ok(()) => Ok(()),
        Err((error, report)) => {
            let failure_info = error.failure();
            Err((
                failure(failure_info.outcome, failure_info.cause.clone()),
                report,
            ))
        }
    }
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

struct CountInput {
    millis: u32,
    encoded: Rc<Cell<usize>>,
    dropped: Rc<Cell<usize>>,
}
impl AktorData for CountInput {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        self.encoded.set(self.encoded.get() + 1);
        writer.write(&self.millis)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(Self {
            millis: reader.read()?,
            encoded: Rc::default(),
            dropped: Rc::default(),
        })
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

#[derive(Er)]
enum CloseErr {
    Database(#[er(source, skip)] rusqlite::Error),
    Storage(#[er(source, skip)] OpfsSAHError),
}
fn close_database(db: Connection, pool: &OpfsSAHPoolUtil) -> ErResult<(), CloseErr> {
    db.close().er_val(|(db, error)| {
        drop(db);
        CloseErr::database(error)
    })?;

    pool.pause_vfs().er_val(CloseErr::storage)
}

#[derive(Er)]
enum OpenErr {
    #[er(format = "preferences storage is already open in another tab")]
    Busy(#[er(source, skip)] OpfsSAHError),
    Storage(#[er(source, skip)] OpfsSAHError),
    Database,
    Schema,
}
async fn open_database() -> ErResult<(Connection, OpfsSAHPoolUtil), OpenErr> {
    let pool = sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(
        &sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfg::default(),
        true,
    )
    .await
    .er_val(|error| {
        if let OpfsSAHError::CreateSyncAccessHandle(value) = &error {
            let name = js_sys::Reflect::get(value, &JsValue::from_str("name"))
                .ok()
                .and_then(|name| name.as_string());

            if name.as_deref() == Some("NoModificationAllowedError") {
                return OpenErr::Busy(error);
            }
        }

        OpenErr::Storage(error)
    })?;

    let db = Connection::open("preferences.sqlite").er(|| OpenErr::Database)?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS preferences(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .er(|| OpenErr::Schema)?;

    Ok((db, pool))
}

pub(super) struct Database;
impl AktorWorkerContract for Database {
    type State = Connection;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = ();
}

#[wasm_bindgen]
pub async fn start_worker() -> Result<(), JsValue> {
    worker::serve_setup::<Database>(crate::options("preferences-v1"), |()| {
        let pool = Rc::new(RefCell::new(None));
        let opened = pool.clone();
        AktorServe {
            start: AktorInit::Try(async move || {
                let (db, owner) = open_database()
                    .await
                    .er_split_with(|(_, report)| ((), report))?;
                *opened.borrow_mut() = Some(owner);
                Ok::<_, ((), String)>(db)
            }),
            end: move |db: Connection| {
                close_database(db, &pool.take().unwrap()).er_split_with(|(_, report)| ((), report))
            },
            hooks: AktorHooks::NONE,
        }
    })
    .await
    .er_report()?;

    Ok(())
}

#[wasm_bindgen]
pub async fn sqlite_listener_check(url: String) -> Result<bool, JsValue> {
    let mut actors = AktorGroup::new();
    let kill = actors.killswitch();
    let after = async |_| ();
    let closing = actors
        .start_with(after)
        .map_err(|(_, report)| js_error(report))?;
    let options = crate::options("preferences-v1");
    let database = actors
        .spawn(crate::worker_setup::<Database, _>(
            "preferences",
            &url,
            (),
            options,
            aktor::Required,
        ))
        .ready()
        .await
        .map_err(|(_, report)| js_error(report))?;

    let saved = write(&database, "listener-proof".into(), "0.8".into())
        .await
        .map_err(js_error)?;
    let found = read(&database, "listener-proof".into())
        .await
        .map_err(js_error)?;
    let missing = read(&database, "listener-missing".into())
        .await
        .map_err(js_error)?;
    let stayed_open = !kill.is_stopping();

    drop(actors);

    let report = closing.wait().await.unwrap();

    Ok(saved.as_deref() == Ok("0.8")
        && found.as_deref() == Ok("0.8")
        && missing.is_err()
        && stayed_open
        && !report.failed()
        && report.actors.len() == 1)
}

#[wasm_bindgen]
pub struct Client {
    _group: AktorGroup,
    worker: Worker<Database>,
    url: String,
}
#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(url: &str) -> Result<Client, JsValue> {
        Self::limited(url, 32, 4 * 1024 * 1024)
    }

    pub async fn ready(&self) -> Result<String, JsValue> {
        json(&startup(self.worker.ready().await))
    }

    pub fn limited(url: &str, capacity: usize, bytes: usize) -> Result<Client, JsValue> {
        let options = Options {
            capacity: NonZeroUsize::new(capacity)
                .ok_or_else(|| js_error("capacity must be nonzero"))?,
            max_outstanding_bytes: core::num::NonZeroUsize::new(bytes).unwrap(),
            ..crate::options("preferences-v1")
        };
        let group = crate::group();
        let (_, worker) = group
            .spawn(crate::worker_setup::<Database, _>(
                "preferences",
                url,
                (),
                options,
                Required,
            ))
            .into_parts()
            .unwrap();
        Ok(Self {
            url: url.into(),
            worker,
            _group: group,
        })
    }

    pub fn executing(&self) -> bool {
        self.worker.executing()
    }

    pub fn outstanding(&self) -> Result<String, JsValue> {
        json(&self.worker.outstanding())
    }

    pub fn abandon(&self, value: String) -> bool {
        write(&self.worker, "abandoned".into(), value)
            .send()
            .now_or_never()
            .map(drop)
            .is_some()
    }

    pub async fn queue_pause(&self, millis: u32) {
        drop(pause(&self.worker, millis).send().await.unwrap());
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
        json(&self.worker.completion().report().await.er_report())
    }

    pub async fn latest_sequence(&self) -> Result<String, JsValue> {
        let running = pause(&self.worker, 150).send().await.unwrap();
        let writing = write(&self.worker, "volume".into(), "0.9".into())
            .send()
            .await
            .unwrap();
        let (panel, mut results) = read(&self.worker, "absent".into()).latest();
        let (other, mut other_results) = read(&self.worker, "absent".into()).latest();

        panel.send("absent".into());

        for _ in 0..2000 {
            panel.send("volume".into());
        }

        other.send("absent".into());

        let before = self.worker.outstanding().0;

        for _ in 0..20_000 {
            let (sender, results) = read(&self.worker, "absent".into()).latest();
            drop(results);
            drop(sender);
        }

        let bounded = self.worker.outstanding().0;
        if bounded != before {
            return Err(js_error(format!(
                "cancelled latest sessions grew outstanding work from {before} to {bounded}"
            )));
        }

        running.await.unwrap();
        writing
            .await
            .unwrap()
            .map_err(|error| js_error(format!("{error:?}")))?;

        let current = results.next().await.transpose().map_err(js_error)?;
        let independent = other_results.next().await.transpose().map_err(js_error)?;

        let (cancelled, cancelled_results) =
            write(&self.worker, "latest-cancelled".into(), "first".into()).latest();
        cancelled.send("latest-cancelled".into(), "pending".into());
        drop(cancelled_results);
        cancelled.send("latest-cancelled".into(), "closed".into());

        let saved = read(&self.worker, "latest-cancelled".into())
            .await
            .map_err(js_error)?
            .map_err(|error| js_error(format!("{error:?}")))?;
        if saved != "first" {
            return Err(js_error(format!(
                "latest cancellation left {saved:?}, expected first"
            )));
        }

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

        let final_result = results.next().await.transpose().map_err(js_error)?;
        let ended = results.next().await.is_none();

        json(&(current, independent, final_result, ended, bounded))
    }

    pub async fn reservation(&self) -> Result<String, JsValue> {
        let running = pause(&self.worker, 150).send().await.unwrap();
        let writing = write(&self.worker, "oversized".into(), "x".repeat(4096)).send();
        let (reply, ()) = futures_util::future::join(writing, async {
            running.await.unwrap();
        })
        .await;
        let counted = self.worker.outstanding();
        let timed = write(&self.worker, "other".into(), "y".repeat(4096))
            .timeout(core::time::Duration::from_millis(5))
            .await;
        let not_admitted = timed.err().is_some_and(|error| !error.admitted);
        let _ = reply
            .unwrap()
            .await
            .map_err(|error| js_error(format!("{error:?}")))?;

        json(&(counted, not_admitted))
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
            .map_err(|(_, report)| js_error(report))?;
        group.start().map_err(|(_, report)| js_error(report))?;
        let worker = group
            .spawn(crate::worker_setup::<crate::group::GroupWorker, _>(
                "codec failure",
                &self.url.replace("/worker.js", "/group-worker.js"),
                (),
                crate::options("group-v1"),
                Required,
            ))
            .ready()
            .await
            .map_err(|(_, report)| js_error(report))?;
        let codec = Transport::<u32, crate::group::BadInput, u32>::request(
            &worker,
            aktor::operation::Operation {
                name: pause::NAME,
                caller: core::panic::Location::caller(),
            },
            crate::group::BadInput("invalid input"),
        )
        .await
        .unwrap_err();
        group.killswitch().stop();
        let report = closing.await.map_err(js_error)?;
        assert!(notified.get());
        assert_eq!(codec.failure_id, Some(report.failures[0].id));

        let mut admitted = Vec::new();
        for _ in 0..4 {
            admitted.push(pause(&self.worker, 0).send().await.unwrap());
        }
        use aktor::{dispatch::Transport, operation::Operation};
        let encoded = Rc::new(Cell::new(0));
        let dropped = Rc::new(Cell::new(0));
        let mut waiting = <&Worker<Database> as Transport<
            Connection,
            CountInput,
            u32,
            (),
            dispatch::DataCodec,
        >>::request(
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
            assert_eq!(reply.await.unwrap(), 0);
        }
        assert_eq!(waiting.send().await.unwrap().await.unwrap(), 0);
        assert_eq!(encoded.get(), 1);

        let full_bytes = write(&self.worker, "diagnostic-budget".into(), "x".repeat(1024))
            .send()
            .await
            .unwrap();
        let mut waiting = pause(&self.worker, 0).into_future();
        assert!((&mut waiting).now_or_never().is_none());
        assert_eq!(self.worker.outstanding(), (1, 1024));
        assert_eq!(
            full_bytes
                .await
                .unwrap()
                .map_err(|error| js_error(format!("{error:?}")))?
                .len(),
            1024
        );
        assert_eq!(waiting.send().await.unwrap().await.unwrap(), 0);
        assert_eq!(self.worker.outstanding(), (0, 0));

        let healthy = read(&self.worker, "volume".into())
            .await
            .map_err(js_error)?;

        json(&(
            serde_json::json!({ "outcome": outcome(codec.outcome), "report": codec.report }),
            healthy,
        ))
    }

    pub async fn large(&self) -> Result<usize, JsValue> {
        let text = "ø".repeat(4 * 1024 * 1024);
        let expected = text.len();

        assert_eq!(
            write(&self.worker, "large".into(), text.clone())
                .await
                .map_err(js_error)?
                .map_err(|error| js_error(format!("{error:?}")))?,
            text
        );
        assert_eq!(
            read(&self.worker, "large".into())
                .await
                .map_err(js_error)?
                .map_err(|error| js_error(format!("{error:?}")))?,
            text
        );
        Ok(expected)
    }

    pub async fn timed_pause(&self, millis: u32, timeout: u32) -> Result<String, JsValue> {
        let result = pause(&self.worker, millis)
            .timeout(core::time::Duration::from_millis(u64::from(timeout)))
            .await
            .map(|output| output.unwrap())
            .map_err(|error| error.admitted);

        json(&result)
    }

    pub async fn submitted(&self) -> Result<String, JsValue> {
        let (_idle, mut results) = read(&self.worker, "absent".into()).latest();

        assert!(results.next().await.unwrap().unwrap().is_err());

        let mut request = pause(&self.worker, 0).into_future();
        let pending = Pin::new(&mut request)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();

        let (ended, output) =
            futures_util::future::join(async { results.next().await.is_none() }, async {
                drop(self.worker.shutdown());

                let reply = request.send().await.unwrap();
                let output = reply.await.map_err(js_error)?;

                self.worker
                    .completion()
                    .report()
                    .await
                    .map_err(|(_, report)| js_error(report))?;
                Ok::<_, JsValue>(output)
            })
            .await;

        json(&(pending, output?, ended))
    }

    pub async fn write(&self, key: String, value: String) -> Result<String, JsValue> {
        json(&write(&self.worker, key, value).await.map_err(js_error)?)
    }

    pub async fn read(&self, key: String) -> Result<String, JsValue> {
        json(&read(&self.worker, key).await.map_err(js_error)?)
    }

    pub async fn occupy(&self, millis: u32) -> Result<String, JsValue> {
        json(&occupy(&self.worker, millis).await.map_err(js_error)?)
    }

    pub async fn pause(&self, millis: u32) -> Result<String, JsValue> {
        json(&pause(&self.worker, millis).await.map_err(js_error)?)
    }

    pub fn terminate(&self) {
        self.worker.terminate();
    }
}

#[wasm_bindgen]
pub struct Observer {
    completion: worker::Completion,
}
#[wasm_bindgen]
impl Observer {
    pub async fn finished(&mut self) -> Result<String, JsValue> {
        json(&self.completion.report().await.er_report())
    }
}
