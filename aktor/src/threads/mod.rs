use crate::{AktorError, message::LocalFuture};
use alloc::{boxed::Box, collections::BTreeMap};
use core::{
    cell::RefCell,
    sync::atomic::{AtomicU32, Ordering},
};
use futures_util::future::{AbortHandle, Abortable};
use wasm_bindgen::prelude::*;

#[doc(hidden)]
pub mod lock;
mod runtime;
pub use crate::cross_core::{Completion, Handle};
#[doc(hidden)]
pub use runtime::Thread;

type Job = Box<dyn FnOnce() -> LocalFuture<'static, Result<(), AktorError>> + Send>;

static JOBS: lock::Mutex<RefCell<BTreeMap<u32, Job>>> =
    lock::Mutex::new(RefCell::new(BTreeMap::new()));
static NEXT: AtomicU32 = AtomicU32::new(1);

thread_local! {
    static ACTIVE: RefCell<Option<(u32, AbortHandle)>> = const { RefCell::new(None) };
}

#[doc(hidden)]
pub fn insert(job: Job) -> u32 {
    JOBS.lock(|jobs| {
        let mut jobs = jobs.borrow_mut();
        let id = loop {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);

            if !jobs.contains_key(&id) {
                break id;
            }
        };

        jobs.insert(id, job);
        id
    })
}

#[doc(hidden)]
pub fn remove(id: u32) -> bool {
    let job = JOBS.lock(|jobs| jobs.borrow_mut().remove(&id));
    let removed = job.is_some();

    drop(job);
    removed
}

#[wasm_bindgen]
#[doc(hidden)]
pub async fn __aktor_thread_start(id: u32) -> Result<(), JsValue> {
    let job = JOBS
        .lock(|jobs| jobs.borrow_mut().remove(&id))
        .ok_or_else(|| JsValue::from_str("shared actor startup job is missing"))?;
    let (abort, registration) = AbortHandle::new_pair();

    ACTIVE.with(|active| *active.borrow_mut() = Some((id, abort)));
    let result = Abortable::new(async move { job().await }, registration).await;
    let active = ACTIVE.with(|active| core::mem::take(&mut *active.borrow_mut()));
    drop(active);

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(JsValue::from_str(&error.diagnostics)),
        Err(_) => Err(JsValue::from_str(
            "shared actor cancelled before cleanup completed",
        )),
    }
}

#[wasm_bindgen]
#[doc(hidden)]
pub fn __aktor_thread_cancel(id: u32) {
    let abort = ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .filter(|(job, _)| *job == id)
            .map(|(_, abort)| abort.clone())
    });

    if let Some(abort) = abort {
        abort.abort();
    }
}
