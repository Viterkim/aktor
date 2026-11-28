//! One supervisor thread for the whole process. Groups mostly wait, a thread each made a server
//! with a hundred small groups run a hundred idle runtimes.

use er::*;

#[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
#[derive(Er)]
pub(crate) enum RuntimeError {
    #[er(format = "could not start the supervisor thread")]
    Thread,
    #[er(format = "could not build the supervisor runtime")]
    Runtime,
    #[er(format = "the supervisor thread stopped during startup")]
    Stopped,
}

/// The Tokio side, shared by every group. It lives as long as the process, so a group keeps
/// shutting down after the runtime that started it is gone.
#[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
pub(crate) fn runtime() -> ErResult<tokio::runtime::Handle, RuntimeError> {
    static SHARED: std::sync::Mutex<Option<tokio::runtime::Handle>> = std::sync::Mutex::new(None);

    let mut shared = SHARED.lock().unwrap_or_else(|error| error.into_inner());

    if let Some(handle) = &*shared {
        return Ok(handle.clone());
    }

    let (started, ready) = std::sync::mpsc::sync_channel(1);

    std::thread::Builder::new()
        .name("aktor".into())
        .spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => {
                    if started.send(Ok(runtime.handle().clone())).is_ok() {
                        runtime.block_on(core::future::pending::<()>());
                    }
                }
                Err(error) => {
                    let _sent = started.send(Err(error));
                }
            }
        })
        .er(|| RuntimeError::Thread)?;

    let started = ready.recv().er(|| RuntimeError::Stopped)?;
    let handle = started.er(|| RuntimeError::Runtime)?;

    *shared = Some(handle.clone());
    Ok(handle)
}

#[derive(Er)]
enum CallbackWorkerErr {
    #[er(format = "could not start the shutdown callback worker")]
    Spawn,
    #[er(format = "the shutdown callback worker closed without returning its result")]
    Closed,
}
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> ErResult<T, CallbackWorkerErr> {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    let (sent, done) = tokio::sync::oneshot::channel();
    #[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
    let runtime = tokio::runtime::Handle::try_current().ok();

    std::thread::Builder::new()
        .name("aktor shutdown closure".into())
        .spawn(move || {
            #[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
            let _runtime = runtime.as_ref().map(tokio::runtime::Handle::enter);
            let _sent = sent.send(catch_unwind(AssertUnwindSafe(work)));
        })
        .er(|| CallbackWorkerErr::Spawn)?;

    match done.await.er(|| CallbackWorkerErr::Closed)? {
        Ok(value) => Ok(value),
        Err(payload) => resume_unwind(payload),
    }
}

pub(super) fn hook<Cleanup, Output, Mode>(cleanup: Cleanup) -> super::ShutdownHook
where
    Cleanup: FnOnce(super::AktorShutdownReport) -> Output + Send + 'static,
    Output: crate::AktorShutdownOutput<Mode>,
    Output::Future: Send + 'static,
{
    Box::new(move |report, kill| {
        Box::pin(async move {
            let (_cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
            let worker = CallbackWorker::new(kill.clone());
            if let Err(error) = blocking(move || {
                let run = || {
                    let future = async move {
                        let shutdown = cleanup(report).into_shutdown();
                        tokio::select! {
                            biased;
                            result = shutdown => Some(result),
                            _ = cancelled => None,
                        }
                    };
                    #[cfg(any(feature = "tokio", feature = "std_thread"))]
                    {
                        crate::executor::block_on(future)
                    }
                    #[cfg(not(any(feature = "tokio", feature = "std_thread")))]
                    {
                        tokio::runtime::Handle::current().block_on(future)
                    }
                };

                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
                    Ok(None) => worker.0.control.lock().report.callback_timed_out(),
                    Ok(Some(Ok(()))) => {}
                    Ok(Some(Err((_, text)))) => {
                        worker.0.fail(crate::AktorFailureInput::new(
                            crate::AktorFailureSource::ShutdownCallback,
                            text,
                        ));
                    }
                    Err(payload) => {
                        worker.0.fail(crate::AktorFailureInput::new(
                            crate::AktorFailureSource::ShutdownCallback,
                            crate::panic::panic_tree(&payload).er_report(),
                        ));
                        super::shutdown::contain_drop(
                            payload,
                            &worker.0,
                            crate::AktorFailureSource::ShutdownCallback,
                        );
                    }
                }
            })
            .await
            {
                kill.fail(crate::AktorFailureInput::new(
                    crate::AktorFailureSource::GroupDriver,
                    error.er_report(),
                ));
            }
            Ok(())
        })
    })
}

struct CallbackWorker(super::AktorKillSwitch);
impl CallbackWorker {
    fn new(kill: super::AktorKillSwitch) -> Self {
        kill.control.lock().callback_workers += 1;
        Self(kill)
    }
}
impl Drop for CallbackWorker {
    fn drop(&mut self) {
        self.0.control.lock().callback_workers -= 1;
        self.0.control.application_changed.send_replace(());
        super::native::poke();
    }
}
