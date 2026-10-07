#![cfg(all(feature = "tokio", not(target_family = "wasm")))]

#[cfg(feature = "macros")]
mod calls;

use aktor::*;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

#[path = "../support/mod.rs"]
pub mod support;

struct StopOnDrop {
    kill: KillSwitch,
    dropped: Arc<AtomicUsize>,
}
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
        self.kill.stop();
    }
}

#[test]
fn closed_runtime() {
    if std::env::var("AKTOR_CHILD").as_deref() != Ok("closed runtime") {
        let output = support::child("closed_runtime", "closed runtime");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let mut group = AktorGroup::with_grace(Duration::from_secs(3));
    let completion = {
        let _entered = runtime.enter();
        group.start_threaded().unwrap()
    };
    drop(runtime);

    let dropped = Arc::new(AtomicUsize::new(0));
    let held = StopOnDrop {
        kill: group.killswitch(),
        dropped: dropped.clone(),
    };
    {
        let _entered = handle.enter();
        group
            .spawn_task(async move {
                let _held = held;
                core::future::pending::<()>().await;
            })
            .unwrap();
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let report = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), completion.wait())
            .await
            .unwrap()
    });
    assert!(report.failed(), "{report}");
}

struct BrokenDrop;
impl Drop for BrokenDrop {
    fn drop(&mut self) {
        panic!("root capture destructor failed");
    }
}

#[tokio::test]
async fn root_drop() {
    let (ready, started) = oneshot::channel();
    let group = AktorGroup::new();
    let completion = group.completion();
    let task = tokio::spawn(group.run(
        async move |_| {
            let _held = BrokenDrop;
            ready.send(()).unwrap();
            core::future::pending::<()>().await;
            Ok::<(), AktorError>(())
        },
        async |_| Ok::<(), AktorCleanupError>(()),
    ));
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let report = tokio::time::timeout(Duration::from_secs(3), completion.wait())
        .await
        .unwrap();
    assert!(
        report
            .failure
            .unwrap()
            .message
            .contains("root capture destructor failed")
    );

    let held = BrokenDrop;
    let group = AktorGroup::new();
    let completion = group.completion();
    drop_before_poll(
        group.run(
            async move |_| {
                let _held = held;
                core::future::pending::<Result<(), AktorError>>().await
            },
            async |_| Ok::<(), AktorCleanupError>(()),
        ),
        completion,
    )
    .await;
}

async fn drop_before_poll(application: impl std::future::Future, completion: GroupCompletion) {
    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(application)));
    assert!(
        dropped.is_ok(),
        "unpolled application destruction escaped its owner"
    );
    let report = tokio::time::timeout(Duration::from_secs(3), completion.wait())
        .await
        .unwrap();
    assert!(
        report
            .failure
            .unwrap()
            .message
            .contains("root capture destructor failed")
    );
}

struct ApplicationError {
    kill: KillSwitch,
    panic: bool,
}
impl fmt::Display for ApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.panic {
            panic!("application error format failed");
        }
        self.kill.stop();
        formatter.write_str("application error")
    }
}

#[test]
fn application_error() {
    let case = std::env::var("AKTOR_CHILD").ok();
    let Some(case) = case.filter(|case| case == "reentrant display" || case == "panicking display")
    else {
        for case in ["reentrant display", "panicking display"] {
            let output = support::child("application_error", case);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut group = AktorGroup::new();
        let kill = group.killswitch();
        let panic = case == "panicking display";
        let report = group
            .start_with(async move |_| {
                let error = ApplicationError { kill, panic };
                Err::<(), _>(AktorCleanupError::new(error.to_string()))
            })
            .unwrap();
        group.killswitch().stop();
        let report = report.await;
        assert!(report.failed());
        if panic {
            assert!(
                report
                    .failure
                    .unwrap()
                    .message
                    .contains("application error format failed")
            );
        } else {
            assert!(
                report
                    .application
                    .iter()
                    .any(|error| error.diagnostics == "application error")
            );
        }
    });
}
