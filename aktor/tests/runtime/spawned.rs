use super::*;
use aktor::listener::Handle;
use core::num::NonZeroUsize;
use core::{future::poll_fn, task::Poll};
use futures_util::task::AtomicWaker;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn start<Actors: AktorStart<Group = AktorGroup>>(
    actors: Actors,
) -> AktorStarted<Actors::Handles, AktorGroup, Actors::Error> {
    aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors,
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
}

#[derive(Default)]
struct Wake {
    pending: AtomicBool,
    waker: AtomicWaker,
    ran: AtomicBool,
    inline: AtomicBool,
}

#[aktor]
async fn serve(wake: &mut Arc<Wake>) {
    let wake = wake.clone();

    aktor_spawn(
        async move {
            poll_fn(|cx| {
                wake.waker.register(cx.waker());

                match wake.pending.swap(false, Ordering::AcqRel) {
                    true => Poll::Ready(()),
                    false => Poll::Pending,
                }
            })
            .await;

            // An audio callback relies on this, a spawned Tokio task would put a lock on its wake.
            wake.inline
                .store(tokio::task::try_id().is_none(), Ordering::SeqCst);
            wake.ran.store(true, Ordering::SeqCst);
        },
        aktor::AktorJob::FinishBeforeEnd,
    )
    .unwrap();
}

// Work spawned on the owner runs inline next to its calls, woken from a thread outside Tokio.
#[tokio::test]
async fn woken_from_outside() {
    let wake = Arc::new(Wake::default());
    let state = wake.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "speaker".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async move || state),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(4).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    started.ready().await.map_err(|_| ()).unwrap();

    serve(started.handles()).await.unwrap();

    let outside = wake.clone();

    std::thread::spawn(move || {
        outside.pending.store(true, Ordering::Release);
        outside.waker.wake();
    })
    .join()
    .unwrap();

    while !wake.ran.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert!(wake.inline.load(Ordering::SeqCst));

    let _ = started.stop().await;
}

struct ThreadSelfRef {
    own: Option<Handle<ThreadSelfRef>>,
    job: Option<AktorOwnerTask<()>>,
}

#[aktor]
async fn thread_store_self(state: &mut ThreadSelfRef, handle: Handle<ThreadSelfRef>) {
    state.own = Some(handle);
}

#[aktor]
async fn thread_self_retain(_state: &mut ThreadSelfRef) {}

#[aktor]
async fn thread_spawn_self_caller(state: &mut ThreadSelfRef, handle: Handle<ThreadSelfRef>) {
    let task = aktor_spawn(
        async move {
            aktor_stopping().stopped().await;
            assert_eq!(
                thread_self_retain(&handle).await.unwrap_err().outcome,
                aktor::AktorDeliveryOutcome::NotAdmitted
            );
        },
        aktor::AktorJob::FinishBeforeEnd,
    )
    .unwrap();
    state.job = Some(task);
}

#[tokio::test]
async fn end_self_call_is_refused() {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "thread self call".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ThreadSelfRef {
                own: None,
                job: None,
            }),
            end: async |state: ThreadSelfRef| {
                let handle = state.own.unwrap();
                assert_eq!(
                    thread_self_retain(&handle).await.unwrap_err().outcome,
                    aktor::AktorDeliveryOutcome::NotAdmitted
                );
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(4).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(10),
    });
    started.ready().await.unwrap();

    thread_store_self(started.handles(), started.handles().handle())
        .await
        .unwrap();

    let report = tokio::time::timeout(Duration::from_secs(2), started.stop())
        .await
        .expect("self call kept end waiting");

    assert!(!report.failed(), "{report}");
}

#[tokio::test]
async fn joined_self_call_is_refused() {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "bodies".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ThreadSelfRef {
                own: None,
                job: None,
            }),
            end: async move |state: ThreadSelfRef| {
                if let Some(job) = state.job {
                    job.await.unwrap();
                }
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(4).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(80),
    });
    started.ready().await.unwrap();

    thread_spawn_self_caller(started.handles(), started.handles().handle())
        .await
        .unwrap();

    started.handles().shutdown().wait().await.unwrap();
    let report = started.stop().await;
    assert!(!report.failed(), "{report}");
}

#[tokio::test]
async fn retire_from_spawned() {
    let (fail, failed) = tokio::sync::oneshot::channel::<()>();
    let (ended, end_saw) = tokio::sync::oneshot::channel();

    let _started = start(AktorNew {
        name: "video".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(move || {
            core::future::ready(
                aktor_spawn_local(
                    async move {
                        let _ = failed.await;
                        aktor_retire();
                        Ok::<_, ((), String)>("device lost")
                    },
                    aktor::AktorJob::FinishBeforeEnd,
                )
                .unwrap(),
            )
        }),
        end: async move |work: AktorOwnerTask<&'static str>| {
            let _ = ended.send(work.await);
        },
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });

    fail.send(()).unwrap();

    assert_eq!(end_saw.await.unwrap(), Ok("device lost"));
}

#[tokio::test]
async fn leave_end() {
    let (release, released) = tokio::sync::oneshot::channel::<()>();

    let (final_report, received) = tokio::sync::oneshot::channel();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "slow end".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ()),
            end: async move |()| {
                released.await.unwrap();
                Err::<(), _>(((), String::from("late end")))
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Leave,
        },
        shutdown: move |report| {
            let _ = final_report.send(report);
        },
        grace: Duration::from_millis(50),
    });

    let completion = started.completion();
    started.ready().await.unwrap();
    drop(started);

    assert!(completion.wait().await.unwrap().ran_past_grace());

    release.send(()).unwrap();

    let report = received.await.unwrap();
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.error.contains("late end"))
    );
}

struct BrokenJob(bool);
impl Future for BrokenJob {
    type Output = ();

    fn poll(self: core::pin::Pin<&mut Self>, _: &mut core::task::Context<'_>) -> Poll<()> {
        if self.0 {
            panic!("job failed")
        }
        Poll::Pending
    }
}
impl Drop for BrokenJob {
    fn drop(&mut self) {
        panic!("job failed")
    }
}

#[tokio::test]
async fn independent_job_panics() {
    let (receipt, job) = tokio::sync::oneshot::channel();
    let group = start(AktorNew {
        name: "jobs".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(async move || {
            aktor_spawn(BrokenJob(false), aktor::AktorJob::FinishBeforeEnd).unwrap();
            let panicking = aktor_spawn(BrokenJob(true), aktor::AktorJob::FinishBeforeEnd).unwrap();
            let _ = receipt.send(panicking);
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(2).unwrap(),
        stuck: AktorStuck::Kill,
    });

    let report = group
        .completion()
        .wait_retired()
        .await
        .unwrap()
        .into_report();
    let (aktor::AktorJobError::Failed(id), _) = job.await.unwrap().await.unwrap_err() else {
        panic!("job lost its panic");
    };
    assert!(report.failure(id).unwrap().delivered());
    assert_eq!(
        report
            .failures
            .iter()
            .filter(|failure| failure.error.contains("job failed"))
            .count(),
        3
    );
}

#[tokio::test]
async fn drop_stops() {
    let (stopped, saw_stop) = tokio::sync::oneshot::channel();

    let started = start(AktorNew {
        name: "owned".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(move || {
            let mut stopping = aktor_stopping();

            core::future::ready(
                aktor_spawn_local(
                    async move {
                        stopping.stopped().await;
                        let _ = stopped.send(());
                    },
                    aktor::AktorJob::FinishBeforeEnd,
                )
                .unwrap(),
            )
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });

    started.ready().await.unwrap();
    drop(started);

    assert!(saw_stop.await.is_ok());
}

#[tokio::test]
async fn retire_in_start() {
    let thread = start(AktorNew {
        name: "thread".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(async || {
            aktor_retire();
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });
    let task = start(AktorNew {
        name: "task".into(),
        kind: AktorKind::TokioTask,
        start: aktor::AktorInit::Ready(async || {
            aktor_retire();
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });

    assert!(thread.ready().await.is_ok());
    assert!(task.ready().await.is_ok());
}

#[tokio::test]
async fn external_jobs() {
    let (actor, group) = thread_actor((), 1).await;
    let (entered, running) = tokio::sync::oneshot::channel();
    let (release, waiting) = tokio::sync::oneshot::channel();

    let receipt = actor
        .spawn(
            async move {
                entered.send(()).unwrap();
                waiting.await.unwrap();
                Ok::<_, ((), String)>(7)
            },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();
    running.await.unwrap();
    assert!(matches!(
        actor.spawn(async {}, AktorJob::DropAtStop),
        Err((AktorSpawnError::Full, _))
    ));

    release.send(()).unwrap();
    assert_eq!(receipt.await.unwrap(), 7);

    let job = actor
        .spawn(
            async { Err::<(), _>(((), String::from("write failed"))) },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();
    drop(job);
    let report = group.completion().wait().await.unwrap();
    assert_eq!(
        report
            .failures
            .iter()
            .filter(|failure| failure.error.contains("write failed"))
            .count(),
        1
    );
    assert!(matches!(
        actor.spawn(async {}, AktorJob::DropAtStop),
        Err((AktorSpawnError::Closing, _))
    ));
}

#[tokio::test]
async fn jobs_at_grace() {
    for stuck in [AktorStuck::Kill, AktorStuck::Leave] {
        let (release, waiting) = tokio::sync::oneshot::channel::<()>();
        let (end, mut ended) = tokio::sync::oneshot::channel();
        let group = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "files".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || ()),
                end: async move |()| {
                    end.send(()).unwrap();
                },
                capacity: NonZeroUsize::MIN,
                hooks: AktorHooks::NONE,
                stuck,
            },
            shutdown: |_| {},
            stuck: AktorGroupStuck::KeepRunning,
            grace: Duration::from_millis(40),
        });
        group.ready().await.unwrap();
        let job = group
            .handles()
            .spawn(
                async move {
                    let _ = waiting.await;
                },
                AktorJob::FinishBeforeEnd,
            )
            .unwrap();
        let _snapshot = group.stop().await;
        if stuck == AktorStuck::Kill {
            let Err((AktorJobError::Cut(id), _)) = job.await else {
                panic!("job was not cut")
            };
            assert!(
                group
                    .stop_released()
                    .await
                    .unwrap()
                    .into_report()
                    .failure(id)
                    .unwrap()
                    .error
                    .contains("spawned.rs")
            );
        } else {
            assert!(ended.try_recv().is_err());
            release.send(()).unwrap();
            job.await.unwrap();
            ended.await.unwrap();
        }
    }
}

#[tokio::test]
async fn blocking_loop_wake() {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let group = start(AktorLoop {
        name: "device".into(),
        kind: AktorKind::TokioThread,
        work: async move |stopping| {
            let (wake, wait) = std::sync::mpsc::channel();
            let guard = stopping.on_stop(|| panic!("unregistered"));
            drop(guard);
            let observed = stopping.clone();
            let _guard = stopping.on_stop(move || {
                assert!(observed.is_stopping());
                wake.send(std::thread::current().id()).unwrap();
                panic!("device wake");
            });
            entered.send(()).unwrap();
            assert_ne!(
                wait.recv_timeout(Duration::from_secs(2)).unwrap(),
                std::thread::current().id()
            );
            let (late, ran) = std::sync::mpsc::channel();
            let _late = stopping.on_stop(move || late.send(std::thread::current().id()).unwrap());
            assert_eq!(ran.try_recv().unwrap(), std::thread::current().id());
            Ok::<_, ((), String)>(())
        },
        stuck: AktorStuck::Leave,
    });
    waiting.await.unwrap();
    let id = group.handles().actor_id().unwrap();
    let report = group.stop().await;
    let mut failures = report.actor_failures(id);
    assert!(failures.next().unwrap().error.contains("device wake"));
    assert!(failures.next().is_none());
}

#[tokio::test]
async fn blocked_job_cut() {
    let (release, waiting) = std::sync::mpsc::channel::<()>();
    let (entered, running) = tokio::sync::oneshot::channel();
    let started = aktor_start(AktorSetup {
        actors: support::thread_actor((), 1),
        stuck: AktorGroupStuck::KeepRunning,
        grace: Duration::from_millis(40),
        shutdown: |_| {},
    });
    started.ready().await.unwrap();
    let receipt = started
        .handles()
        .spawn(
            async move {
                entered.send(()).unwrap();
                waiting.recv().unwrap();
            },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();
    running.await.unwrap();

    let _snapshot = started.stop().await;
    let Err((AktorJobError::Cut(id), _)) = receipt.await else {
        panic!("blocked job missed its cut")
    };
    release.send(()).unwrap();
    assert!(
        started
            .stop_released()
            .await
            .unwrap()
            .into_report()
            .failure(id)
            .is_some()
    );
}

#[aktor]
async fn wait_for_refresh(_: &mut (), ready: tokio::sync::oneshot::Sender<()>) {
    let refresh = aktor_spawn(core::future::pending::<()>(), AktorJob::DropAtStop).unwrap();
    ready.send(()).unwrap();
    assert!(matches!(refresh.await, Err((AktorJobError::Dropped, _))));
}

#[tokio::test]
async fn drop_job_during_call() {
    let (actor, group) = thread_actor((), 1).await;
    let (ready, running) = tokio::sync::oneshot::channel();

    let reply = wait_for_refresh(&actor, ready).send().await.unwrap();
    running.await.unwrap();
    let stop = group.stop();
    reply.await.unwrap();
    assert!(!stop.await.failed());
}
