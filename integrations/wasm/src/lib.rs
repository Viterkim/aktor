use aktor::*;
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use er::*;
use futures_util::future::join;
use std::{
    cell::{Cell, RefCell},
    future::{Future, poll_fn},
    rc::Rc,
    task::{Context, Poll, Waker},
};

struct State {
    text: String,
}

struct Line(String);

#[derive(Er)]
struct AppendErr;

#[aktor]
async fn append(state: &mut State, line: Line) -> ErResult<String, AppendErr> {
    if line.0.is_empty() {
        er_bail!(AppendErr);
    }

    state.text.push_str(&line.0);
    Ok(state.text.clone())
}

#[aktor]
async fn hold(
    state: &mut State,
    started: Rc<Signal<NoopRawMutex, ()>>,
    release: Rc<Signal<NoopRawMutex, ()>>,
) -> ErResult<String, AppendErr> {
    started.signal(());
    release.wait().await;
    append(state, Line("a".into())).await
}

async fn yield_once() {
    let mut yielded = false;

    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[cfg(target_family = "wasm")]
struct Clock;
#[cfg(target_family = "wasm")]
static TICKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(target_family = "wasm")]
impl embassy_time_driver::Driver for Clock {
    fn now(&self) -> u64 {
        TICKS.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn schedule_wake(&self, _: u64, waker: &Waker) {
        waker.wake_by_ref();
    }
}
#[cfg(target_family = "wasm")]
embassy_time_driver::time_driver_impl!(static CLOCK:Clock=Clock);

async fn listening() -> Result<(), &'static str> {
    let mut actors = embassy::AktorGroup::new();
    let kill = actors.killswitch();
    let closing = actors
        .listen_with(async |_| Ok::<_, AktorError>(()))
        .map_err(|_| "listener")?;
    let setup = async || {
        Ok::<_, AktorError>(State {
            text: String::new(),
        })
    };

    let cleanup = async |_| Ok::<_, AktorError>(());
    let mut args = ActorArgs::new("lines", setup, cleanup);

    args.capacity = 1;

    let actor = actors.spawn::<State, 1, ()>(args).map_err(|_| "actor")?;
    let work = async {
        let value = append(&actor, Line("ordinary startup".into()))
            .await
            .unwrap_report();

        drop(actors);
        value
    };

    let (report, value) = join(closing, work).await;

    if !kill.is_stopping() {
        return Err("dropped group did not close");
    }

    if report.failed() || value != "ordinary startup" || report.actors.len() != 1 {
        return Err("listener shutdown");
    }

    Ok(())
}

async fn grouped() -> Result<(), &'static str> {
    let cleaned = Rc::new(Cell::new(false));
    let cleanup = cleaned.clone();
    let value = embassy::AktorGroup::new()
        .run(
            async move |group| {
                let actor = group
                    .spawn::<State, 1, ()>(ActorArgs {
                        name: "lines".into(),
                        capacity: 1,
                        setup: async || {
                            Ok(State {
                                text: String::new(),
                            })
                        },
                        cleanup: async move |_| {
                            cleanup.set(true);
                            Ok(())
                        },
                    })
                    .unwrap();

                let (sender, mut results) = append::latest(&actor);

                sender.send(Line("b".into()));
                sender.send(Line("c".into()));
                drop(sender);
                assert_eq!(results.next().await.unwrap().unwrap_report(), "c");
                assert!(results.next().await.is_none());
                Ok::<_, AktorError>(17)
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .map_err(|_| "local group")?;

    if value != Some(17) || !cleaned.get() {
        return Err("local group cleanup");
    }

    let timed = embassy::AktorGroup::with_grace(core::time::Duration::from_micros(10))
        .run(
            async |group| {
                let actor = group
                    .spawn::<State, 1, ()>(ActorArgs {
                        name: "stuck cleanup".into(),
                        capacity: 1,
                        setup: async || {
                            Ok(State {
                                text: String::new(),
                            })
                        },
                        cleanup: async |_| {
                            core::future::pending::<()>().await;
                            Ok(())
                        },
                    })
                    .unwrap();

                append(&actor, Line("a".into())).await.unwrap_report();
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .err()
        .ok_or("group timeout")?;

    if !timed.timed_out {
        return Err("group timeout outcome");
    }

    Ok(())
}

async fn timeout_limits() -> Result<(), &'static str> {
    while embassy_time::Instant::now().as_ticks() == 0 {
        yield_once().await;
    }

    let (actor, owner) = embassy::channel::<State, 1, ()>().map_err(|_| "timeout channel")?;
    let mut reply = append::request(&actor, Line("kept".into()))
        .try_send()
        .map_err(|_| "timeout admission")?;
    let mut context = Context::from_waker(Waker::noop());

    let mut request =
        Box::pin(append(&actor, Line("unqueued".into())).timeout(std::time::Duration::MAX));

    for _ in 0..2 {
        if request.as_mut().poll(&mut context).is_ready() {
            return Err("huge request timeout expired");
        }
    }

    drop(request);

    let mut timeout = Box::pin(reply.timeout(std::time::Duration::MAX));

    for _ in 0..2 {
        if timeout.as_mut().poll(&mut context).is_ready() {
            return Err("huge reply timeout expired");
        }
    }

    drop(timeout);

    #[cfg(target_family = "wasm")]
    for duration in [
        std::time::Duration::from_nanos(1),
        std::time::Duration::from_millis(1),
    ] {
        let ticks = duration
            .as_nanos()
            .saturating_mul(u128::from(embassy_time::TICK_HZ))
            .div_ceil(1_000_000_000) as u64;
        let mut timeout = Box::pin(reply.timeout(duration));

        if timeout.as_mut().poll(&mut context).is_ready() {
            return Err("positive timeout expired on first poll");
        }

        TICKS.fetch_add(ticks - 1, std::sync::atomic::Ordering::Relaxed);

        if timeout.as_mut().poll(&mut context).is_ready() {
            return Err("positive timeout lost its clock tick");
        }

        TICKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let Poll::Ready(Err(error)) = timeout.as_mut().poll(&mut context) else {
            return Err("positive timeout did not expire");
        };

        if !error.admitted {
            return Err("reply admission lost");
        }
    }

    let error = append(&actor, Line("unqueued".into()))
        .timeout(std::time::Duration::ZERO)
        .await
        .err()
        .ok_or("zero request timeout")?;

    if error.admitted {
        return Err("full request was admitted");
    }

    let error = reply
        .timeout(std::time::Duration::ZERO)
        .await
        .err()
        .ok_or("zero reply timeout")?;

    if !error.admitted {
        return Err("reply was not admitted");
    }

    drop(reply);
    drop(actor.shutdown());
    owner
        .run(
            State {
                text: String::new(),
            },
            async |state| {
                if state.text == "kept" {
                    Ok(())
                } else {
                    Err(AktorCleanupError::new("timed out operation was lost"))
                }
            },
        )
        .await
        .map_err(|_| "timeout ownership")?;
    Ok(())
}

async fn local_setup() -> Result<(), &'static str> {
    use aktor::message::LocalFuture;

    let tasks: Rc<RefCell<Vec<LocalFuture<'static, ()>>>> = Rc::new(RefCell::new(Vec::new()));
    let spawner = tasks.clone();
    let cleaned = Rc::new(Cell::new(false));
    let ended = cleaned.clone();
    let work = async move {
        let actors = start(AktorSetup {
            name: AktorName::new("host executor"),
            role: AktorNoRole,
            kind: AktorKind::Local::<local::clock::Embassy>(move |future| {
                spawner.borrow_mut().push(future);
                Ok(())
            }),
            closures: AktorClosures {
                start: async || {
                    Ok(State {
                        text: String::new(),
                    })
                },
                end: Some(
                    (async move |_: State| {
                        ended.set(true);
                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: None,
        })
        .await
        .map_err(|_| "host setup")?;

        let (input, mut output) = append::latest(&actors.handles);

        input.send(Line("a".into()));
        input.send(Line("b".into()));

        let latest = output
            .next()
            .await
            .ok_or("host latest ended")?
            .unwrap_report();

        if latest != "b" {
            return Err("host latest input");
        }

        if append(&actors.handles, Line("c".into()))
            .await
            .unwrap_report()
            != "bc"
        {
            return Err("host ordinary input");
        }

        let report = actors.shutdown().await;

        if report.failed() || report.actors[0].kind != Some(AktorExecution::Local) {
            return Err("host cleanup report");
        }

        Ok(())
    };

    let mut work = Box::pin(work);

    poll_fn(|cx| {
        let result = work.as_mut().poll(cx);
        let mut active = core::mem::take(&mut *tasks.borrow_mut());

        active.retain_mut(|task| task.as_mut().poll(cx).is_pending());
        tasks.borrow_mut().extend(active);
        result
    })
    .await?;

    if !cleaned.get() {
        return Err("host cleanup");
    }

    Ok(())
}

async fn exercise() -> Result<(), &'static str> {
    local_setup().await?;
    timeout_limits().await?;

    let (actor, owner) = embassy::channel::<State, 1, &'static str>().map_err(|_| "channel")?;
    let cleaned = Rc::new(Cell::new(false));
    let cleanup = cleaned.clone();
    let run = owner.run_with(
        async || {
            yield_once().await;
            Ok(State {
                text: String::new(),
            })
        },
        async move |state| {
            yield_once().await;
            cleanup.set(state.text == "abcd");
            Ok(())
        },
    );

    let client = async {
        actor.ready().await.map_err(|_| "setup")?;

        let error = append(&actor, Line(String::new()))
            .await
            .err()
            .ok_or("domain error")?;

        if !error.er_report_string().contains("AppendErr") {
            return Err("diagnostics");
        }

        let started = Rc::new(Signal::new());
        let release = Rc::new(Signal::new());
        let running = hold::request(&actor, started.clone(), release.clone())
            .send()
            .await;

        started.wait().await;

        drop(append::request(&actor, Line("b".into())).send().await);

        let waiting = match append::request(&actor, Line("c".into())).try_send() {
            Err(aktor::message::TrySendError::Full(request)) => request,
            _ => return Err("queue pressure"),
        };

        release.signal(());

        if running.await.map_err(|_| "running call")? != "a" {
            return Err("running output");
        }

        if waiting.send().await.await.map_err(|_| "queued call")? != "abc" {
            return Err("queued output");
        }

        let last = append::request(&actor, Line("d".into())).send().await;
        let completed = actor.shutdown();
        let observer = completed.new_observer();

        drop(actor);

        if last.await.map_err(|_| "last call")? != "abcd" {
            return Err("last output");
        }

        (&completed).await.map_err(|_| "borrowed completion")?;
        observer.await.map_err(|_| "completion observer")?;
        completed.await.map_err(|_| "completion")?;
        Ok(())
    };

    let (owner, client) = join(run, client).await;

    owner.map_err(|_| "owner")?;
    client?;

    if !cleaned.get() {
        return Err("cleanup");
    }

    let (actor, owner) =
        embassy::channel::<State, 1, ErTree<AppendErr>>().map_err(|_| "cleanup channel")?;
    let completion = actor.shutdown();
    let result = owner
        .run(
            State {
                text: String::new(),
            },
            async |_| {
                let tree = ErTree::new(AppendErr, ["backup folder is read only"]);

                Err(AktorCleanupError {
                    diagnostics: tree.er_report_string(),
                    data: tree,
                })
            },
        )
        .await;

    let error = result.err().ok_or("cleanup failure")?;

    if !error.to_string().contains("backup folder is read only") {
        return Err("cleanup diagnostics");
    }

    let embassy::OwnerError::Cleanup(cleanup) = &*error else {
        return Err("cleanup kind");
    };

    if cleanup.data.er_snapshot().entries.len() != 2 {
        return Err("cleanup structure");
    }

    let observed = completion.await.err().ok_or("cleanup observation")?;

    if !Rc::ptr_eq(&error, &observed) {
        return Err("retained cleanup failure");
    }

    listening().await?;
    grouped().await?;
    Ok(())
}

#[cfg(target_family = "wasm")]
pub fn check() -> Result<(), &'static str> {
    let mut exercise = Box::pin(exercise());
    let mut context = Context::from_waker(Waker::noop());

    for _ in 0..128 {
        match exercise.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => {
                TICKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    Err("executor did not finish")
}

#[cfg(not(target_family = "wasm"))]
pub fn check() -> Result<(), &'static str> {
    use std::{
        sync::Arc,
        task::Wake,
        thread,
        time::{Duration, Instant},
    };

    struct Wakeup(thread::Thread);
    impl Wake for Wakeup {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let waker = Waker::from(Arc::new(Wakeup(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut exercise = Box::pin(exercise());

    loop {
        if let Poll::Ready(result) = exercise.as_mut().poll(&mut context) {
            return result;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return Err("executor did not finish");
        }

        thread::park_timeout(remaining);
    }
}

#[cfg(target_family = "wasm")]
#[unsafe(no_mangle)]
pub extern "C" fn aktor_check() -> u32 {
    u32::from(check().is_err())
}

#[cfg(test)]
mod tests {
    #[test]
    fn local() {
        for _ in 0..4 {
            assert_eq!(super::check(), Ok(()));
        }
    }
}
