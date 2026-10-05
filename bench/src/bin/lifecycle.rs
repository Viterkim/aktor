use aktor::{
    setup::{AktorStart, AktorStartContext},
    *,
};
use std::{collections::BTreeMap, fs, future::Future, pin::Pin, thread, time::Duration};

struct Resource {
    thread: thread::ThreadId,
    calls: usize,
}

fn threads() -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();

    for task in fs::read_dir("/proc/self/task").unwrap() {
        if let Ok(name) = fs::read_to_string(task.unwrap().path().join("comm")) {
            *counts.entry(name.trim().to_owned()).or_default() += 1;
        }
    }

    counts
}

struct Batch {
    count: usize,
    intervals: bool,
}
impl AktorStart for Batch {
    type Error = AktorStartError;
    type Group = AktorGroup;
    type Handles = Vec<Aktor<Resource, AktorSetupError, AktorCleanupError>>;
    type Startup = Pin<Box<dyn Future<Output = Result<Self::Handles, AktorStartError>> + Send>>;

    fn grace(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start_standard().map(|_| ())
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        Box::pin(async move {
            let mut owners = Vec::new();

            for _ in 0..self.count {
                let setup = AktorSetup {
                    name: AktorName::new("resource"),
                    role: AktorNoRole,
                    kind: AktorKind::StdThread,
                    closures: AktorClosures {
                        start: async || {
                            Ok(Resource {
                                thread: thread::current().id(),
                                calls: 0,
                            })
                        },
                        end: Some(
                            (async |state: Resource| {
                                assert_eq!(state.thread, thread::current().id());
                                assert_eq!(state.calls, 33);
                                Ok(())
                            })
                            .into(),
                        ),
                        intervals: if self.intervals {
                            vec![AktorInterval {
                                every: Duration::from_secs(3600),
                                run: (async |state: &mut Resource| {
                                    assert_eq!(state.thread, thread::current().id());
                                })
                                .into(),
                            }]
                        } else {
                            vec![]
                        },
                        before_each: None,
                        after_each: None,
                    },
                    options: None,
                };

                owners.push(AktorStart::start_in(setup, context.clone()).await?);
            }

            Ok(owners)
        })
    }
}

fn main() {
    let count = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
    let intervals = std::env::args()
        .nth(2)
        .is_some_and(|arg| arg == "intervals");
    let initial = threads().values().sum::<usize>();

    executor::block_on(async {
        for cycle in 0..2 {
            let actors = start(Batch { count, intervals }).await.unwrap();
            let active = threads();

            println!(
                "{count} actors, intervals={intervals}, cycle={cycle}: {} threads above baseline, {active:?}",
                active.values().sum::<usize>() - initial
            );

            let mut gates = Vec::new();

            for owner in &actors.handles {
                let entered = std::sync::Arc::new(tokio::sync::Notify::new());
                let started = entered.clone();
                let release = std::sync::Arc::new(tokio::sync::Notify::new());
                let gate = release.clone();

                message::call_async(
                    &owner.handle,
                    async move |state: &mut Resource, ()| {
                        assert_eq!(state.thread, thread::current().id());
                        state.calls += 1;
                        started.notify_one();
                        gate.notified().await;
                    },
                    (),
                )
                .cast()
                .await;
                entered.notified().await;

                for _ in 0..32 {
                    message::call(&owner.handle, |state, ()| state.calls += 1, ())
                        .try_cast()
                        .unwrap();
                }

                gates.push(release);
            }

            let began = std::time::Instant::now();
            let closing = actors.shutdown();

            for gate in gates {
                gate.notify_one();
            }

            assert!(!closing.await.failed());
            println!(
                "shutdown drained {} admitted calls in {:?}",
                count * 33,
                began.elapsed()
            );
            drop(actors);

            let deadline = std::time::Instant::now() + Duration::from_secs(2);

            while threads()
                .keys()
                .any(|name| name.starts_with("aktor ") || name == "resource")
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "helper thread survived shutdown"
                );
                thread::yield_now();
            }
        }

        let failed = start(AktorSetup {
            name: AktorName::new("failed resource"),
            role: AktorNoRole,
            kind: AktorKind::StdThread,
            closures: AktorClosures::new(async || {
                Err::<Resource, _>(AktorSetupError::new("setup refused"))
            }),
            options: None,
        })
        .await;

        assert!(failed.is_err());
    });
}
