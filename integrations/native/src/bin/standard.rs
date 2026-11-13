use aktor::*;
use std::{cell::Cell, rc::Rc, sync::mpsc, thread, time::Duration};

struct Counter(Rc<Cell<u32>>);

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    counter.0.set(counter.0.get() + by);
    Ok(counter.0.get())
}

pub fn check() -> Result<(), String> {
    executor::block_on(async {
        let failed = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "typed startup".into(),
                kind: AktorKind::StdThread,
                start: async || Err::<Counter, _>(String::from("storage refused")),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        })
        .await;

        match failed {
            Err((
                AktorStartError {
                    cause: AktorStartCause::Startup(error),
                    report: Some(report),
                    ..
                },
                _,
            )) => {
                let listener::DedicatedStartError::Init(failure) = error.cause.primary() else {
                    return Err("typed startup lost its original failure".into());
                };

                assert_eq!(failure.diagnostics, "storage refused");
                if !report.failed() {
                    return Err("typed startup lost its rollback report".into());
                }
            }
            _ => return Err("typed startup lost its data".into()),
        }

        let caller = thread::current().id();
        let (end, ended) = mpsc::channel();
        let (tick, ticked) = mpsc::channel();
        let actors = aktor_start(AktorSetup {
            actors: (
                AktorNew {
                    name: "counter".into(),
                    kind: AktorKind::StdThread,
                    start: async move || {
                        assert_ne!(thread::current().id(), caller);
                        aktor_ready(Counter(Rc::new(Cell::new(0))))
                    },
                    end: async move |counter: Counter| {
                        end.send(counter.0.get()).map_err(|error| error.to_string())
                    },
                    hooks: AktorHooks {
                        intervals: vec![AktorInterval {
                            every: Duration::from_millis(1),
                            run: (async move |_: &mut Counter| {
                                let _sent = tick.send(());
                            })
                            .into(),
                        }],
                        before_each: None,
                        after_each: None,
                    },
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "sibling".into(),
                    kind: AktorKind::StdThread,
                    start: async || aktor_ready(0_u32),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        })
        .await
        .map_err(|(_, report)| report)?;

        ticked
            .recv_timeout(Duration::from_secs(1))
            .map_err(|error| error.to_string())?;
        assert_eq!(add(&actors.handles.0, 3).await, Ok(3));
        assert_eq!(add(&actors.handles.0, 0).await, Err("give me something"));

        let (input, mut output) = add::latest(&actors.handles.0);

        input.send(4);
        actors
            .handles
            .0
            .shutdown()
            .await
            .map_err(|(_, report)| report)?;
        executor::sleep_until(std::time::Instant::now() + Duration::from_millis(10)).await;
        assert!(!actors.killswitch().is_stopping());
        assert_eq!(
            message::call(&actors.handles.1.handle, |value, ()| *value, ()).await,
            0
        );

        let report = actors.shutdown().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(output.next().await, Some(Ok(7)));
        assert_eq!(output.next().await, None);
        assert_eq!(ended.recv().unwrap(), 7);
        drop(input);
        Ok(())
    })
}

fn main() -> Result<(), String> {
    check()
}

#[cfg(test)]
mod tests {
    #[test]
    fn counter() {
        super::check().unwrap();
    }
}
