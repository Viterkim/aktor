#[path = "../../../cli.rs"]
mod cli;

use aktor::*;
use cli::Report;
use core::num::NonZeroUsize;
use std::{cell::Cell, process::ExitCode, rc::Rc, sync::mpsc, thread, time::Duration};

struct Counter(Rc<Cell<u32>>);

#[aktor(latest)]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    counter.0.set(counter.0.get() + by);
    Ok(counter.0.get())
}

#[aktor]
async fn read(value: &u32) -> u32 {
    *value
}

fn check() -> Result<(), Report> {
    executor::block_on(async {
        let attempt = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "typed startup".into(),
                kind: AktorKind::StdThread,
                start: AktorInit::Try(async || {
                    Err::<Counter, _>(((), String::from("storage refused")))
                }),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        let failed = attempt.ready().await;

        match failed {
            Err((AktorStartError::Initializer { error: (), startup }, text)) => {
                assert!(text.contains("storage refused"));

                if !startup.report.as_ref().unwrap().failed() {
                    return Err("typed startup lost its rollback report".into());
                }
            }
            _ => return Err("typed startup lost its data".into()),
        }

        let caller = thread::current().id();
        let (end, ended) = mpsc::channel();
        let (tick, ticked) = mpsc::channel();
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: (
                AktorNew {
                    name: "counter".into(),
                    kind: AktorKind::StdThread,
                    start: AktorInit::Ready(async move || {
                        assert_ne!(thread::current().id(), caller);
                        Counter(Rc::new(Cell::new(0)))
                    }),
                    end: async move |counter: Counter| {
                        end.send(counter.0.get())
                            .map_err(|error| ((), error.to_string()))
                    },
                    hooks: AktorHooks {
                        intervals: vec![
                            AktorInterval::new(
                                Duration::from_millis(1),
                                async move |_: &mut Counter| {
                                    let _sent = tick.send(());
                                },
                            )
                            .unwrap(),
                        ],
                        before_each: (),
                        after_each: (),
                    },
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "sibling".into(),
                    kind: AktorKind::StdThread,
                    start: AktorInit::Ready(async || 0_u32),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        actors.ready().await?;

        ticked
            .recv_timeout(Duration::from_secs(1))
            .map_err(|error| Report(error.to_string()))?;

        assert_eq!(add(&actors.handles().0, 3).await.unwrap(), Ok(3));
        assert_eq!(
            add(&actors.handles().0, 0).await.unwrap(),
            Err("give me something")
        );

        let (input, mut output) = add::latest(&actors.handles().0);

        input.send(4);
        actors.handles().0.shutdown().await?;

        executor::sleep_until(std::time::Instant::now() + Duration::from_millis(10)).await;
        assert!(!actors.killswitch().is_stopping());
        assert_eq!(read(&actors.handles().1).await.unwrap(), 0);

        let report = actors.stop().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(output.next().await, Some(Ok(Ok(7))));
        assert_eq!(output.next().await, None);
        assert_eq!(ended.recv().unwrap(), 7);
        drop(input);
        Ok(())
    })
}

fn main() -> ExitCode {
    cli::exit(check())
}

#[cfg(test)]
mod tests {
    #[test]
    fn counter() {
        assert_eq!(super::main(), std::process::ExitCode::SUCCESS);
    }
}
