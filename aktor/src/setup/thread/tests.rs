use super::*;
use crate::{AktorSetup, AktorShutdownReport, executor};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        executor::FAIL_SPAWN.with(|setting| setting.set(None));
        executor::FAIL_SPAWN_AFTER.with(|setting| setting.set(0));
    }
}

#[test]
fn interval_failure() {
    for earlier in [0, 1] {
        let _reset = Reset;
        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        let callback = Arc::new(Mutex::new(None::<AktorShutdownReport>));
        let report = callback.clone();

        executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor interval")));
        executor::FAIL_SPAWN_AFTER.with(|setting| setting.set(earlier));

        let startup = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "interval owner".into(),
                kind: kind::StdThread,
                start: async || aktor_ready(()),
                end: async move |()| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
                hooks: AktorHooks {
                    intervals: (0..=earlier)
                        .map(|_| AktorInterval {
                            every: Duration::from_secs(3600),
                            run: (async |_: &mut ()| {}).into(),
                        })
                        .collect(),
                    before_each: None,
                    after_each: None,
                },
                capacity: 32,
                stuck: crate::AktorStuck::Kill,
            },
            shutdown: move |finished| *report.lock() = Some(finished),
            grace: Duration::from_secs(5),
        });
        let completion = startup.completion.clone();

        executor::block_on(async {
            let (_, error_report) = startup.await.err().unwrap();

            assert!(error_report.contains("injected scheduling failure"));
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);

            let completed = completion.wait().await;
            let callback = callback.lock();

            for report in [&completed, callback.as_ref().unwrap()] {
                assert!(report.failed(), "interval launch failure looked successful");
                assert!(!report.ran_past_grace);

                assert_eq!(
                    report
                        .to_string()
                        .matches("injected scheduling failure")
                        .count(),
                    1
                );
            }
        });
    }
}
