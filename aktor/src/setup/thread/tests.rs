use super::*;
use crate::{AktorSetup, ShutdownReport, executor};
use std::sync::atomic::{AtomicUsize, Ordering};

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
        let callback = Arc::new(Mutex::new(None::<ShutdownReport>));
        let report = callback.clone();

        executor::FAIL_SPAWN.with(|setting| setting.set(Some("aktor interval")));
        executor::FAIL_SPAWN_AFTER.with(|setting| setting.set(earlier));

        let startup = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("interval owner"),
                role: AktorNoRole,
                kind: kind::StdThread,
                closures: AktorClosures {
                    start: async || Ok(()),
                    end: async move |()| {
                        count.fetch_add(1, Ordering::SeqCst);
                    },
                    intervals: (0..=earlier)
                        .map(|_| AktorInterval {
                            every: Duration::from_secs(3600),
                            run: (async |_: &mut ()| {}).into(),
                        })
                        .collect(),
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            },
            shutdown: move |finished| *report.lock() = Some(finished),
            options: Default::default(),
        });
        let completion = startup.completion.clone();

        executor::block_on(async {
            let error = startup.await.err().unwrap();

            assert!(
                error
                    .diagnostics
                    .report
                    .contains("injected scheduling failure")
            );
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);

            let completed = completion.wait().await;
            let callback = callback.lock();

            for report in [&completed, callback.as_ref().unwrap()] {
                assert!(report.failed(), "interval launch failure looked successful");
                assert!(!report.timed_out);

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
