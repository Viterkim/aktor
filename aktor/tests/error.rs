use aktor::{AktorError, AktorFailure};

#[test]
fn diagnostics() {
    struct Payload;

    let text = "open database\n`- refused connection";
    let err = AktorError::new(text, Payload);

    assert_eq!(err.to_string(), "actor lifecycle failed: open database");
    assert_eq!(format!("{err:#}"), text);

    let tree = er::ErTree::new(std::fmt::Error, [err]);
    assert!(!tree.er_report().contains("refused"));

    let failure = AktorFailure::from(AktorError::new(text, ()));
    assert_eq!(failure.to_string(), "actor lifecycle failed: open database");

    fn with_data() -> Result<(), AktorError<Payload>> {
        use er::*;

        let tree = ErTree::from(std::io::Error::other("payload sentinel"));
        Err(tree.er_split_with(|(_, report)| AktorError::new(report, Payload)))
    }

    let err = with_data().unwrap_err();
    assert!(err.diagnostics.contains("payload sentinel"));
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[test]
fn group_rejections() {
    use aktor::AktorGroupError;

    let mut group = aktor::AktorGroup::new();
    group.on_shutdown(|_| ()).unwrap();

    assert!(matches!(
        group.on_shutdown(|_| ()),
        Err(AktorGroupError::ShutdownTaken)
    ));
    assert!(matches!(group.start(), Err((AktorGroupError::NoTokio, _))));
    assert!(matches!(
        group.spawn_task(async {}),
        Err(AktorGroupError::NoTokio)
    ));
}

#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
#[test]
fn thread() {
    use aktor::{listener::CleanupErrors, owner::OwnerError};
    use std::sync::Arc;

    let failure = Arc::new(AktorFailure::from(AktorError::new("cleanup sentinel", ())));
    let errors = CleanupErrors {
        errors: vec![failure],
        diagnostics: vec![AktorError::new("cleanup drop sentinel", ())],
    };

    let err = OwnerError::Cleanup(errors);
    assert_eq!(err.to_string(), "actor cleanup failed 2 time(s)");

    assert!(!format!("{err:?}").contains("sentinel"));
}

#[cfg(feature = "local")]
#[test]
fn local() {
    use aktor::local::{OwnerError, SharedOwnerError};
    use std::rc::Rc;

    struct Payload;

    for (err, message) in [
        (
            OwnerError::Setup(AktorError::new("setup sentinel", Payload)),
            "actor setup failed",
        ),
        (
            OwnerError::Cleanup(AktorError::new("cleanup sentinel", Payload)),
            "actor cleanup failed",
        ),
    ] {
        let diagnostic = format!("{err:#}");
        assert_eq!(err.to_string(), message);

        let (shared, report) = SharedOwnerError::from_failure(Rc::new(err), &[]);
        let copy = shared.cause.without_data();
        assert_eq!(format!("{copy:#}"), diagnostic);
        assert_eq!(report, diagnostic);
    }
}

#[cfg(feature = "wasm_browser_workers")]
#[test]
fn worker() {
    use aktor::{
        message::CallError,
        worker::{WorkerCause, WorkerFailure},
    };
    use er::*;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Payload;

    for mut err in [
        WorkerFailure::setup(
            CallError::NotAdmitted,
            "worker lifecycle failed\n`- saved report sentinel",
        ),
        WorkerFailure::cleanup(
            CallError::OutcomeUnknown,
            "worker lifecycle failed\n`- saved report sentinel",
        ),
        WorkerFailure::new(
            CallError::NotAdmitted,
            WorkerCause::Operations {
                expected: vec![],
                actual: vec!["saved report sentinel".into()],
            },
        ),
    ] {
        err.data = Some(Payload);
        let bytes = aktor::worker::encode(&err).unwrap();
        let err: WorkerFailure<Payload> = aktor::worker::decode(&bytes).unwrap();

        let failure: (aktor::worker::WorkerError<Payload>, String) = err.into();
        let report = Err::<(), _>(failure)
            .er(|| std::fmt::Error)
            .unwrap_err()
            .er_report();

        assert!(report.contains("saved report sentinel"));
    }
}
