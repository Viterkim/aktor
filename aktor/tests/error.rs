use aktor::{AktorDiagnostic, AktorError, AktorFailure};

#[test]
fn diagnostics() {
    struct Payload;

    let text = "database refused connection";
    let err = AktorError::new(text, Payload);

    assert_eq!(err.to_string(), "actor lifecycle failed");
    assert_eq!(format!("{err:#}"), text);
    assert_eq!(format!("{err:?}"), err.to_string());

    let tree = er::ErTree::new(std::fmt::Error, [err]);
    assert!(!tree.er_report_string().contains(text));

    let failure = AktorFailure::from(AktorError::new(text, ()));
    assert_eq!(failure.to_string(), "actor lifecycle failed");
    assert_eq!(format!("{failure:#}"), text);
    assert_eq!(format!("{failure:?}"), failure.to_string());
    assert!(!er::ErTree::from(failure).er_report_string().contains(text));

    fn saved(message: &str) -> Result<(), AktorDiagnostic> {
        use er::*;

        Err::<(), _>(ErTree::from(std::io::Error::other(message.to_owned())))
            .er_into(|report| (report, "open database"))
    }

    fn with_data() -> Result<(), AktorError<Payload>> {
        use er::*;

        Err::<(), _>(ErTree::from(std::io::Error::other("payload sentinel")))
            .er_into(|report| (report, Payload))
    }

    use er::*;

    #[derive(Er)]
    struct OpenErr;

    #[derive(Er)]
    struct PreferencesErr;

    let first = saved("first sentinel")
        .er::<OpenErr>(())
        .er::<PreferencesErr>(());
    let second = saved("second sentinel")
        .er::<OpenErr>(())
        .er::<PreferencesErr>(());
    let combined: ErResult<(), PreferencesErr> = er_all!((), [first, second]);
    let err = combined.unwrap_err();

    assert!(!err.er_report_string().contains("sentinel"));

    let mut reports = String::new();

    for diagnostic in err.er_find_all::<AktorDiagnostic>() {
        reports.push_str(&diagnostic.report);
    }

    for message in ["first sentinel", "second sentinel"] {
        assert_eq!(reports.matches(message).count(), 1);
    }

    let err = with_data().unwrap_err();
    assert!(err.diagnostics.contains("payload sentinel"));
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[test]
fn group_rejections() {
    let mut group = aktor::AktorGroup::new();
    group.on_shutdown(|_| ()).unwrap();

    for (error, reason) in [
        (
            group.on_shutdown(|_| ()).err().unwrap(),
            "already has a shutdown closure",
        ),
        (group.start().err().unwrap(), "needs a Tokio runtime"),
        (
            group.spawn_task(async {}).err().unwrap(),
            "need a Tokio runtime",
        ),
    ] {
        assert!(er::ErTree::from(error).er_report_string().contains(reason));
    }
}

#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
#[test]
fn thread() {
    use aktor::{
        AktorSetupError,
        listener::{CleanupErrors, ReplaceError},
        owner::{OwnerError, OwnerFailure},
    };
    use std::sync::Arc;

    let failure = Arc::new(AktorFailure::from(AktorError::new("cleanup sentinel", ())));
    let wrapped = ReplaceError::<AktorSetupError, AktorFailure>::Cleanup(Arc::clone(&failure));

    assert_eq!(wrapped.to_string(), "ReplaceError::Cleanup");
    assert!(
        !er::ErTree::from(wrapped)
            .er_report_string()
            .contains("cleanup sentinel")
    );

    let errors = CleanupErrors {
        errors: vec![failure],
        diagnostics: vec![AktorError::new("cleanup drop sentinel", ())],
    };
    assert_eq!(errors.to_string(), "2 actor cleanup failure(s)");

    let err = OwnerError::from(OwnerFailure::Cleanup(errors));
    assert_eq!(err.to_string(), "actor cleanup failed 2 time(s)");

    let output = format!("{err:#}");
    assert_eq!(output.matches("cleanup sentinel").count(), 1);
    assert_eq!(output.matches("cleanup drop sentinel").count(), 1);
    assert!(!format!("{err:?}").contains("sentinel"));

    assert!(
        !er::ErTree::from(err)
            .er_report_string()
            .contains("sentinel")
    );
}

#[cfg(feature = "local")]
#[test]
fn local() {
    use aktor::local::{OwnerFailure, SharedOwnerError};
    use std::rc::Rc;

    struct Payload;

    for (err, message) in [
        (
            OwnerFailure::Setup(AktorError::new("setup sentinel", Payload)),
            "actor setup failed",
        ),
        (
            OwnerFailure::Cleanup(AktorError::new("cleanup sentinel", Payload)),
            "actor cleanup failed",
        ),
    ] {
        let diagnostic = format!("{err:#}");
        assert!(diagnostic.contains("sentinel"));
        assert_eq!(err.to_string(), message);
        assert_eq!(format!("{err:?}"), message);

        let shared = SharedOwnerError::from_failure(Rc::new(err), &[]);
        let copy = shared.error.without_data();
        assert_eq!(format!("{copy:#}"), diagnostic);
        assert_eq!(shared.to_string(), message);
        assert_eq!(format!("{shared:#}"), diagnostic);
        assert_eq!(format!("{shared:?}"), message);
    }
}

#[cfg(feature = "wasm_browser_workers")]
#[test]
fn worker() {
    use aktor::{
        message::CallError,
        worker::{WorkerCause, WorkerError},
    };

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Payload;

    for mut err in [
        WorkerError::setup(CallError::NotAdmitted, "saved report sentinel"),
        WorkerError::cleanup(CallError::OutcomeUnknown, "saved report sentinel"),
        WorkerError::new(
            CallError::NotAdmitted,
            WorkerCause::Operations {
                expected: vec![],
                actual: vec!["saved report sentinel".into()],
            },
        ),
    ] {
        err.data = Some(Payload);
        let bytes = aktor::worker::encode(&err).unwrap();
        let err: WorkerError<Payload> = aktor::worker::decode(&bytes).unwrap();

        assert!(
            err.diagnostics
                .as_ref()
                .unwrap()
                .report
                .contains("saved report sentinel")
        );
        assert!(!err.to_string().contains("sentinel"));
        assert_eq!(
            format!("{err:#}").matches("saved report sentinel").count(),
            1
        );
        assert_eq!(format!("{err:?}"), err.to_string());
        assert!(
            !er::ErTree::from(err)
                .er_report_string()
                .contains("sentinel")
        );
    }
}
