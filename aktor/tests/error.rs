use aktor::{AktorError, AktorFailure};

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

    fn saved() -> Result<(), AktorError> {
        use er::*;

        Err::<(), _>(ErTree::from(std::io::Error::other("boundary sentinel")))
            .er_into(|report| report)
    }

    fn with_data() -> Result<(), AktorError<Payload>> {
        use er::*;

        Err::<(), _>(ErTree::from(std::io::Error::other("payload sentinel")))
            .er_into(|report| (report, Payload))
    }

    let err = saved().unwrap_err();
    assert!(err.diagnostics.contains("boundary sentinel"));

    let err = with_data().unwrap_err();
    assert!(err.diagnostics.contains("payload sentinel"));
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
        owner::OwnerError,
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

    let err = OwnerError::Cleanup(errors);
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
        assert!(diagnostic.contains("sentinel"));
        assert_eq!(err.to_string(), message);
        assert_eq!(format!("{err:?}"), message);

        let shared = SharedOwnerError {
            error: Rc::new(err),
        };
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

    struct Payload;

    for cause in [
        WorkerCause::Setup("saved report sentinel".into()),
        WorkerCause::Cleanup("saved report sentinel".into()),
    ] {
        let err = WorkerError {
            outcome: CallError::NotAdmitted,
            cause,
            data: Some(Payload),
        };
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
