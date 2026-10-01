use super::*;
use aktor::listener::spawn_async;
use std::rc::Rc;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::test]
async fn local() {
    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "local".into(),
        capacity: 1,
        failure: FailurePolicy::Abort,
        setup: || {
            assert_eq!(std::thread::current().name(), Some("local"));
            Ok::<_, std::convert::Infallible>(Rc::new("local".to_owned()))
        },
        cleanup: |_| Ok::<_, std::convert::Infallible>(()),
    })
    .await
    .unwrap();

    let caller = handle.new_handle();

    // The submitted reply must be Send even though the state is not.
    #[allow(clippy::async_yields_async)]
    let reply = tokio::spawn(async move {
        call(
            &caller,
            |state: &mut Rc<String>, ()| state.as_ref().clone(),
            (),
        )
        .send()
        .await
    })
    .await
    .unwrap();

    actor.pause().await.unwrap();
    drop(handle);
    thread.join_async().await.unwrap().unwrap();

    assert_eq!(tokio::spawn(reply).await.unwrap(), "local");
}

#[tokio::test]
async fn init() {
    let result = spawn_runner(
        SpawnArgs {
            name: "init".into(),
            capacity: 1,
            failure: FailurePolicy::Abort,
            setup: || Err::<(), _>("no state"),
            cleanup: |_, _: Result<(), RunError<&str>>| panic!("nothing to clean"),
        },
        |_, _| Ok(()),
    )
    .await;

    assert!(matches!(result, Err(DedicatedStartError::Init("no state"))));
}

#[tokio::test]
async fn startup_panic() {
    let result = spawn(SpawnArgs {
        name: "startup".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || -> Result<(), &'static str> { panic!("setup sentinel") },
        cleanup: |_| Ok::<_, std::convert::Infallible>(()),
    })
    .await;

    match result {
        Err(DedicatedStartError::Panicked { actor, cause }) => {
            assert_eq!(actor, "startup");
            assert!(cause.to_string().contains("setup sentinel"));
        }
        _ => panic!("startup panic was lost"),
    }
}

#[tokio::test]
async fn async_lifecycle() {
    fn setup(value: usize) -> impl Future<Output = Result<usize, ()>> {
        let timer = tokio::time::sleep(Duration::from_millis(1));

        async move {
            timer.await;
            Ok(value)
        }
    }

    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let observed = cleaned.clone();
    let (handle, actor, thread) = spawn_async(SpawnArgs {
        name: "runtime callbacks".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || setup(1),
        cleanup: move |state| {
            let timer = tokio::time::sleep(Duration::from_millis(1));
            let observed = observed.clone();

            async move {
                timer.await;
                observed.lock().unwrap().push(state);

                Ok::<_, ()>(())
            }
        },
    })
    .await
    .unwrap();

    actor.pause().await.unwrap();
    actor.resume_async(|| setup(2)).await.unwrap();
    actor.replace_async(|| setup(3)).await.unwrap();
    assert_eq!(call(&handle, |state, ()| *state, ()).await, 3);

    actor.shutdown().await.unwrap();
    thread.join_async().await.unwrap().unwrap();

    assert_eq!(*cleaned.lock().unwrap(), [1, 2, 3]);
}

#[test]
fn sources() {
    use std::{error::Error, io, sync::Arc};

    let errors: Vec<Box<dyn Error>> = vec![
        Box::new(DedicatedStartError::Init(io::Error::other("setup"))),
        Box::new(DedicatedStartError::<io::Error>::Thread(io::Error::other(
            "thread",
        ))),
        Box::new(ReplaceError::<io::Error, io::Error>::Setup(
            io::Error::other("setup"),
        )),
        Box::new(ReplaceError::<io::Error, io::Error>::Cleanup(Arc::new(
            io::Error::other("cleanup"),
        ))),
        Box::new(RunError::Failed(io::Error::other("serve"))),
    ];

    for error in errors {
        assert!(
            error
                .source()
                .is_some_and(|source| source.is::<io::Error>()),
            "missing source: {error}"
        );
    }
}
