use super::*;

#[aktor]
async fn explode(_: &()) {
    panic!("broken query");
}

#[tokio::test]
async fn caller() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (db, mut listener) = channel(1).unwrap();
            let (reported, report) = std::sync::mpsc::channel();

            listener.failure = FailurePolicy::shutdown(move |failure| {
                let FailureKind::Operation(operation) = failure.kind else {
                    panic!("lost operation");
                };

                reported
                    .send((operation.name, operation.caller.line()))
                    .unwrap();
            });

            let task = tokio::task::spawn_local(listener.run(()));
            let expected = line!() + 1;
            explode::request(&db).cast().await;

            assert!(matches!(task.await, Err(error) if error.is_panic()));

            let (name, line) = report.try_recv().unwrap();
            assert!(name.ends_with("::explode"));
            assert_eq!(line, expected);
        })
        .await
}
