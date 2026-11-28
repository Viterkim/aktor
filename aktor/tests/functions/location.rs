use super::*;

#[aktor]
async fn explode(_: &()) {
    panic!("broken query");
}

#[tokio::test]
async fn caller() {
    let (db, _group) = thread_actor(());
    let completion = db.group_completion();
    let expected = line!() + 1;
    drop(explode(&db).send().await.unwrap());

    let report = completion.wait().await.unwrap();
    let aktor::AktorFailureSource::Actor { phase, .. } = &report.failures[0].source else {
        panic!("expected actor failure")
    };
    let phase = phase.to_string();

    assert!(phase.contains("::explode @ "), "{phase}");
    assert!(phase.contains(&format!(":{expected}:")), "{phase}");
}
