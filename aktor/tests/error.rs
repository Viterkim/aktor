#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[test]
fn group_rejections() {
    use aktor::AktorGroupError;

    let mut group = aktor::AktorGroup::new();
    group.on_shutdown(|_| ()).unwrap();

    assert!(matches!(
        group.on_shutdown(|_| ()),
        Err((AktorGroupError::ShutdownTaken, _))
    ));
    assert!(matches!(group.start(), Err((AktorGroupError::NoTokio, _))));
    assert!(matches!(
        group.spawn_task(async {}, aktor::AktorJob::DropAtStop),
        Err((AktorGroupError::NoTokio, _))
    ));
}
