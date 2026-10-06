use super::*;

#[test]
fn options() {
    let options = syn::parse_str::<Options>("crate = my_actor, role = db::Main,").unwrap();

    assert!(options.crate_path.unwrap().is_ident("my_actor"));
    assert_eq!(options.role.unwrap().segments.last().unwrap().ident, "Main");

    assert!(syn::parse_str::<Options>("data, role = Main").unwrap().data);
    assert!(
        syn::parse_str::<Options>("role = Main, data,")
            .unwrap()
            .data
    );
    assert!(syn::parse_str::<Options>("data, data").is_err());

    assert!(syn::parse_str::<Options>("actor = Main, actor = Audit").is_err());
    assert!(syn::parse_str::<Options>("role = Main, actor = Audit").is_err());
}

#[test]
fn async_required() {
    let error = syn::parse_str::<Function>("fn load(state: &State) {}")
        .err()
        .unwrap();

    assert!(error.to_string().contains("must be async"));
    assert!(syn::parse_str::<Function>("async fn load(state: &State) {}").is_ok());
}
