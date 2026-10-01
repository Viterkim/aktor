use super::*;

#[allow(clippy::toplevel_ref_arg)]
#[aktor]
async fn patterns(
    _: &usize,
    (a, b): (usize, usize),
    mut value: String,
    ref suffix: String,
    _: (),
) -> String {
    value.push_str(suffix);

    format!("{a}:{b}:{value}")
}

#[allow(clippy::ptr_arg)]
#[aktor]
async fn generic<T, const N: usize>(state: &Vec<T>, item: impl Into<String>) -> (usize, String)
where
    T: Send + 'static,
{
    (state.len() + N, item.into())
}

#[aktor]
async fn lifetimes<'__aktor_handle: 'static>(
    _: &usize,
    value: &'__aktor_handle str,
) -> &'__aktor_handle str {
    value
}

#[aktor]
async fn nested(_: &usize) -> usize {
    async fn independent() {
        std::future::ready(()).await;
    }

    let future = async {
        independent().await;
    };
    drop(future);

    let _callback = async || {
        std::future::ready(()).await;
    };

    1
}

#[tokio::test]
async fn signatures() {
    assert_eq!(
        patterns(&0, (1, 2), "a".into(), "b".into(), ()).await,
        "1:2:ab"
    );
    assert_eq!(generic::<u8, 2, _, _>(&vec![1], "x").await, (3, "x".into()));
    assert_eq!(lifetimes(&0, "static").await, "static");
    assert_eq!(nested(&0).await, 1);
}

#[aktor]
async fn identity(_: &usize) -> impl Fn(&str) -> &str + Send {
    |text| text
}

#[allow(non_camel_case_types)]
struct __AktorTarget;

struct Target(usize);
struct T(usize);
struct M(usize);

#[aktor(actor = __AktorTarget)]
async fn collision(state: &Target, first: T, second: M) -> usize {
    state.0 + first.0 + second.0
}

mod relative {
    pub use aktor as runtime;

    pub struct State(pub usize);
    pub struct Value(pub usize);
    pub struct Role;

    pub mod queries {
        #[aktor::aktor(actor = super::Role)]
        pub async fn read(state: &super::State, value: super::Value) -> super::Value {
            super::Value(state.0 + value.0)
        }

        #[aktor::aktor(crate = super::runtime)]
        pub async fn generic<T>(state: &super::State, value: T) -> super::Value
        where
            T: Into<super::Value> + Send + 'static,
        {
            super::Value(state.0 + value.into().0)
        }

        pub struct Own(pub usize);

        #[aktor::aktor]
        pub async fn own(state: &self::Own) -> usize {
            state.0
        }
    }
}

#[aktor]
async fn r#type(_: &usize, __aktor_state: usize, __aktor_input: usize) -> usize {
    let _size = std::mem::size_of::<__AktorTarget>();
    __aktor_state + __aktor_input
}

#[tokio::test]
async fn names() {
    assert_eq!(r#type(&0, 1, 2).await, 3);
    assert_eq!(identity(&0).await("text"), "text");
    assert_eq!(collision(&Target(1), T(2), M(3)).await, 6);

    assert_eq!(
        relative::queries::read(&relative::State(1), relative::Value(2))
            .await
            .0,
        3
    );

    assert_eq!(
        relative::queries::generic(&relative::State(1), relative::Value(2))
            .await
            .0,
        3
    );

    assert_eq!(relative::queries::own(&relative::queries::Own(4)).await, 4);
}

#[aktor]
async fn by_name(_: &std::rc::Rc<usize>, name: impl AsRef<str>) -> String {
    name.as_ref().to_owned()
}

#[aktor]
async fn borrowed(_: &usize, name: &str) -> usize {
    name.len()
}

#[tokio::test]
async fn inputs() {
    let text = String::from("name");
    assert_eq!(borrowed(&0, &text).await, 4);

    let state = std::rc::Rc::new(0);
    assert_eq!(by_name(&state, &text).await, text);
    assert_eq!(
        by_name(&state, std::rc::Rc::<str>::from(text.as_str())).await,
        text
    );

    let (handle, _actor, thread) = spawn(SpawnArgs {
        name: "inputs".into(),
        capacity: 1,
        failure: FailurePolicy::Abort,
        setup: || Ok::<_, ()>(std::rc::Rc::new(0)),
        cleanup: |_| Ok::<_, ()>(()),
    })
    .await
    .unwrap();

    let reply = by_name::request(&handle, text).send().await;
    drop(handle);
    assert_eq!(tokio::spawn(reply).await.unwrap(), "name");
    thread.join_async().await.unwrap().unwrap();
}

#[aktor]
async fn length(_: &usize, text: std::borrow::Cow<'_, str>) -> usize {
    text.len()
}

#[aktor]
async fn state_length(state: &impl AsRef<str>) -> usize {
    state.as_ref().len()
}

#[tokio::test]
async fn state() {
    let text = String::from("BingoManden");
    assert_eq!(state_length(&text).await, 11);

    let (handle, _actor, thread) = spawn(SpawnArgs {
        name: "state".into(),
        capacity: 1,
        failure: FailurePolicy::Abort,
        setup: || Ok::<_, ()>(std::rc::Rc::<str>::from("BingoManden")),
        cleanup: |_| Ok::<_, ()>(()),
    })
    .await
    .unwrap();

    assert_eq!(state_length::<std::rc::Rc<str>, _>(&handle).await, 11);

    drop(handle);
    thread.join_async().await.unwrap().unwrap();
}

#[tokio::test]
async fn anonymous() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            use std::borrow::Cow;

            let text = String::from("Haandboldfuglen");
            assert_eq!(length(&0, Cow::Borrowed(&text)).await, 15);
            assert_eq!(
                mapped(&0, text.as_str(), |s| s.len(), |s| s.len()).await,
                30
            );

            let (handle, task) = spawn_local(&executor, 0, 8).unwrap();
            assert_eq!(length(&handle, Cow::Owned(text)).await, 15);

            drop(handle);
            task.await.unwrap();
        })
        .await
}

#[aktor]
async fn mapped(
    _: &usize,
    text: impl Into<std::borrow::Cow<'_, str>>,
    first: fn(std::borrow::Cow<'_, str>) -> usize,
    second: impl Fn(std::borrow::Cow<'_, str>) -> usize,
) -> usize {
    let text = text.into();
    first(std::borrow::Cow::Borrowed(&text)) + second(text)
}
