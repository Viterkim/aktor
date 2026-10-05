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

mod state_borrows {
    use super::*;

    pub struct State(pub String);

    #[aktor]
    pub async fn length<'a>(state: &'a State) -> usize {
        let text: &'a str = &state.0;
        text.len()
    }

    #[aktor]
    pub async fn append<'a>(state: &'a mut State) -> usize {
        let text: &'a mut String = &mut state.0;
        text.push('!');
        text.len()
    }
}

mod siblings {
    use super::*;

    pub fn __aktor_read_body() -> usize {
        99
    }

    pub fn __aktor_generic_body() -> usize {
        17
    }

    #[aktor]
    pub async fn read(state: &usize) -> usize {
        *state
    }

    #[aktor]
    pub async fn generic<T: Into<usize> + Send + 'static>(state: &usize, value: T) -> usize {
        *state + value.into()
    }
}

mod named_parameters {
    use super::*;

    #[allow(non_snake_case)]
    #[aktor]
    pub async fn echo(NAME: &usize, value: usize) -> usize {
        *NAME + value
    }

    #[allow(non_snake_case)]
    #[aktor]
    pub async fn value(_: &usize, NAME: usize) -> usize {
        NAME
    }

    #[allow(non_snake_case)]
    #[aktor]
    pub async fn generic<T: Into<usize> + Send + 'static>(NAME: &usize, value: T) -> usize {
        *NAME + value.into()
    }
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
    assert_eq!(named_parameters::echo(&7, 1).await, 8);
    assert_eq!(named_parameters::value(&7, 17).await, 17);
    assert_eq!(named_parameters::generic(&7, 1usize).await, 8);

    let (handle, owner) = spawn_thread(7usize, 1).unwrap();

    assert_eq!(named_parameters::echo(&handle, 1).await, 8);
    assert_eq!(named_parameters::value(&handle, 17).await, 17);
    assert_eq!(named_parameters::generic(&handle, 1usize).await, 8);

    let (sender, mut results) = named_parameters::value(&handle, 27).latest();

    assert_eq!(results.next().await, Some(27));
    drop((sender, results));

    let (sender, mut results) = named_parameters::echo(&handle, 2).latest();

    assert_eq!(results.next().await, Some(9));
    drop((sender, results, handle));
    owner.join().unwrap();

    assert_eq!(siblings::__aktor_read_body(), 99);
    assert_eq!(siblings::__aktor_generic_body(), 17);
    assert_eq!(siblings::read(&7).await, 7);
    assert_eq!(siblings::generic(&7, 1usize).await, 8);

    let mut state = state_borrows::State("kat".into());

    assert_eq!(state_borrows::length(&state).await, 3);
    assert_eq!(state_borrows::append(&mut state).await, 4);

    let (handle, owner) = spawn_thread(state, 1).unwrap();

    assert_eq!(state_borrows::length(&handle).await, 4);
    assert_eq!(state_borrows::append(&handle).await, 5);
    drop(handle);
    owner.join().unwrap();
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

mod shadowed {
    use super::aktor;

    pub const NAME: usize = 4;
    pub struct Send;

    #[aktor]
    pub async fn length(_: &Send, bytes: [u8; NAME]) -> usize {
        bytes.len()
    }
}

#[aktor]
async fn r#type(_: &usize, __aktor_state: usize, __aktor_input: usize) -> usize {
    let _size = std::mem::size_of::<__AktorTarget>();
    __aktor_state + __aktor_input
}

#[aktor]
async fn locals(
    function: &mut usize,
    operation: usize,
    inner: usize,
    sender: usize,
    results: usize,
    input: usize,
    r#state: usize,
) -> usize {
    *function = operation + inner + sender + results + input + r#state;
    *function
}

#[aktor]
async fn generic_locals<T: Into<usize> + Send + 'static>(
    operation: &mut usize,
    inner: T,
    sender: usize,
    results: usize,
    input: usize,
    function: usize,
    state: usize,
) -> usize {
    *operation = inner.into() + sender + results + input + function + state;
    *operation
}

#[tokio::test]
async fn names() {
    let mut state = 0;

    assert_eq!(locals(&mut state, 1, 2, 3, 4, 5, 6).await, 21);
    assert_eq!(generic_locals(&mut state, 1usize, 2, 3, 4, 5, 6).await, 21);

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

    assert_eq!(
        shadowed::length(&shadowed::Send, [0; shadowed::NAME]).await,
        4
    );

    let (handle, actor) = spawn_thread(shadowed::Send, 1).unwrap();

    assert_eq!(shadowed::length(&handle, [0; shadowed::NAME]).await, 4);

    drop(handle);
    actor.join().unwrap();
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

    let (sender, mut results) = by_name(&handle, String::from("search")).latest();

    assert_eq!(results.next().await, Some(String::from("search")));

    let cloned = sender.clone();

    cloned.send(String::from("new search"));
    assert_eq!(results.next().await, Some(String::from("new search")));
    drop(cloned);
    drop(sender);
    drop(results);

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

struct Inner(u32, std::marker::PhantomPinned);

#[aktor]
async fn put(_: &mut usize, value: Inner) -> u32 {
    value.0
}

#[aktor]
async fn keep<Latest: Send + 'static>(_: &usize, value: Latest) -> Latest {
    value
}

#[aktor]
async fn echo<LatestSender: Send + 'static>(_: &usize, value: LatestSender) -> LatestSender {
    value
}

#[allow(non_camel_case_types, non_upper_case_globals)]
mod helpers {
    use super::*;

    pub struct LatestSender(pub u8);

    #[aktor]
    pub async fn concrete(_: &usize, value: LatestSender) -> u8 {
        value.0
    }

    #[aktor]
    pub async fn binders<r#request, const r#latest: usize>(
        _: &usize,
        value: request,
    ) -> (Option<<request as IntoIterator>::Item>, usize)
    where
        request: IntoIterator + Send + 'static,
        <request as IntoIterator>::Item: Send + 'static,
    {
        (value.into_iter().next(), latest)
    }
}

#[tokio::test]
async fn latest_names() {
    let (handle, owner) = spawn_thread(0usize, 1).unwrap();

    assert_eq!(locals(&handle, 1, 2, 3, 4, 5, 6).await, 21);
    assert_eq!(
        generic_locals::request(&handle, 1usize, 2, 3, 4, 5, 6).await,
        21
    );

    let (sender, mut output) = locals(&handle, 1, 2, 3, 4, 5, 6).latest();

    assert_eq!(output.next().await, Some(21));
    drop(sender);
    drop(output);

    let (sender, mut output) = generic_locals(&handle, 1usize, 2, 3, 4, 5, 6).latest();

    assert_eq!(output.next().await, Some(21));
    drop(sender);
    drop(output);

    assert_eq!(
        put(&handle, Inner(12, std::marker::PhantomPinned)).await,
        12
    );

    let (sender, mut results) = put(&handle, Inner(17, std::marker::PhantomPinned)).latest();
    let (generic, mut found) = keep(&handle, String::from("query")).latest();

    assert_eq!(found.next().await.as_deref(), Some("query"));
    drop(generic);
    drop(found);

    assert_eq!(echo(&0usize, 7u8).await, 7);
    assert_eq!(echo::request(&handle, 8u8).await, 8);

    let (echo_sender, mut found) = echo(&handle, 9u8).latest();

    assert_eq!(found.next().await, Some(9));
    drop(echo_sender);
    drop(found);

    assert_eq!(
        helpers::concrete(&handle, helpers::LatestSender(7)).await,
        7
    );

    let (bound_sender, mut found) = helpers::binders::<_, 17, _>(&handle, [8u8]).latest();

    assert_eq!(found.next().await, Some((Some(8), 17)));
    bound_sender.send([9]);
    assert_eq!(found.next().await, Some((Some(9), 17)));
    drop(bound_sender);
    drop(found);

    let (pattern, mut matched) = patterns(&handle, (1, 2), "a".into(), "b".into(), ()).latest();

    assert_eq!(matched.next().await.as_deref(), Some("1:2:ab"));
    pattern.send((3, 4), "c".into(), "d".into(), ());
    assert_eq!(matched.next().await.as_deref(), Some("3:4:cd"));
    drop(pattern);
    drop(matched);

    assert_eq!(results.next().await, Some(17));
    drop(sender);
    drop(results);
    drop(handle);
    owner.join().unwrap();
}
