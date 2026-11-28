use super::*;
use core::num::NonZeroUsize;
use std::rc::Rc;

#[aktor(latest)]
async fn local_value(state: &Rc<u32>, value: Rc<u32>) -> Rc<u32> {
    Rc::new(**state + *value)
}

#[aktor]
async fn generic_local<T: 'static>(_: &(), value: T) -> T {
    value
}

#[aktor]
async fn opaque_local_value(state: &Rc<u32>) -> impl std::ops::Deref<Target = u32> {
    state.clone()
}

#[tokio::test]
async fn local_results() {
    let value = Rc::new(85);

    assert_eq!(*local_value(&value, Rc::new(5)).await, 90);
    assert_eq!(*opaque_local_value(&value).await, 85);
    assert!(Rc::ptr_eq(&generic_local(&(), value.clone()).await, &value));

    #[cfg(feature = "local")]
    {
        let (handle, owner) = aktor::local::channel::<Rc<u32>, ()>(NonZeroUsize::new(2).unwrap());
        let client = async {
            assert_eq!(*local_value(&handle, Rc::new(5)).await.unwrap(), 90);
            assert_eq!(*opaque_local_value(&handle).await.unwrap(), 85);

            let (sender, mut results) = local_value::latest(&handle);

            sender.send(Rc::new(10));
            assert_eq!(*results.next().await.unwrap().unwrap(), 95);
            drop(sender);
            drop(results);
            handle.shutdown();
        };

        let (result, ()) = tokio::join!(owner.run(value, async |_| Ok(())), client);

        result.unwrap();
    }
}

#[aktor]
async fn indices(state: &usize) -> impl Iterator<Item = usize> + Send {
    0..*state
}

#[aktor]
async fn fallible(state: &usize) -> Result<impl Iterator<Item = usize> + Send, ()> {
    Ok(0..*state)
}

#[aktor]
async fn supplied<T>(
    _: &usize,
    values: impl IntoIterator<Item = T>,
) -> impl Iterator<Item = T> + Send
where
    T: Send + 'static,
{
    // Collect first so the returned iterator owns its items.
    values.into_iter().collect::<Vec<_>>().into_iter()
}

#[aktor]
async fn precise<'a>(state: &'a usize) -> impl Iterator<Item = usize> + Send + use<> {
    0..*state
}

#[aktor]
#[track_caller]
async fn precise_borrowed(
    _: &usize,
    text: impl AsRef<str>,
) -> impl Iterator<Item = usize> + Send + use<> {
    0..text.as_ref().len()
}

#[aktor]
async fn precise_input<T>(
    _: &usize,
    text: impl AsRef<str>,
) -> impl Iterator<Item = usize> + Send + use<T> {
    0..text.as_ref().len() + core::mem::size_of::<T>()
}

#[aktor]
async fn named<'a>(state: &'a usize) -> impl Iterator<Item = usize> + Send {
    let count: &'a usize = state;
    0..*count
}

#[tokio::test]
async fn direct_output() {
    let items = {
        let values = [1, 2];
        supplied(&values.len(), values.iter().copied()).await
    };

    assert!(items.eq([1, 2]));
    let items = {
        let text = String::from("hi");
        precise_borrowed(&0, text.as_str()).await
    };
    assert!(items.eq([0, 1]));

    let items = {
        let text = String::from("hi");
        precise_input::<u8, _, _>(&0, text.as_str()).await
    };
    assert!(items.eq([0, 1, 2]));
}

#[tokio::test]
async fn replies() {
    let task = tokio::spawn(async {
        let state = 3;
        indices(&state).await.collect::<Vec<_>>()
    });

    assert_eq!(task.await.unwrap(), [0, 1, 2]);

    let owned = {
        let values = [1, 2];
        supplied(&0, values.iter().copied()).await
    };

    assert_eq!(owned.collect::<Vec<_>>(), [1, 2]);

    let nested_owned = {
        let names = [String::from("Haandboldfuglen")];
        lengths(&0, &names).await
    };

    assert_eq!(nested_owned.collect::<Vec<_>>(), [15]);

    let (handle, _group) = thread_actor(3usize);
    let plain = indices(&handle).send().await.unwrap();
    let nested = fallible(&handle).send().await.unwrap();
    let generic = supplied(&handle, vec!["a", "b"]).send().await.unwrap();
    let captured = precise(&handle).send().await.unwrap();
    let named = named(&handle).send().await.unwrap();
    let nested_inputs = lengths(&handle, vec![String::from("Haandboldfuglen")])
        .send()
        .await
        .unwrap();

    drop(handle);

    assert_eq!(
        tokio::spawn(async move { plain.await.unwrap().collect::<Vec<_>>() })
            .await
            .unwrap(),
        [0, 1, 2]
    );

    assert_eq!(
        tokio::spawn(async move { nested.await.unwrap().unwrap().collect::<Vec<_>>() })
            .await
            .unwrap(),
        [0, 1, 2]
    );

    assert_eq!(
        tokio::spawn(async move { generic.await.unwrap().collect::<Vec<_>>() })
            .await
            .unwrap(),
        ["a", "b"]
    );

    assert_eq!(
        tokio::spawn(async move { captured.await.unwrap().collect::<Vec<_>>() })
            .await
            .unwrap(),
        [0, 1, 2]
    );

    assert_eq!(nested_inputs.await.unwrap().collect::<Vec<_>>(), [15]);
    assert_eq!(named.await.unwrap().collect::<Vec<_>>(), [0, 1, 2]);
}

#[aktor]
async fn lengths(
    _: &usize,
    names: impl IntoIterator<Item = impl AsRef<str>>,
) -> impl Iterator<Item = usize> + Send {
    names
        .into_iter()
        .map(|name| name.as_ref().len())
        .collect::<Vec<_>>()
        .into_iter()
}
