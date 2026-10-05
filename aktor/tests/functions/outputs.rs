use super::*;
use std::rc::Rc;

#[aktor]
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
        let (handle, owner) = aktor::local::channel::<Rc<u32>, 2, ()>().unwrap();
        let client = async {
            assert_eq!(*local_value(&handle, Rc::new(5)).await, 90);
            assert_eq!(*opaque_local_value(&handle).await, 85);

            let (sender, mut results) = local_value::latest(&handle);

            sender.send(Rc::new(10));
            assert_eq!(*results.next().await.unwrap(), 95);
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
    // The iterator itself is made owned here, regardless of the input iterator type.
{
    values.into_iter().collect::<Vec<_>>().into_iter()
}

#[aktor]
async fn precise<'a>(state: &'a usize) -> impl Iterator<Item = usize> + Send + use<> {
    0..*state
}

#[aktor]
async fn named<'a>(state: &'a usize) -> impl Iterator<Item = usize> + Send {
    let count: &'a usize = state;
    0..*count
}

#[tokio::test]
async fn replies() {
    let task = tokio::spawn(async {
        let state = 3;
        indices(&state).await.collect::<Vec<_>>()
    });

    assert_eq!(task.await.unwrap(), [0, 1, 2]);

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
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

            let (handle, task) = spawn_local(&executor, 3, 4).unwrap();
            let plain = indices::request(&handle).send().await;
            let nested = fallible::request(&handle).send().await;
            let generic = supplied::request(&handle, vec!["a", "b"]).send().await;
            let captured = precise::request(&handle).send().await;
            let named = named::request(&handle).send().await;
            let nested_inputs = lengths::request(&handle, vec![String::from("Haandboldfuglen")])
                .send()
                .await;

            drop(handle);

            assert_eq!(
                tokio::spawn(async move { plain.await.collect::<Vec<_>>() })
                    .await
                    .unwrap(),
                [0, 1, 2]
            );

            assert_eq!(
                tokio::spawn(async move { nested.await.unwrap().collect::<Vec<_>>() })
                    .await
                    .unwrap(),
                [0, 1, 2]
            );

            assert_eq!(
                tokio::spawn(async move { generic.await.collect::<Vec<_>>() })
                    .await
                    .unwrap(),
                ["a", "b"]
            );

            assert_eq!(
                tokio::spawn(async move { captured.await.collect::<Vec<_>>() })
                    .await
                    .unwrap(),
                [0, 1, 2]
            );

            assert_eq!(nested_inputs.await.collect::<Vec<_>>(), [15]);
            assert_eq!(named.await.collect::<Vec<_>>(), [0, 1, 2]);
            task.await.unwrap();
        })
        .await
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
