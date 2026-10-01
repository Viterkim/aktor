use super::*;

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

#[tokio::test]
async fn replies() {
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
