use super::*;
use alloc::string::ToString;

impl<S, Role> Preset<S, Role> {
    pub fn new(handlers: impl IntoIterator<Item = Handler<S, Role>>) -> Self {
        let mut entries: Vec<Entry> = Vec::new();
        for handler in handlers {
            let entry = handler.entry;
            if let Some(previous) = entries
                .iter_mut()
                .find(|previous| previous.bodies == entry.bodies)
            {
                *previous = entry;
            } else {
                entries.push(entry);
            }
        }

        Self {
            handlers: Handlers { entries },
            state: PhantomData,
        }
    }
}

impl<E, F, C> Run for Callback<E, F, C>
where
    E: Export<C>,
    E::Input: 'static,
    E::Output: 'static,
    F: AsyncFnMut(E::Input) -> E::Output + Send + 'static,
{
    fn run(&mut self, input: Value) -> LocalFuture<'_, Result<Value, MissingHandler>> {
        Box::pin(async move {
            let Ok(input) = input.downcast::<E::Input>() else {
                return Err(MissingHandler {
                    operation: E::NAME.to_string(),
                });
            };

            let output = (self.function)(*input).await;
            Ok(Box::new(output) as Value)
        })
    }
}

impl Handlers {
    pub async fn run(&mut self, name: &'static str, input: Value) -> Result<Value, MissingHandler> {
        let mut handlers = self.entries.iter_mut().filter(|entry| entry.name == name);

        match (handlers.next(), handlers.next()) {
            (Some(entry), None) => entry.handler.run(input).await,
            _ => Err(MissingHandler {
                operation: name.to_string(),
            }),
        }
    }

    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    pub(super) async fn run_local(
        &mut self,
        body: TypeId,
        name: &'static str,
        input: Value,
    ) -> Result<Value, MissingHandler> {
        match self
            .entries
            .iter_mut()
            .find(|entry| entry.bodies.contains(&body))
        {
            Some(entry) => entry.handler.run(input).await,
            None => Err(MissingHandler {
                operation: name.to_string(),
            }),
        }
    }
}
impl core::fmt::Display for MissingHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "missing or incorrectly typed test handler for {}",
            self.operation
        )
    }
}
impl core::error::Error for MissingHandler {}
