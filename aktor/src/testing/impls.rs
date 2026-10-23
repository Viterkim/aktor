use super::*;
use alloc::string::ToString;

impl<S, Role> Default for Preset<S, Role> {
    fn default() -> Self {
        Self {
            handlers: Handlers::default(),
            state: PhantomData,
        }
    }
}
impl<S, Role> Preset<S, Role> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on<E, F, C>(mut self, handler: Handler<E, F, C>) -> Self
    where
        E: Export<C, State = S, Role = Role> + 'static,
        E::Input: Send + 'static,
        E::Output: Send + 'static,
        F: AsyncFnMut(E::Input) -> E::Output + Send + 'static,
        C: 'static,
    {
        self.handlers.entries.insert(E::NAME, Box::new(handler));
        self
    }
}

impl<E, F, C> Run for Handler<E, F, C>
where
    E: Export<C>,
    E::Input: Send + 'static,
    E::Output: Send + 'static,
    F: AsyncFnMut(E::Input) -> E::Output + Send + 'static,
{
    fn run(&mut self, input: Value) -> LocalFuture<'_, Result<Value, MissingHandler>> {
        Box::pin(async move {
            let input = input.downcast::<E::Input>().map_err(|_| MissingHandler {
                operation: E::NAME.to_string(),
            })?;
            let output = (self.function)(*input).await;
            Ok(Box::new(output) as Value)
        })
    }
}

impl Handlers {
    pub async fn run(&mut self, name: &'static str, input: Value) -> Result<Value, MissingHandler> {
        let Some(handler) = self.entries.get_mut(name) else {
            return Err(MissingHandler {
                operation: name.to_string(),
            });
        };
        handler.run(input).await
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
