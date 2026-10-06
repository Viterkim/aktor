use super::*;
use crate::{dispatch::Export, message::LocalFuture};
use core::{
    any::{Any, TypeId, type_name},
    marker::PhantomData,
};

pub struct Registration(pub fn() -> Option<Registered>);
inventory::collect!(Registration);

type Run = for<'a> fn(&'a mut dyn Any, &'a [u8]) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>;

pub struct Registered {
    name: &'static str,
    state: TypeId,
    role: TypeId,
    input: &'static str,
    output: &'static str,
    codec: &'static str,
    run: Run,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operations(pub Vec<String>);
impl Operations {
    pub fn for_actor<S: 'static, Role: 'static>() -> Self {
        let mut operations: Vec<_> = registered::<S, Role>()
            .map(|operation| {
                format!(
                    "{}: {} -> {}{}",
                    operation.name, operation.input, operation.output, operation.codec
                )
            })
            .collect();

        operations.sort_unstable();
        Self(operations)
    }
}

pub struct Registry(Vec<Registered>);
impl Registry {
    pub fn for_actor<S: 'static, Role: 'static>() -> Result<Self, WorkerError> {
        let mut operations: Vec<_> = registered::<S, Role>().collect();

        operations.sort_unstable_by_key(|operation| operation.name);

        for pair in operations.windows(2) {
            if pair[0].name == pair[1].name {
                return Err(WorkerError::new(
                    CallError::NotAdmitted,
                    WorkerCause::Setup(format!("duplicate worker operation {:?}", pair[0].name)),
                ));
            }
        }

        Ok(Self(operations))
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub fn operations(&self) -> Operations {
        let mut operations: Vec<_> = self
            .0
            .iter()
            .map(|operation| {
                format!(
                    "{}: {} -> {}{}",
                    operation.name, operation.input, operation.output, operation.codec
                )
            })
            .collect();

        operations.sort_unstable();
        Operations(operations)
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub fn operation_name(&self, name: &str) -> Option<&'static str> {
        self.0
            .iter()
            .find(|operation| operation.name == name)
            .map(|operation| operation.name)
    }

    pub async fn dispatch<S: 'static>(
        &self,
        state: &mut S,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, WorkerError> {
        let operation = self
            .0
            .iter()
            .find(|operation| operation.name == name)
            .ok_or_else(|| {
                WorkerError::new(CallError::Discarded, WorkerCause::UnknownOperation(name))
            })?;

        (operation.run)(state, &input).await
    }
}

pub struct Exporter<E, C = crate::dispatch::SerdeCodec> {
    pub name: &'static str,
    pub export: PhantomData<(E, C)>,
}

pub trait Register {
    fn register(self) -> Option<Registered>;
}
impl<E, C> Register for &Exporter<E, C> {
    fn register(self) -> Option<Registered> {
        None
    }
}
impl<E, C> Register for &&Exporter<E, C>
where
    E: Export<C> + Default,
    C: Codec<E::Input> + Codec<E::Output>,
    E::Role: 'static,
{
    fn register(self) -> Option<Registered> {
        Some(Registered {
            name: self.name,
            state: TypeId::of::<E::State>(),
            role: TypeId::of::<E::Role>(),
            input: type_name::<E::Input>(),
            output: type_name::<E::Output>(),
            codec: <C as Codec<E::Input>>::NAME,
            run: run::<E, C>,
        })
    }
}

fn run<'a, E, C>(
    state: &'a mut dyn Any,
    input: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>
where
    E: Export<C> + Default,
    C: Codec<E::Input> + Codec<E::Output>,
{
    Box::pin(async move {
        let state = state
            .downcast_mut::<E::State>()
            .ok_or_else(|| WorkerError::new(CallError::Discarded, WorkerCause::Protocol))?;

        run_export::<E, C>(E::default(), state, input).await
    })
}

fn registered<S: 'static, Role: 'static>() -> impl Iterator<Item = Registered> {
    inventory::iter::<Registration>
        .into_iter()
        .filter_map(|registration| registration.0())
        .filter(|operation| {
            operation.state == TypeId::of::<S>() && operation.role == TypeId::of::<Role>()
        })
}

pub async fn dispatch<S: 'static, Role: 'static>(
    state: &mut S,
    name: String,
    input: Vec<u8>,
) -> Result<Vec<u8>, WorkerError> {
    Registry::for_actor::<S, Role>()?
        .dispatch(state, name, input)
        .await
}

#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_register {
    ($export:ty, $name:expr, $codec:ty) => {
        $crate::worker::inventory::submit! {
            $crate::worker::Registration(|| {
                use $crate::worker::Register as _;

                // The fallback keeps local Rust calls free of Serde requirements.
                (&&$crate::worker::Exporter::<$export, $codec> {
                    name: $name,
                    export: ::core::marker::PhantomData,
                }).register()
            })
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct State(usize);
    struct Duplicate;
    struct First;
    struct Second;

    fn first<'a>(
        state: &'a mut dyn Any,
        _: &'a [u8],
    ) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>> {
        Box::pin(async move {
            state.downcast_mut::<State>().unwrap().0 += 1;
            Ok(Vec::new())
        })
    }

    fn second<'a>(
        state: &'a mut dyn Any,
        _: &'a [u8],
    ) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>> {
        Box::pin(async move {
            state.downcast_mut::<State>().unwrap().0 += 17;
            Ok(Vec::new())
        })
    }

    fn operation<Role: 'static>(run: Run) -> Option<Registered> {
        Some(Registered {
            name: "same operation",
            state: TypeId::of::<State>(),
            role: TypeId::of::<Role>(),
            input: "()",
            output: "()",
            codec: "",
            run,
        })
    }

    inventory::submit!(Registration(|| operation::<Duplicate>(first)));
    inventory::submit!(Registration(|| operation::<Duplicate>(second)));
    inventory::submit!(Registration(|| operation::<First>(first)));
    inventory::submit!(Registration(|| operation::<Second>(second)));

    #[tokio::test]
    async fn names_are_unambiguous() {
        let mut state = State(0);

        assert!(Registry::for_actor::<State, Duplicate>().is_err());

        let error = dispatch::<State, Duplicate>(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap_err();

        assert!(format!("{error}").contains("same operation"));
        assert_eq!(state.0, 0);

        dispatch::<State, First>(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        dispatch::<State, Second>(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        assert_eq!(state.0, 18);
    }
}
