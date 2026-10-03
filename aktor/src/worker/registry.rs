use super::*;
use crate::{message::LocalFuture, target::Export};
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
    run: Run,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operations(pub Vec<String>);
impl Operations {
    pub fn for_actor<S: 'static, Role: 'static>() -> Self {
        let mut operations: Vec<_> = registered::<S, Role>()
            .map(|operation| {
                format!(
                    "{}: {} -> {}",
                    operation.name, operation.input, operation.output
                )
            })
            .collect();
        operations.sort_unstable();
        Self(operations)
    }
}

pub struct Exporter<E> {
    pub name: &'static str,
    pub export: PhantomData<E>,
}

pub trait Register {
    fn register(self) -> Option<Registered>;
}
impl<E> Register for &Exporter<E> {
    fn register(self) -> Option<Registered> {
        None
    }
}
impl<E> Register for &&Exporter<E>
where
    E: Export + Default,
    E::Input: Serialize + DeserializeOwned,
    E::Output: Serialize + DeserializeOwned,
    E::Role: 'static,
{
    fn register(self) -> Option<Registered> {
        Some(Registered {
            name: self.name,
            state: TypeId::of::<E::State>(),
            role: TypeId::of::<E::Role>(),
            input: type_name::<E::Input>(),
            output: type_name::<E::Output>(),
            run: run::<E>,
        })
    }
}

fn run<'a, E>(
    state: &'a mut dyn Any,
    input: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>
where
    E: Export + Default,
    E::Input: DeserializeOwned,
    E::Output: Serialize,
{
    Box::pin(async move {
        let state = state
            .downcast_mut::<E::State>()
            .ok_or_else(|| WorkerError::new(CallError::Discarded, WorkerCause::Protocol))?;
        run_export(E::default(), state, input).await
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
    let operation = registered::<S, Role>()
        .find(|operation| operation.name == name)
        .ok_or_else(|| {
            WorkerError::new(CallError::Discarded, WorkerCause::UnknownOperation(name))
        })?;
    (operation.run)(state, &input).await
}

#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_register {
    ($export:ty, $name:expr) => {
        $crate::worker::inventory::submit! {
            $crate::worker::Registration(|| {
                use $crate::worker::Register as _;

                // The fallback keeps local Rust calls free of Serde requirements.
                (&&$crate::worker::Exporter::<$export> {
                    name: $name,
                    export: ::core::marker::PhantomData,
                }).register()
            })
        }
    };
}
