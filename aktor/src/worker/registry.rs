use super::*;
use crate::{dispatch::Export, message::LocalFuture};
use core::{
    any::{Any, TypeId, type_name},
    marker::PhantomData,
};

pub struct Registration(pub fn() -> Option<Registered>);
inventory::collect!(Registration);

type Run = for<'a> fn(&'a mut dyn Any, &'a [u8]) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>;
#[cfg(feature = "testing")]
type MockRun = for<'a> fn(
    &'a mut crate::testing::Handlers,
    &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>;

pub struct Registered {
    name: &'static str,
    state: TypeId,
    role: TypeId,
    input: &'static str,
    output: &'static str,
    codec: &'static str,
    remote: bool,
    run: Run,
    #[cfg(feature = "testing")]
    mock: MockRun,
}
impl Registered {
    pub fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> String {
        format!(
            "{}: {} -> {}{}",
            self.name, self.input, self.output, self.codec,
        )
    }

    pub fn execute<'a>(
        &self,
        state: &'a mut dyn Any,
        input: &'a [u8],
    ) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>> {
        (self.run)(state, input)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operations(pub Vec<String>);
impl Operations {
    pub fn for_remote<S: 'static, Role: 'static>() -> Self {
        let mut operations: Vec<_> = registered::<S, Role>()
            .filter(|operation| operation.remote)
            .map(|operation| operation.signature())
            .collect();
        operations.sort_unstable();
        Self(operations)
    }

    pub fn supports(&self, required: &Self) -> bool {
        required
            .0
            .iter()
            .all(|operation| self.0.binary_search(operation).is_ok())
    }

    pub fn for_actor<S: 'static, Role: 'static>() -> Self {
        let mut operations: Vec<_> = registered::<S, Role>()
            .map(|operation| operation.signature())
            .collect();

        operations.sort_unstable();
        Self(operations)
    }
}

pub struct Registry(Vec<Registered>);
impl Registry {
    #[cfg(feature = "testing")]
    pub async fn dispatch_mock(
        &self,
        handlers: &mut crate::testing::Handlers,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, WorkerError> {
        let operation = self.resolve(&name).ok_or_else(|| {
            WorkerError::new(CallError::NotAdmitted, WorkerCause::UnknownOperation(name))
        })?;
        (operation.mock)(handlers, &input).await
    }

    pub fn for_remote<S: 'static, Role: 'static>() -> Result<Self, WorkerError> {
        Self::collect(registered::<S, Role>().filter(|operation| operation.remote))
    }

    pub fn resolve(&self, name: &str) -> Option<&Registered> {
        if self.0.len() <= 16 {
            return self.0.iter().find(|operation| operation.name == name);
        }

        if let Some(first) = self.0.first()
            && first.name == name
        {
            return Some(first);
        }

        self.0
            .binary_search_by_key(&name, |operation| operation.name)
            .ok()
            .map(|index| &self.0[index])
    }

    pub fn for_actor<S: 'static, Role: 'static>() -> Result<Self, WorkerError> {
        Self::collect(registered::<S, Role>())
    }

    fn collect(operations: impl Iterator<Item = Registered>) -> Result<Self, WorkerError> {
        let mut operations: Vec<_> = operations.collect();

        operations.sort_unstable_by_key(|operation| operation.name);

        for pair in operations.windows(2) {
            if pair[0].name == pair[1].name {
                return Err(WorkerError::setup(
                    CallError::NotAdmitted,
                    format!("duplicate worker operation {:?}", pair[0].name),
                ));
            }
        }

        Ok(Self(operations))
    }

    pub fn operations(&self) -> Operations {
        let mut operations: Vec<_> = self.0.iter().map(Registered::signature).collect();

        operations.sort_unstable();
        Operations(operations)
    }

    pub async fn dispatch<S: 'static>(
        &self,
        state: &mut S,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, WorkerError> {
        let operation = self.resolve(&name).ok_or_else(|| {
            WorkerError::new(CallError::Discarded, WorkerCause::UnknownOperation(name))
        })?;

        operation.execute(state, &input).await
    }
}

pub struct Exporter<E, C = crate::dispatch::DataCodec> {
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
    E::Input: 'static,
{
    fn register(self) -> Option<Registered> {
        Some(Registered {
            name: self.name,
            state: TypeId::of::<E::State>(),
            role: TypeId::of::<E::Role>(),
            input: type_name::<E::Input>(),
            output: type_name::<E::Output>(),
            codec: <C as Codec<E::Input>>::NAME,
            remote: E::REMOTE,
            run: run::<E, C>,
            #[cfg(feature = "testing")]
            mock: run_mock::<E, C>,
        })
    }
}

#[cfg(feature = "testing")]
fn run_mock<'a, E, C>(
    handlers: &'a mut crate::testing::Handlers,
    input: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, WorkerError>>
where
    E: Export<C>,
    E::Input: 'static,
    C: Codec<E::Input> + Codec<E::Output>,
{
    Box::pin(async move {
        let input = <C as Codec<E::Input>>::decode(input).map_err(|mut error| {
            error.outcome = CallError::Discarded;
            error
        })?;
        let output = handlers
            .run(E::NAME, Box::new(input))
            .await
            .map_err(|error| {
                WorkerError::new(
                    CallError::Discarded,
                    WorkerCause::UnknownOperation(error.operation),
                )
            })?;
        let output = output
            .downcast::<E::Output>()
            .map_err(|_| WorkerError::new(CallError::OutcomeUnknown, WorkerCause::Protocol))?;
        <C as Codec<E::Output>>::encode(&output).map_err(|mut error| {
            error.outcome = CallError::OutcomeUnknown;
            error
        })
    })
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
    ($export:ty, $name:expr, $codec:ty, true) => {
        const _: () = {
            fn require_codec<E, C>()
            where
                E: $crate::dispatch::Export<C>,
                C: $crate::worker::Codec<E::Input> + $crate::worker::Codec<E::Output>,
            {}

            let _ = require_codec::<$export, $codec>;
        };
        $crate::__aktor_register!($export, $name, $codec);
    };
    ($export:ty, $name:expr, $codec:ty, false) => {
        $crate::__aktor_register!($export, $name, $codec);
    };
    ($export:ty, $name:expr, $codec:ty) => {
        const _: () = {
            fn registration() -> ::core::option::Option<$crate::worker::Registered> {
                use $crate::worker::Register as _;

                // Local calls don't need a transport codec.
                (&&$crate::worker::Exporter::<$export, $codec> {
                    name: $name,
                    export: ::core::marker::PhantomData,
                }).register()
            }

            #[cfg(not(target_family = "wasm"))]
            $crate::worker::inventory::submit! {
                $crate::worker::Registration(registration)
            }

            #[cfg(target_family = "wasm")]
            const _: () = {
                static NODE: $crate::worker::inventory::Node = $crate::worker::inventory::Node {
                    value: &$crate::worker::Registration(registration),
                    next: ::core::cell::UnsafeCell::new(::core::option::Option::None),
                    initialized: ::core::sync::atomic::AtomicBool::new(false),
                };

                // Exporting the constructor also pulls its object out of dependency archives.
                #[unsafe(export_name = ::core::concat!(
                    "__aktor_register_", ::core::module_path!(), "@", ::core::env!("CARGO_PKG_VERSION"),
                    ":", ::core::line!(), ":", ::core::column!()
                ))]
                pub extern "C" fn initialize() {
                    // NODE.value is the Registration stored in NODE. Inventory guards repeat calls.
                    unsafe { $crate::worker::inventory::ErasedNode::submit(NODE.value, &NODE) }
                }

                #[used]
                #[unsafe(link_section = ".init_array")]
                static CTOR: extern "C" fn() = initialize;
            };
        };
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
            remote: false,
            run,
            #[cfg(feature = "testing")]
            mock: |_, _| {
                Box::pin(async {
                    Err(WorkerError::new(
                        CallError::NotAdmitted,
                        WorkerCause::Protocol,
                    ))
                })
            },
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

        assert!(format!("{error:#}").contains("same operation"));
        assert_eq!(state.0, 0);

        dispatch::<State, First>(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        dispatch::<State, Second>(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        assert_eq!(state.0, 18);

        let unknown = dispatch::<State, First>(&mut state, "unknown".into(), Vec::new())
            .await
            .unwrap_err();

        assert!(format!("{unknown}").contains("unknown"));
        assert_eq!(state.0, 18);
    }

    #[test]
    fn lookup() {
        for count in [0, 1, 16, 17, 85, 256] {
            let registry = Registry(
                (0..count)
                    .map(|index| {
                        let mut operation = operation::<First>(first).unwrap();

                        operation.name =
                            Box::leak(format!("catalogue::operation_{index:03}").into_boxed_str());
                        operation
                    })
                    .collect(),
            );

            for operation in &registry.0 {
                assert!(core::ptr::eq(
                    registry.resolve(operation.name).unwrap(),
                    operation
                ));
            }

            for unknown in [
                "",
                "catalogue::operation_000_extra",
                "catalogue::operation_missing",
            ] {
                assert!(registry.resolve(unknown).is_none());
            }
        }
    }
}
