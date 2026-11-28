use super::*;
use crate::dispatch::Export;
#[cfg(any(
    not(target_family = "wasm"),
    feature = "wasm_browser_worker_server",
    feature = "testing"
))]
use crate::message::LocalFuture;
#[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
use core::any::Any;
use core::{
    any::{TypeId, type_name},
    marker::PhantomData,
};

pub struct Registration(pub fn() -> Option<Registered>);
inventory::collect!(Registration);

#[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
type Run = for<'a> fn(
    &'a mut dyn Any,
    &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>>;
#[cfg(feature = "testing")]
type MockRun = for<'a> fn(
    &'a mut crate::testing::Handlers,
    &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>>;

pub struct Registered {
    name: &'static str,
    state: TypeId,
    role: TypeId,
    input: &'static str,
    output: &'static str,
    codec: &'static str,
    remote: bool,
    #[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
    run: Option<Run>,
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

    #[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
    pub fn execute<'a>(
        &self,
        state: &'a mut dyn Any,
        input: &'a [u8],
    ) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>> {
        (self.run.unwrap_or(unserved))(state, input)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, aktor_macros::AktorData)]
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

#[derive(Er)]
pub struct CollectOperationsErr {
    pub operation: &'static str,
    pub msg: &'static str,
}

pub struct Registry(Vec<Registered>);
impl Registry {
    #[cfg(feature = "testing")]
    pub async fn dispatch_mock(
        &self,
        handlers: &mut crate::testing::Handlers,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, (WorkerError, String)> {
        let operation = self.resolve(&name).er(|| {
            WorkerError::new(
                AktorDeliveryOutcome::NotAdmitted,
                WorkerCause::UnknownOperation(name),
            )
        })?;
        (operation.mock)(handlers, &input).await
    }

    pub fn for_remote<S: 'static, Role: 'static>() -> Result<Self, (WorkerError, String)> {
        Ok(
            Self::collect(registered::<S, Role>().filter(|operation| operation.remote))
                .er(|| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Setup))?,
        )
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

    pub fn for_actor<S: 'static, Role: 'static>() -> Result<Self, (WorkerError, String)> {
        Ok(Self::collect(registered::<S, Role>())
            .er(|| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Setup))?)
    }

    fn collect(
        operations: impl Iterator<Item = Registered>,
    ) -> ErResult<Self, CollectOperationsErr> {
        let mut operations: Vec<_> = operations.collect();

        operations.sort_unstable_by_key(|operation| operation.name);

        for pair in operations.windows(2) {
            if pair[0].name == pair[1].name {
                er_bail!(CollectOperationsErr::new(
                    pair[0].name,
                    "duplicate worker operation, functions declared inside other functions share \
                     their module's name. Give one of them #[aktor(name = \"...\")]",
                ));
            }
        }

        Ok(Self(operations))
    }

    #[cfg(all(
        feature = "wasm_browser_worker_server",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    pub(super) fn check_handlers(&self) -> ErResult<(), CollectOperationsErr> {
        if let Some(operation) = self.0.iter().find(|operation| operation.run.is_none()) {
            er_bail!(CollectOperationsErr::new(
                operation.name,
                "aktor_serve excludes this operation's state"
            ));
        }

        Ok(())
    }

    pub fn operations(&self) -> Operations {
        let mut operations: Vec<_> = self.0.iter().map(Registered::signature).collect();

        operations.sort_unstable();
        Operations(operations)
    }

    #[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
    pub async fn dispatch<S: 'static>(
        &self,
        state: &mut S,
        name: String,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, (WorkerError, String)> {
        let operation = self.resolve(&name).er(|| {
            WorkerError::new(
                AktorDeliveryOutcome::Discarded,
                WorkerCause::UnknownOperation(name),
            )
        })?;

        operation.execute(state, &input).await
    }
}

pub struct Exporter<E, C = crate::dispatch::DataCodec> {
    pub name: &'static str,
    pub export: PhantomData<(E, C)>,
}

#[doc(hidden)]
pub trait Register {
    fn register<const SERVE: bool>(self) -> Option<Registered>;
}
impl<E, C> Register for &Exporter<E, C> {
    fn register<const SERVE: bool>(self) -> Option<Registered> {
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
    fn register<const SERVE: bool>(self) -> Option<Registered> {
        Some(Registered {
            name: self.name,
            state: TypeId::of::<E::State>(),
            role: TypeId::of::<E::Role>(),
            input: type_name::<E::Input>(),
            output: type_name::<E::Output>(),
            codec: <C as Codec<E::Input>>::NAME,
            remote: E::REMOTE,
            #[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
            run: if SERVE { Some(run::<E, C>) } else { None },
            #[cfg(feature = "testing")]
            mock: run_mock::<E, C>,
        })
    }
}

#[cfg(feature = "testing")]
fn run_mock<'a, E, C>(
    handlers: &'a mut crate::testing::Handlers,
    input: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>>
where
    E: Export<C>,
    E::Input: 'static,
    C: Codec<E::Input> + Codec<E::Output>,
{
    Box::pin(async move {
        let input = crate::worker::wire::codec(<C as Codec<E::Input>>::decode(input), || {
            WorkerError::new(AktorDeliveryOutcome::Discarded, WorkerCause::Codec)
        })?;
        let output = handlers
            .run(E::NAME, Box::new(input))
            .await
            .er_with(|error| {
                WorkerError::new(
                    AktorDeliveryOutcome::Discarded,
                    WorkerCause::UnknownOperation(error.operation.clone()),
                )
            })?;
        let output = output
            .downcast::<E::Output>()
            .ok()
            .er(|| WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Protocol))?;

        Ok(crate::worker::wire::codec(
            <C as Codec<E::Output>>::encode(&output),
            || WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Codec),
        )?)
    })
}

#[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
fn unserved<'a>(
    _: &'a mut dyn Any,
    _: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>> {
    Box::pin(async {
        er_bail!(
            WorkerError::new(AktorDeliveryOutcome::Discarded, WorkerCause::Setup),
            "this worker's aktor_serve selector excludes the requested state"
        )
    })
}

#[cfg(any(not(target_family = "wasm"), feature = "wasm_browser_worker_server"))]
fn run<'a, E, C>(
    state: &'a mut dyn Any,
    input: &'a [u8],
) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>>
where
    E: Export<C> + Default,
    C: Codec<E::Input> + Codec<E::Output>,
{
    Box::pin(async move {
        let state = state
            .downcast_mut::<E::State>()
            .er(|| WorkerError::new(AktorDeliveryOutcome::Discarded, WorkerCause::Protocol))?;

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

// Worker builds register ordinary calls too. Native transport only needs explicit remote exports.
#[doc(hidden)]
#[macro_export]
#[cfg(feature = "wasm_browser_workers")]
macro_rules! __aktor_register_worker {
    ($export:ty, $function:ident, $name:expr, $codec:ty, $serve:meta) => {
        $crate::__aktor_register!($export, $function, $name, $codec, $serve);
    };
}

#[doc(hidden)]
#[macro_export]
#[cfg(not(feature = "wasm_browser_workers"))]
macro_rules! __aktor_register_worker {
    ($($operation:tt)*) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_require_codec {
    ($export:ty, $codec:ty) => {
        const _: () = {
            fn require_codec<E, C>()
            where
                E: $crate::dispatch::Export<C>,
                C: $crate::worker::Codec<E::Input> + $crate::worker::Codec<E::Output>,
            {
            }

            let _ = require_codec::<$export, $codec>;
        };
    };
}

#[doc(hidden)]
#[macro_export]
#[cfg(all(aktor_serve_selected, target_family = "wasm"))]
macro_rules! __aktor_serve {
    ($serve:meta) => {{
        #[allow(unexpected_cfgs)]
        let selected = ::core::cfg!($serve);
        selected
    }};
}

#[doc(hidden)]
#[macro_export]
#[cfg(not(all(aktor_serve_selected, target_family = "wasm")))]
macro_rules! __aktor_serve {
    ($serve:meta) => {
        true
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_register {
    ($export:ty, $function:ident, $name:expr, $codec:ty, true, $serve:meta) => {
        $crate::__aktor_require_codec!($export, $codec);
        $crate::__aktor_register!($export, $function, $name, $codec, $serve);
    };
    ($export:ty, $function:ident, $name:expr, $codec:ty, false, $serve:meta) => {
        $crate::__aktor_register_worker!($export, $function, $name, $codec, $serve);
    };
    ($export:ty, $function:ident, $name:expr, $codec:ty, $serve:meta) => {
        const _: () = {
            fn registration() -> ::core::option::Option<$crate::worker::Registered> {
                use $crate::worker::Register as _;

                // Local calls don't need a transport codec.
                (&&$crate::worker::Exporter::<$export, $codec> {
                    name: $name,
                    export: ::core::marker::PhantomData,
                }).register::<{ $crate::__aktor_serve!($serve) }>()
            }

            $crate::worker::inventory::submit! {
                $crate::worker::Registration(registration)
            }

            #[cfg(target_family = "wasm")]
            #[allow(unsafe_code)]
            // Pull registrations from dependency archives into the Wasm module.
            #[unsafe(export_name = ::core::concat!(
                "__aktor_register_", ::core::module_path!(), "@", ::core::env!("CARGO_PKG_VERSION"),
                ":", ::core::file!(), ":", ::core::line!(), ":", ::core::column!(), ":", ::core::stringify!($function)
            ))]
            pub extern "C" fn anchor() {
                ::core::hint::black_box(registration as fn() -> _);
            }
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
    ) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>> {
        Box::pin(async move {
            state.downcast_mut::<State>().unwrap().0 += 1;
            Ok(Vec::new())
        })
    }

    fn second<'a>(
        state: &'a mut dyn Any,
        _: &'a [u8],
    ) -> LocalFuture<'a, Result<Vec<u8>, (WorkerError, String)>> {
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
            run: Some(run),
            #[cfg(feature = "testing")]
            mock: |_, _| {
                Box::pin(async {
                    er_bail!(WorkerError::new(
                        AktorDeliveryOutcome::NotAdmitted,
                        WorkerCause::Protocol
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

        let first = Registry::for_actor::<State, First>().unwrap();
        first
            .dispatch(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        Registry::for_actor::<State, Second>()
            .unwrap()
            .dispatch(&mut state, "same operation".into(), Vec::new())
            .await
            .unwrap();
        assert_eq!(state.0, 18);

        let unknown = first
            .dispatch(&mut state, "unknown".into(), Vec::new())
            .await
            .unwrap_err();

        assert_eq!(
            unknown.0.cause,
            WorkerCause::UnknownOperation("unknown".into())
        );
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
