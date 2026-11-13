use super::*;
use core::ops::{Deref, DerefMut};

macro_rules! dispatch {
    ($name:ident, $call:ident, $lease:ident, $($mutable:tt)?) => {
        #[doc(hidden)]
        #[diagnostic::on_unimplemented(
            note = "Check the actor's state type and role. Transport arguments and results must implement AktorData, or Serialize and DeserializeOwned with #[aktor(serde)]."
        )]
        pub trait $name<Mode, S: 'static, I, O: 'static, Role = (), Codec = DataCodec> {
            type Lease: $lease<Target = S>;
            type Request<Fut>;

            fn request<F, Fut>(
                self,
                input: I,
                operation: Operation,
                function: F,
                factory: fn(Self::Lease, I) -> Fut,
            ) -> Self::Request<Fut>
            where
                F: for<'s> AsyncFnOnce(&'s $($mutable)? S, I) -> O + Send + 'static,
                Fut: Future<Output = (Self::Lease, O)>;
        }

        impl<T, S: 'static, I, O: 'static, Role, Codec> $name<Native, S, I, O, Role, Codec>
            for T
        where
            T: $call<S, I, O, Role>,
        {
            type Lease = T::Lease;
            type Request<Fut> = <T as $call<S, I, O, Role>>::Request<Fut>;

            fn request<F, Fut>(
                self,
                input: I,
                operation: Operation,
                function: F,
                factory: fn(Self::Lease, I) -> Fut,
            ) -> Self::Request<Fut>
            where
                F: for<'s> AsyncFnOnce(&'s $($mutable)? S, I) -> O + Send + 'static,
                Fut: Future<Output = (Self::Lease, O)>,
            {
                <T as $call<S, I, O, Role>>::dispatch(self, operation, function, input, factory)
            }
        }
        impl<T, S: 'static, I, O: 'static, Role, Codec> $name<Remote, S, I, O, Role, Codec>
            for T
        where
            T: Transport<S, I, O, Role, Codec>,
        {
            type Lease = OwnedState<S>;
            type Request<Fut> = <T as Transport<S, I, O, Role, Codec>>::Request;

            fn request<F, Fut>(
                self,
                input: I,
                operation: Operation,
                _: F,
                _: fn(Self::Lease, I) -> Fut,
            ) -> Self::Request<Fut>
            where
                F: for<'s> AsyncFnOnce(&'s $($mutable)? S, I) -> O + Send + 'static,
                Fut: Future<Output = (Self::Lease, O)>,
            {
                <T as Transport<S, I, O, Role, Codec>>::request(self, operation, input)
            }
        }
    };
}

dispatch!(ReadDispatch, ReadCall, Deref,);
dispatch!(WriteDispatch, WriteCall, DerefMut, mut);
