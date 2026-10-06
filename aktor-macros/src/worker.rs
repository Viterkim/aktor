use crate::{input, names, path, signature};
use proc_macro2::TokenStream;
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{FnArg, ReturnType, parse_quote};

pub fn supports(function: &input::Function) -> syn::Result<bool> {
    if !function.signature.generics.params.is_empty() {
        return Ok(false);
    }

    let state = input::parse::state(&function.signature.inputs[0])?;
    let mut unsupported = References(false);

    syn::visit::Visit::visit_type(&mut unsupported, &state.ty);

    for argument in function.signature.inputs.iter().skip(1) {
        if let FnArg::Typed(argument) = argument {
            syn::visit::Visit::visit_type(&mut unsupported, &argument.ty);
        }
    }

    syn::visit::Visit::visit_return_type(&mut unsupported, &function.signature.output);
    Ok(!unsupported.0)
}

pub fn expand(options: input::Options, mut function: input::Function) -> syn::Result<TokenStream> {
    let output = match &function.signature.output {
        ReturnType::Default => parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    let aktor = path::aktor(options.crate_path)?;
    let role = options
        .role
        .as_ref()
        .map_or_else(|| quote!(()), |role| quote!(#role));
    let mut names = names::Names::new(&function.signature, &function.body, quote!(#aktor #role));

    let state = input::parse::state(&function.signature.inputs[0])?;
    let state_type = &state.ty;
    let (implementation, invoke) = signature::implementation(&function, &mut names, &output)?;
    let body_type = &names.body;

    let input_type = signature::inputs(&function.signature);
    let input_types: Vec<_> = function
        .signature
        .inputs
        .iter()
        .skip(1)
        .filter_map(|argument| match argument {
            FnArg::Typed(argument) => Some((*argument.ty).clone()),
            _ => None,
        })
        .collect();

    let bindings: Vec<_> = function
        .inputs
        .iter()
        .zip(&names.arguments)
        .map(|(argument, generated)| match &argument.pattern {
            syn::Pat::Ident(pattern) if pattern.subpat.is_none() => pattern.ident.clone(),
            _ => generated.clone(),
        })
        .collect();

    for (argument, binding) in function.signature.inputs.iter_mut().skip(1).zip(&bindings) {
        if let FnArg::Typed(argument) = argument {
            *argument.pat = parse_quote!(#binding);
        }
    }

    let target = &names.target;
    let state_name = &names.state;
    let input_name = &names.input;
    let operation_name = &names.operation;
    let inner_name = &names.inner;
    let results_name = &names.results;
    let sender_name = &names.sender;
    let function_name = &names.function;
    let parameter = match &state.pattern {
        syn::Pat::Ident(pattern) if pattern.subpat.is_none() => pattern.ident.clone(),
        _ => state_name.clone(),
    };

    if let FnArg::Typed(argument) = &mut function.signature.inputs[0] {
        *argument.ty = parse_quote!(#target);
        *argument.pat = parse_quote!(#parameter);
    }

    let mode = names::binding(&mut names.reserved, "__AktorMode");
    let adapter = names::binding(&mut names.reserved, "__AktorExport");
    let dispatch_trait = names::binding(&mut names.reserved, "__AktorDispatch");
    let state_parameter = names::binding(&mut names.reserved, "__AktorState");
    let input_parameter = names::binding(&mut names.reserved, "__AktorInput");
    let output_parameter = names::binding(&mut names.reserved, "__AktorOutput");
    let role_parameter = names::binding(&mut names.reserved, "__AktorRole");
    let latest_factory = names::binding(&mut names.reserved, "__AktorLatestFactory");
    let latest_state = names::binding(&mut names.reserved, "__AktorLatestState");
    let latest_input = names::binding(&mut names.reserved, "__AktorLatestInput");
    let latest_output = names::binding(&mut names.reserved, "__AktorLatestOutput");
    let latest_role = names::binding(&mut names.reserved, "__AktorLatestRole");
    let latest_lease = names::binding(&mut names.reserved, "__AktorLatestLease");
    let latest_future = names::binding(&mut names.reserved, "__AktorLatestFuture");
    let latest_inner = names::binding(&mut names.reserved, "__AktorLatestInner");
    let function_parameter = names::binding(&mut names.reserved, "__AktorFunction");
    let future_parameter = names::binding(&mut names.reserved, "__AktorFuture");
    let future_name = names::binding(&mut names.reserved, "__aktor_future");
    let future_lifetime = syn::Lifetime::new(&format!("'{future_name}"), future_name.span());
    let output_name = names::binding(&mut names.reserved, "__aktor_output");
    let lease_bound = if state.mutable {
        quote!(::core::ops::DerefMut)
    } else {
        quote!(::core::ops::Deref)
    };

    let lease_binding = if state.mutable {
        quote!(mut #state_name)
    } else {
        quote!(#state_name)
    };

    let lease_reference = if state.mutable {
        quote!(&mut *#state_name)
    } else {
        quote!(&*#state_name)
    };

    let state_reference_generic = if state.mutable {
        quote!(&'s mut #state_parameter)
    } else {
        quote!(&'s #state_parameter)
    };

    let name = &function.signature.ident;

    function.signature.generics.params.push(
        parse_quote!(#target: #name::#dispatch_trait<#mode, #state_type, #input_type, #output, #role>),
    );
    function.signature.generics.params.push(parse_quote!(#mode));

    function
        .signature
        .generics
        .params
        .insert(0, parse_quote!(#future_lifetime));

    let predicates = &mut function.signature.generics.make_where_clause().predicates;

    predicates.push(parse_quote!(<#target as #name::#dispatch_trait<#mode, #state_type, #input_type, #output, #role>>::Lease: #future_lifetime));
    predicates.push(parse_quote!(#input_type: #future_lifetime));
    function.signature.asyncness = None;
    function.signature.output = parse_quote!(-> #aktor::call::Call<
        #target,
        #input_type,
        <#target as #name::#dispatch_trait<
            #mode, #state_type, #input_type, #output, #role
        >>::Request<
            impl ::core::future::Future<Output = (
                <#target as #name::#dispatch_trait<
                    #mode, #state_type, #input_type, #output, #role
                >>::Lease,
                #output
            )> + #future_lifetime
        >,
        #name::#latest_factory<
            #state_type,
            #input_type,
            #output,
            #role,
            <#target as #name::#dispatch_trait<
                #mode, #state_type, #input_type, #output, #role
            >>::Lease,
            impl ::core::future::Future<Output = (
                <#target as #name::#dispatch_trait<
                    #mode, #state_type, #input_type, #output, #role
                >>::Lease,
                #output
            )> + #future_lifetime
        >
    >);

    let signature = &function.signature;
    let visibility = &function.visibility;
    let attributes = input::attributes::wrapper(&function.attributes)?;

    let mut scope = signature::scope::Child::new(
        &[
            "NAME",
            "request",
            "export",
            "latest",
            "LatestSender",
            "Latest",
        ],
        &function.signature.generics,
    );

    let mut state_type = state_type.clone();

    scope.visit_type_mut(&mut state_type);

    let mut input_type = input_type.clone();

    scope.visit_type_mut(&mut input_type);

    let mut input_types = input_types;

    for ty in &mut input_types {
        scope.visit_type_mut(ty);
    }

    let mut output = output;

    scope.visit_type_mut(&mut output);

    let mut role: syn::Type = syn::parse2(role)?;

    scope.visit_type_mut(&mut role);

    let mut aktor: syn::Path = syn::parse2(aktor)?;

    scope.visit_path_mut(&mut aktor);

    let (wire, transport_note) = if options.data {
        (
            quote!(#aktor::dispatch::DataCodec),
            "Check the actor's state type and actor marker. #[aktor(data)] worker arguments and results must implement AktorData.",
        )
    } else {
        (
            quote!(#aktor::dispatch::SerdeCodec),
            "Check the actor's state type and actor marker. Browser worker arguments and results must implement Serialize and DeserializeOwned.",
        )
    };

    let dispatch = if state.mutable {
        quote!(#aktor::dispatch::WriteCall)
    } else {
        quote!(#aktor::dispatch::ReadCall)
    };

    let state_reference = if state.mutable {
        quote!(&mut #state_type)
    } else {
        quote!(&#state_type)
    };

    let latest_lease_trait = if state.mutable {
        quote!(#aktor::dispatch::WriteState<#state_type, #role>)
    } else {
        quote!(#aktor::dispatch::ReadState<#state_type, #role>)
    };

    let generated_bindings = &names.arguments;
    let latest_body = quote!(|#lease_binding: <#target as #latest_lease_trait>::Lease, #input_name: #input_type| async move {
        let (#(#generated_bindings,)*) = #input_name;
        let #output_name = #invoke(#lease_reference, #(#generated_bindings),*).await;
        (#state_name, #output_name)
    });

    let wrapper = quote! {
        #(#attributes)*
        #[track_caller]
        #visibility #signature {
            #name::request::<#future_lifetime, #target, #mode>(#parameter, #(#bindings),*)
        }

    };

    let bindings = &names.arguments;
    let parameter = state_name;

    Ok(quote! {
        #wrapper
        #implementation

        #visibility mod #name {
            use super::*;

            #[allow(non_camel_case_types)]
            #[doc(hidden)]
            pub struct #body_type;

            pub const NAME: &str = ::core::module_path!();

            #[derive(Clone)]
            pub struct LatestSender<#latest_inner> {
                pub inner: #latest_inner,
            }
            impl<#latest_inner: #aktor::latest::SendLatest<#input_type>> LatestSender<#latest_inner> {
                pub fn send(&self, #(#bindings: #input_types),*) {
                    #aktor::latest::SendLatest::send(&self.inner, (#(#bindings,)*));
                }
            }

            #[doc(hidden)]
            pub struct #latest_factory<#latest_state, #latest_input, #latest_output, #latest_role, #latest_lease, #latest_future> {
                pub factory: fn(#latest_lease, #latest_input) -> #latest_future,
                pub marker: ::core::marker::PhantomData<fn() -> (#latest_state, #latest_input, #latest_output, #latest_role)>,
            }
            impl<#target, #latest_lease, #latest_future> #aktor::latest::Factory<#target, #input_type>
                for #latest_factory<
                    #state_type, #input_type, #output, #role, #latest_lease, #latest_future
                >
            where
                #target: #aktor::latest::TypedSession<#state_type, #input_type, #output, #latest_lease, #role, #wire>,
                #latest_future: ::core::future::Future<Output = (#latest_lease, #output)>,
                <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #output, #latest_lease, #role, #wire
                >>::Sender<#latest_future>: #aktor::latest::SendLatest<#input_type>,
            {
                type Sender = LatestSender<
                    <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #output, #latest_lease, #role, #wire
                    >>::Sender<#latest_future>
                >;
                type Results = <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #output, #latest_lease, #role, #wire
                >>::Results;

                fn start(
                    self,
                    #parameter: #target,
                    #input_name: #input_type,
                    #operation_name: #aktor::operation::Operation
                ) -> (Self::Sender, Self::Results) {
                    let (#(#bindings,)*) = #input_name;
                    let (#inner_name, #results_name) = <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #output, #latest_lease, #role, #wire
                    >>::session(
                        #parameter, #operation_name,
                        async move |#state_name: &mut #state_type, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            #invoke(#state_name, #(#bindings),*).await
                        },
                        self.factory,
                    );
                    let #sender_name = LatestSender { inner: #inner_name };
                    #sender_name.send(#(#bindings),*);
                    (#sender_name, #results_name)
                }
            }

            #[track_caller]
            pub fn latest<#future_lifetime, #target>(#parameter: #target) -> (
                LatestSender<
                    <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #output,
                        <#target as #latest_lease_trait>::Lease, #role, #wire
                    >>::Sender<
                        impl ::core::future::Future<
                            Output = (<#target as #latest_lease_trait>::Lease, #output)
                        > + #future_lifetime
                    >
                >,
                <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #output, <#target as #latest_lease_trait>::Lease, #role, #wire
                >>::Results
            )
            where
                #target: #latest_lease_trait,
                <#target as #latest_lease_trait>::Lease: #future_lifetime,
                #target: #aktor::latest::TypedSession<#state_type, #input_type, #output, <#target as #latest_lease_trait>::Lease, #role, #wire>,
            {
                let (#inner_name, #results_name) = <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #output, <#target as #latest_lease_trait>::Lease, #role, #wire
                >>::session(
                    #parameter,
                    #aktor::operation::Operation { name: NAME, caller: ::core::panic::Location::caller() },
                    async move |#state_name: &mut #state_type, #input_name| {
                        let (#(#bindings,)*) = #input_name;
                        #invoke(#state_name, #(#bindings),*).await
                    },
                    #latest_body,
                );
                (LatestSender { inner: #inner_name }, #results_name)
            }

            #[doc(hidden)]
            #[diagnostic::on_unimplemented(
                note = #transport_note
            )]
            pub trait #dispatch_trait<
                #mode,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: 'static,
                #role_parameter
            > {
                type Lease: #lease_bound<Target = #state_parameter>;
                type Request<#future_parameter>;

                fn request<#function_parameter, #future_parameter>(
                    self,
                    #input_name: #input_parameter,
                    #operation_name: #aktor::operation::Operation,
                    #function_name: #function_parameter,
                    #sender_name: fn(Self::Lease, #input_parameter) -> #future_parameter
                ) -> Self::Request<#future_parameter>
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static,
                    #future_parameter: ::core::future::Future<Output = (Self::Lease, #output_parameter)>;
            }

            impl<
                #target,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: 'static,
                #role_parameter
            > #dispatch_trait<
                #aktor::dispatch::Native,
                #state_parameter,
                #input_parameter,
                #output_parameter,
                #role_parameter
            > for #target
            where
                #target: #dispatch<#state_parameter, #input_parameter, #output_parameter, #role_parameter>
            {
                type Lease = #target::Lease;
                type Request<#future_parameter> = <#target as #dispatch<
                    #state_parameter,
                    #input_parameter,
                    #output_parameter,
                    #role_parameter
                >>::Request<#future_parameter>;

                fn request<#function_parameter, #future_parameter>(
                    self,
                    #input_name: #input_parameter,
                    #operation_name: #aktor::operation::Operation,
                    #function_name: #function_parameter,
                    #sender_name: fn(Self::Lease, #input_parameter) -> #future_parameter
                ) -> Self::Request<#future_parameter>
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static,
                    #future_parameter: ::core::future::Future<Output = (Self::Lease, #output_parameter)>
                {
                    <#target as #dispatch<#state_parameter, #input_parameter, #output_parameter, #role_parameter>>::dispatch(
                        self,
                        #operation_name,
                        #function_name,
                        #input_name,
                        #sender_name
                    )
                }
            }
            impl<
                #target,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: 'static,
                #role_parameter
            > #dispatch_trait<
                #aktor::dispatch::Remote,
                #state_parameter,
                #input_parameter,
                #output_parameter,
                #role_parameter
            > for #target
            where
                #target: #aktor::dispatch::Transport<
                    #state_parameter,
                    #input_parameter,
                    #output_parameter,
                    #role_parameter,
                    #wire
                >
            {
                type Lease = #aktor::dispatch::OwnedState<#state_parameter>;
                type Request<#future_parameter> = <#target as #aktor::dispatch::Transport<
                    #state_parameter,
                    #input_parameter,
                    #output_parameter,
                    #role_parameter,
                    #wire
                >>::Request;

                fn request<#function_parameter, #future_parameter>(
                    self,
                    #input_name: #input_parameter,
                    #operation_name: #aktor::operation::Operation,
                    _: #function_parameter,
                    _: fn(Self::Lease, #input_parameter) -> #future_parameter
                ) -> Self::Request<#future_parameter>
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static,
                    #future_parameter: ::core::future::Future<Output = (Self::Lease, #output_parameter)>
                {
                    <#target as #aktor::dispatch::Transport<
                        #state_parameter, #input_parameter, #output_parameter, #role_parameter, #wire
                    >>::request(self, #operation_name, #input_name)
                }
            }

            #[track_caller]
            pub fn request<
                #future_lifetime,
                #target: #dispatch_trait<#mode, #state_type, #input_type, #output, #role>,
                #mode
            >(#parameter: #target, #(#bindings: #input_types),*) -> #aktor::call::Call<
                #target,
                #input_type,
                #target::Request<
                    impl ::core::future::Future<Output = (#target::Lease, #output)>
                        + #future_lifetime
                >,
                #latest_factory<
                    #state_type, #input_type, #output, #role, #target::Lease,
                    impl ::core::future::Future<Output = (#target::Lease, #output)>
                        + #future_lifetime
                >
            >
            where
                #target::Lease: #future_lifetime,
                #input_type: #future_lifetime,
            {
                #aktor::call::Call::new(
                    #parameter, (#(#bindings,)*),
                    #aktor::operation::Operation { name: NAME, caller: ::core::panic::Location::caller() },
                    |#parameter, #input_name, #operation_name| #parameter.request(
                        #input_name, #operation_name,
                        async move |#state_name: #state_reference, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            #invoke(#state_name, #(#bindings),*).await
                        },
                        |#lease_binding: #target::Lease, #input_name| async move {
                            let (#(#bindings,)*) = #input_name;
                            let #output_name = #invoke(#lease_reference, #(#bindings),*).await;
                            (#state_name, #output_name)
                        },
                    ),
                    #latest_factory {
                        factory: |#lease_binding: #target::Lease, #input_name: #input_type| async move {
                            let (#(#bindings,)*) = #input_name;
                            let #output_name = #invoke(#lease_reference, #(#bindings),*).await;
                            (#state_name, #output_name)
                        },
                        marker: ::core::marker::PhantomData,
                    },
                )
            }

            #[doc(hidden)]
            #[derive(Default)]
            struct #adapter;
            impl #aktor::dispatch::Export<#wire> for #adapter {
                type State = #state_type;
                type Input = #input_type;
                type Output = #output;
                type Role = #role;

                fn run(
                    self,
                    #state_name: &mut #state_type,
                    #input_name: #input_type
                ) -> #aktor::message::LocalFuture<'_, #output> {
                    #aktor::message::boxed(async move {
                        let (#(#bindings,)*) = #input_name;
                        #invoke(#state_name, #(#bindings),*).await
                    })
                }
            }

            pub fn export() -> impl #aktor::dispatch::Export<#wire,
                State = #state_type,
                Input = #input_type,
                Output = #output,
                Role = #role
            > {
                #adapter
            }

            #aktor::__aktor_register!(#adapter, NAME, #wire);
        }
    })
}

struct References(bool);
impl<'ast> syn::visit::Visit<'ast> for References {
    fn visit_lifetime(&mut self, _: &'ast syn::Lifetime) {
        self.0 = true;
    }

    fn visit_type_reference(&mut self, _: &'ast syn::TypeReference) {
        self.0 = true;
    }

    fn visit_type_impl_trait(&mut self, _: &'ast syn::TypeImplTrait) {
        self.0 = true;
    }
}
