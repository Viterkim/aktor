use crate::{input, names, path, signature};
use proc_macro2::TokenStream;
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{FnArg, parse_quote};

pub fn expand(options: input::Options, mut function: input::Function) -> syn::Result<TokenStream> {
    if crate::worker::supports(&function)? {
        return crate::worker::expand(options, function);
    }

    if options.data {
        return Err(syn::Error::new_spanned(
            &function.signature,
            "#[aktor(data)] needs concrete owned arguments and an owned result",
        ));
    }

    let aktor = path::aktor(options.crate_path)?;
    let role = options
        .role
        .map_or_else(|| quote!(()), |role| quote!(#role));
    let mut names = names::Names::new(&function.signature, &function.body, quote!(#aktor #role));
    let output = signature::output(&function.signature);
    let (implementation, invoke) = signature::implementation(&function, &mut names, &output)?;
    let output = signature::dispatch_output(output);
    let mut opaque_output = OpaqueOutput(false);

    syn::visit::Visit::visit_type(&mut opaque_output, &output);

    let latest_factory = names::binding(&mut names.reserved, "__AktorLatestFactory");
    let latest_state = names::binding(&mut names.reserved, "__AktorLatestState");
    let latest_input = names::binding(&mut names.reserved, "__AktorLatestInput");
    let latest_output = names::binding(&mut names.reserved, "__AktorLatestOutput");
    let latest_role = names::binding(&mut names.reserved, "__AktorLatestRole");
    let latest_lease = names::binding(&mut names.reserved, "__AktorLatestLease");
    let latest_future = names::binding(&mut names.reserved, "__AktorLatestFuture");
    let latest_inner = names::binding(&mut names.reserved, "__AktorLatestInner");

    signature::normalize(&mut function.signature, &mut names);

    let body_type = &names.body;

    let state = input::parse::state(&function.signature.inputs[0])?;
    let state_type = &state.ty;
    let dispatch = if state.mutable {
        if opaque_output.0 {
            quote!(#aktor::dispatch::WriteOpaque)
        } else {
            quote!(#aktor::dispatch::WriteCall)
        }
    } else {
        if opaque_output.0 {
            quote!(#aktor::dispatch::ReadOpaque)
        } else {
            quote!(#aktor::dispatch::ReadCall)
        }
    };

    let lease_trait = if state.mutable {
        quote!(#aktor::dispatch::WriteState<#state_type, #role>)
    } else {
        quote!(#aktor::dispatch::ReadState<#state_type, #role>)
    };

    let input_type = signature::inputs(&function.signature);
    let dispatch_type = if opaque_output.0 {
        quote!(#dispatch<#state_type, #input_type, #role>)
    } else {
        quote!(#dispatch<#state_type, #input_type, #output, #role>)
    };

    let target = &names.target;
    let state_name = &names.state;
    let input_name = &names.input;
    let operation_name = &names.operation;
    let inner_name = &names.inner;
    let results_name = &names.results;
    let sender_name = &names.sender;

    let future_name = names::binding(&mut names.reserved, "__aktor_future");
    let future_lifetime = syn::Lifetime::new(&format!("'{future_name}"), future_name.span());
    let output_name = names::binding(&mut names.reserved, "__aktor_output");

    let bindings: Vec<_> = function
        .inputs
        .iter()
        .zip(&names.arguments)
        .map(|(argument, generated)| match &argument.pattern {
            syn::Pat::Ident(pattern) if pattern.subpat.is_none() => pattern.ident.clone(),
            _ => generated.clone(),
        })
        .collect();

    let parameter = match &state.pattern {
        syn::Pat::Ident(pattern) if pattern.subpat.is_none() => pattern.ident.clone(),
        _ => state_name.clone(),
    };

    function.signature.asyncness = None;
    function
        .signature
        .generics
        .params
        .push(parse_quote!(#target: #dispatch_type));

    function
        .signature
        .generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#state_type: 'static));

    let latest_generics = function.signature.generics.clone();

    {
        function
            .signature
            .generics
            .params
            .insert(0, parse_quote!(#future_lifetime));

        let predicates = &mut function.signature.generics.make_where_clause().predicates;

        predicates.push(parse_quote!(<#target as #lease_trait>::Lease: #future_lifetime));
        predicates.push(parse_quote!(#input_type: #future_lifetime));
    }

    function.signature.output = if opaque_output.0 {
        parse_quote!(-> #aktor::call::Call<
            #target,
            #input_type,
            <#target as #dispatch_type>::Output<
                impl ::core::future::Future<
                    Output = (<#target as #lease_trait>::Lease, #output)
                > + #future_lifetime
            >
        >)
    } else {
        parse_quote!(-> <#target as #dispatch_type>::Request<
            impl ::core::future::Future<
                Output = (<#target as #lease_trait>::Lease, #output)
            > + #future_lifetime
        >)
    };

    for (index, argument) in function.signature.inputs.iter_mut().enumerate() {
        if let FnArg::Typed(argument) = argument {
            if index == 0 {
                *argument.ty = parse_quote!(#target);
                *argument.pat = parse_quote!(#parameter);
            } else {
                let binding = &bindings[index - 1];
                *argument.pat = parse_quote!(#binding);
            }
        }
    }

    let mut factory_arguments = signature::arguments(&latest_generics);
    factory_arguments.pop();

    let raw_output = function.signature.output.clone();

    if !opaque_output.0 {
        let name = &function.signature.ident;
        let syn::ReturnType::Type(_, raw) = &raw_output else {
            return Err(syn::Error::new_spanned(
                &function.signature,
                "missing actor output",
            ));
        };

        function.signature.output = parse_quote!(-> #aktor::call::Call<
            #target,
            #input_type,
            #raw,
            #name::#latest_factory<
                #(#factory_arguments,)*
                #state_type,
                #input_type,
                #output,
                #role,
                <#target as #lease_trait>::Lease,
                impl ::core::future::Future<
                    Output = (<#target as #lease_trait>::Lease, #output)
                > + #future_lifetime
            >
        >);
    }

    let attributes = crate::input::attributes::wrapper(&function.attributes)?;
    let visibility = &function.visibility;
    let signature = &function.signature;
    let name = &signature.ident;
    let generic_arguments = signature::arguments(&signature.generics);

    let wrapper = quote! {
        #(#attributes)*
        #[allow(clippy::extra_unused_lifetimes)]
        #[track_caller]
        #visibility #signature {
            #name::request::<#(#generic_arguments),*>(#parameter, #(#bindings),*)
        }
    };

    let bindings = &names.arguments;
    let parameter = state_name;

    let mut request = signature.clone();

    request.ident = parse_quote!(request);

    for (argument, binding) in request
        .inputs
        .iter_mut()
        .zip(::core::iter::once(parameter).chain(bindings.iter()))
    {
        if let FnArg::Typed(argument) = argument {
            *argument.pat = parse_quote!(#binding);
        }
    }

    let mut scope =
        signature::scope::Child::new(&["request", "latest", "LatestSender"], &request.generics);

    scope.visit_signature_mut(&mut request);

    if !opaque_output.0
        && let syn::ReturnType::Type(_, ty) = &mut request.output
        && let syn::Type::Path(path) = &mut **ty
        && let Some(segment) = path.path.segments.last_mut()
        && let syn::PathArguments::AngleBracketed(arguments) = &mut segment.arguments
        && let Some(syn::GenericArgument::Type(syn::Type::Path(factory))) =
            arguments.args.last_mut()
        && let Some(name) = factory.path.segments.last().cloned()
    {
        factory.path.segments = ::core::iter::once(name).collect();
    }

    let mut state_type = state_type.clone();

    scope.visit_type_mut(&mut state_type);

    let mut input_type = input_type.clone();

    scope.visit_type_mut(&mut input_type);

    let mut role: syn::Type = syn::parse2(role)?;

    scope.visit_type_mut(&mut role);

    let mut aktor: syn::Path = syn::parse2(aktor)?;

    scope.visit_path_mut(&mut aktor);

    let dispatch = if state.mutable {
        if opaque_output.0 {
            quote!(#aktor::dispatch::WriteOpaque)
        } else {
            quote!(#aktor::dispatch::WriteCall)
        }
    } else {
        if opaque_output.0 {
            quote!(#aktor::dispatch::ReadOpaque)
        } else {
            quote!(#aktor::dispatch::ReadCall)
        }
    };

    let mut relocated_output = output.clone();

    scope.visit_type_mut(&mut relocated_output);

    let dispatch_type = if opaque_output.0 {
        quote!(#dispatch<#state_type, #input_type, #role>)
    } else {
        quote!(#dispatch<#state_type, #input_type, #relocated_output, #role>)
    };

    let lease_trait = if state.mutable {
        quote!(#aktor::dispatch::WriteState<#state_type, #role>)
    } else {
        quote!(#aktor::dispatch::ReadState<#state_type, #role>)
    };

    let state_reference = if state.mutable {
        quote!(&mut #state_type)
    } else {
        quote!(&#state_type)
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

    let factory = quote! {
        |#lease_binding: <#target as #lease_trait>::Lease, #input_name: #input_type| async move {
            let (#(#bindings,)*) = #input_name;
            let #output_name = #invoke(#lease_reference, #(#bindings),*).await;
            (#state_name, #output_name)
        },
    };

    let latest = if opaque_output.0 {
        TokenStream::new()
    } else {
        let mut session_output = output.clone();

        scope.visit_type_mut(&mut session_output);

        let input_types: Vec<_> = request
            .inputs
            .iter()
            .skip(1)
            .filter_map(|argument| match argument {
                FnArg::Typed(argument) => Some(&argument.ty),
                _ => None,
            })
            .collect();

        let mut user_generics = latest_generics.clone();

        scope.visit_generics_mut(&mut user_generics);
        user_generics.params.pop();

        let arguments = signature::arguments(&user_generics);

        let markers: Vec<_> = user_generics
            .params
            .iter()
            .filter_map(|parameter| match parameter {
                syn::GenericParam::Lifetime(parameter) => {
                    let lifetime = &parameter.lifetime;
                    Some(quote!(&#lifetime ()))
                }
                syn::GenericParam::Type(parameter) => {
                    let name = &parameter.ident;
                    Some(quote!(*const #name))
                }
                _ => None,
            })
            .collect();

        let mut sender_generics = user_generics.clone();

        sender_generics.params.push(parse_quote!(#latest_inner));

        let (sender_impl, _, sender_where) = sender_generics.split_for_impl();
        let sender_type = quote!(<#(#arguments,)* #latest_inner>);
        let mut send_generics = sender_generics.clone();

        send_generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#latest_inner: #aktor::latest::SendLatest<#input_type>));

        let (send_impl, _, send_where) = send_generics.split_for_impl();
        let mut clone_generics = sender_generics.clone();

        clone_generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#latest_inner: ::core::clone::Clone));

        let (clone_impl, _, clone_where) = clone_generics.split_for_impl();
        let mut latest_signature = request.clone();

        latest_signature.ident = parse_quote!(latest);
        latest_signature.inputs = latest_signature.inputs.into_iter().take(1).collect();
        latest_signature.generics = user_generics.clone();
        latest_signature.generics.params.push(parse_quote!(#target));

        let mut bounds: Vec<syn::WherePredicate> = Vec::new();

        for parameter in &mut latest_signature.generics.params {
            if let syn::GenericParam::Type(parameter) = parameter
                && !parameter.bounds.is_empty()
            {
                let name = &parameter.ident;
                let bound = core::mem::take(&mut parameter.bounds);
                bounds.push(parse_quote!(#name: #bound));
            }
        }

        let predicates = &mut latest_signature.generics.make_where_clause().predicates;

        predicates.extend(bounds);
        predicates.push(parse_quote!(#target: #lease_trait));
        predicates.push(parse_quote!(#target: #aktor::latest::TypedSession<
            #state_type, #input_type, #session_output, <#target as #lease_trait>::Lease, #role
        >));
        predicates.push(parse_quote!(<#target as #lease_trait>::Lease: #future_lifetime));
        predicates.push(parse_quote!(#input_type: 'static));
        predicates.push(parse_quote!(#session_output: 'static));
        latest_signature
            .generics
            .params
            .insert(0, parse_quote!(#future_lifetime));

        let mut factory_generics = sender_generics.clone();

        factory_generics.params.pop();

        for name in [
            &latest_state,
            &latest_input,
            &latest_output,
            &latest_role,
            &latest_lease,
            &latest_future,
        ] {
            factory_generics.params.push(parse_quote!(#name));
        }

        let (factory_impl, _, factory_where) = factory_generics.split_for_impl();
        let mut start_generics = user_generics.clone();

        start_generics.params.push(parse_quote!(#target));
        start_generics.params.push(parse_quote!(#latest_lease));
        start_generics.params.push(parse_quote!(#latest_future));

        let predicates = &mut start_generics.make_where_clause().predicates;

        predicates.push(parse_quote!(
            #target: #aktor::latest::TypedSession<
                #state_type, #input_type, #session_output, #latest_lease, #role
            >
        ));
        predicates.push(parse_quote!(
            #latest_future: ::core::future::Future<Output = (#latest_lease, #session_output)>
        ));
        predicates.push(parse_quote!(
            <#target as #aktor::latest::TypedSession<
                #state_type, #input_type, #session_output, #latest_lease, #role
            >>::Sender<#latest_future>: #aktor::latest::SendLatest<#input_type>
        ));
        predicates.push(parse_quote!(#input_type: 'static));
        predicates.push(parse_quote!(#session_output: 'static));

        let (latest_impl, _, latest_where) = start_generics.split_for_impl();

        latest_signature.output = parse_quote!(-> (
            self::LatestSender<
                #(#arguments,)*
                <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #session_output, <#target as #lease_trait>::Lease, #role
                >>::Sender<
                    impl ::core::future::Future<
                        Output = (<#target as #lease_trait>::Lease, #session_output)
                    > + #future_lifetime
                >
            >,
            <#target as #aktor::latest::TypedSession<
                #state_type, #input_type, #session_output, <#target as #lease_trait>::Lease, #role
            >>::Results
        ));

        quote! {
            #[doc(hidden)]
            pub struct #latest_factory #factory_impl #factory_where {
                pub factory: fn(#latest_lease, #latest_input) -> #latest_future,
                pub marker: ::core::marker::PhantomData<
                    fn(#(#markers),*) -> (#latest_state, #latest_input, #latest_output, #latest_role)
                >,
            }
            impl #latest_impl #aktor::latest::Factory<#target, #input_type>
                for #latest_factory<
                    #(#arguments,)* #state_type, #input_type, #session_output,
                    #role, #latest_lease, #latest_future
                > #latest_where
            {
                type Sender = self::LatestSender<
                    #(#arguments,)*
                    <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #session_output, #latest_lease, #role
                    >>::Sender<#latest_future>
                >;
                type Results = <#target as #aktor::latest::TypedSession<
                    #state_type, #input_type, #session_output, #latest_lease, #role
                >>::Results;

                fn start(
                    self,
                    #parameter: #target,
                    #input_name: #input_type,
                    #operation_name: #aktor::operation::Operation
                ) -> (Self::Sender, Self::Results) {
                    let (#(#bindings,)*) = #input_name;

                    let (#inner_name, #results_name) = #aktor::latest::TypedSession::session(
                        #parameter, #operation_name,
                        async move |#state_name: &mut #state_type, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            #invoke(#state_name, #(#bindings),*).await
                        },
                        self.factory,
                    );
                    let #sender_name = self::LatestSender { inner: #inner_name, marker: ::core::marker::PhantomData };

                    #sender_name.send(#(#bindings),*);

                    (#sender_name, #results_name)
                }
            }

            pub struct LatestSender #sender_impl #sender_where {
                pub inner: #latest_inner,
                marker: ::core::marker::PhantomData<fn(#(#markers),*)>,
            }
            impl #send_impl self::LatestSender #sender_type #send_where {
                pub fn send(&self, #(#bindings: #input_types),*) {
                    #aktor::latest::SendLatest::send(&self.inner, (#(#bindings,)*));
                }
            }
            impl #clone_impl ::core::clone::Clone for self::LatestSender #sender_type #clone_where {
                fn clone(&self) -> Self {
                    Self { inner: self.inner.clone(), marker: ::core::marker::PhantomData }
                }
            }

            #[track_caller]
            pub #latest_signature {
                let (#inner_name, #results_name) = #aktor::latest::TypedSession::session(
                    #parameter,
                    #aktor::operation::Operation { name: ::core::module_path!(), caller: ::core::panic::Location::caller() },
                    async move |#state_name: &mut #state_type, #input_name| {
                        let (#(#bindings,)*) = #input_name;
                        #invoke(#state_name, #(#bindings),*).await
                    },
                    #factory
                );
                (self::LatestSender { inner: #inner_name, marker: ::core::marker::PhantomData }, #results_name)
            }
        }
    };

    let dispatch_body = quote! {
        <#target as #dispatch_type>::dispatch(
            #parameter, #operation_name,
            async move |#state_name: #state_reference, #input_name: #input_type| {
                let (#(#bindings,)*) = #input_name;
                #invoke(#state_name, #(#bindings),*).await
            },
            #input_name,
            #factory
        )
    };

    let operation = quote!(#aktor::operation::Operation {
        name: ::core::module_path!(), caller: ::core::panic::Location::caller(),
    });
    let request_body = if opaque_output.0 {
        quote! {
            #aktor::call::Call::new(
                #parameter,
                (#(#bindings,)*),
                #operation,
                |#parameter: #target, #input_name: #input_type, #operation_name| {
                    #dispatch_body
                },
                ()
            )
        }
    } else {
        quote! {
            #aktor::call::Call::new(
                #parameter, (#(#bindings,)*), #operation,
                |#parameter: #target, #input_name: #input_type, #operation_name| { #dispatch_body },
                #latest_factory { factory: #factory marker: ::core::marker::PhantomData },
            )
        }
    };

    Ok(quote! {
        #wrapper

        #implementation

        #visibility mod #name {
            use super::*;

            #[allow(non_camel_case_types)]
            #[doc(hidden)]
            pub struct #body_type;

            #latest

            #[track_caller]
            #[allow(clippy::extra_unused_lifetimes)]
            pub #request {
                #request_body
            }
        }
    })
}

struct OpaqueOutput(bool);
impl<'ast> syn::visit::Visit<'ast> for OpaqueOutput {
    fn visit_type_impl_trait(&mut self, _: &'ast syn::TypeImplTrait) {
        self.0 = true;
    }
}
