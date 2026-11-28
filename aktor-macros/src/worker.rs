use crate::{input, names, path, signature};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::visit_mut::VisitMut;
use syn::{FnArg, ReturnType, parse_quote};

pub fn supports(function: &input::Function) -> syn::Result<bool> {
    if function
        .signature
        .generics
        .params
        .iter()
        .any(|param| !matches!(param, syn::GenericParam::Lifetime(_)))
    {
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
    let remote = options.remote;
    let wire_name = options
        .name
        .as_ref()
        .map_or_else(|| quote!(::core::module_path!()), |name| quote!(#name));
    let output = match &function.signature.output {
        ReturnType::Default => parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    let aktor = path::aktor(options.crate_path)?;
    let outer_aktor = aktor.clone();
    let role = options
        .role
        .as_ref()
        .map_or_else(|| quote!(()), |role| quote!(#role));
    let mut names = names::Names::new(&function.signature, &function.body, quote!(#aktor #role));

    let state = input::parse::state(&function.signature.inputs[0])?;
    let state_type = &state.ty;
    let serve = serve_cfg(state_type);
    let (implementation, invoke) =
        signature::implementation(&function, &mut names, &output, &aktor)?;
    let body_type = &names.body;

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

    let argument_type = syn::parse2(arguments(&input_types))?;
    let result_type = output;
    let mut callbacks = Callbacks(false);
    syn::visit::Visit::visit_type(&mut callbacks, &argument_type);
    syn::visit::Visit::visit_type(&mut callbacks, &result_type);

    let args_alias = names::binding(&mut names.reserved, "__AktorArguments");
    let result_alias = names::binding(&mut names.reserved, "__AktorResult");
    let module_name = &function.signature.ident;
    let (input_type, output): (syn::Type, syn::Type) = if callbacks.0 {
        (
            parse_quote!(#module_name::#args_alias),
            parse_quote!(#module_name::#result_alias),
        )
    } else {
        (argument_type.clone(), result_type.clone())
    };
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
    let body = names::binding(&mut names.reserved, "__aktor_call");
    let latest_body_fn = names::binding(&mut names.reserved, "__aktor_latest_call");
    let dispatch_trait = if state.mutable {
        quote!(WriteDispatch)
    } else {
        quote!(ReadDispatch)
    };
    // Built from whichever crate path applies where it lands, the module sees a relocated one.
    let codec = |aktor: &dyn quote::ToTokens| {
        if remote {
            quote!(#aktor::dispatch::RemoteCodec<#aktor::dispatch::DataCodec>)
        } else {
            quote!(#aktor::dispatch::DataCodec)
        }
    };
    let outer_codec = codec(&aktor);

    let latest_factory = names::binding(&mut names.reserved, "__AktorLatestFactory");
    let latest_state = names::binding(&mut names.reserved, "__AktorLatestState");
    let latest_input = names::binding(&mut names.reserved, "__AktorLatestInput");
    let latest_output = names::binding(&mut names.reserved, "__AktorLatestOutput");
    let latest_role = names::binding(&mut names.reserved, "__AktorLatestRole");
    let latest_lease = names::binding(&mut names.reserved, "__AktorLatestLease");
    let latest_future = names::binding(&mut names.reserved, "__AktorLatestFuture");
    let latest_inner = names::binding(&mut names.reserved, "__AktorLatestInner");

    let function_parameter = names::binding(&mut names.reserved, "__AktorFunction");
    let function_name = &names.function;

    let future_name = names::binding(&mut names.reserved, "__aktor_future");
    let future_lifetime = syn::Lifetime::new(&format!("'{future_name}"), future_name.span());
    let output_name = names::binding(&mut names.reserved, "__aktor_output");

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

    let name = &function.signature.ident;

    function.signature.generics.params.push(
        parse_quote!(#target: #aktor::dispatch::#dispatch_trait<#mode, #state_type, #input_type, #output, #role, #outer_codec>),
    );
    function.signature.generics.params.push(parse_quote!(#mode));

    function
        .signature
        .generics
        .params
        .insert(0, parse_quote!(#future_lifetime));

    let predicates = &mut function.signature.generics.make_where_clause().predicates;

    predicates.push(parse_quote!(#target::Lease: #future_lifetime));
    predicates.push(parse_quote!(#input_type: #future_lifetime));

    // Only a function marked `latest` carries the factory `.latest()` needs.
    let wrapper_latest = match options.latest {
        true => quote!(#name::#latest_factory<
            #state_type, #input_type, #output, #role, #target::Lease,
            impl ::core::future::Future<Output = (#target::Lease, #output)>
                + #future_lifetime
        >),
        false => quote!(#aktor::latest::AddAktorLatest),
    };

    function.signature.asyncness = None;
    function.signature.output = parse_quote!(-> #aktor::call::Call<
        #target,
        #input_type,
        #target::Request<
            impl ::core::future::Future<Output = (#target::Lease, #output)>
                + #future_lifetime
        >,
        #wrapper_latest
    >);

    let signature = &function.signature;
    let visibility = &function.visibility;
    let factory_visibility = signature::scope::nested_visibility(visibility);
    let attributes = input::attributes::wrapper(&function.attributes)?;
    let lints = input::attributes::lints(&function.attributes)?;

    // Docs instead of an allow, so `forbid(missing_docs)` crates can use `pub` functions too.
    let module_docs = format!(
        "More ways to call `{}`, like `.latest()` and test handlers.",
        function
            .signature
            .ident
            .to_string()
            .trim_start_matches("r#")
    );

    let input = arguments(&bindings);
    let inner_bindings = &names.arguments;
    let unpack = arguments(inner_bindings);
    let lease_factory = quote!(|#lease_binding: #target::Lease, #input_name: #input_type| async move {
        let #unpack = #input_name;
        let #output_name = #name::#invoke(#lease_reference, #(#inner_bindings),*).await;
        (#state_name, #output_name)
    });
    let factory = if options.latest {
        quote!(#name::#latest_factory { factory: #lease_factory, marker: ::core::marker::PhantomData })
    } else {
        quote!(#aktor::latest::AddAktorLatest)
    };
    let operation = quote!(#aktor::operation::Operation {
        name: #name::NAME, caller: ::core::panic::Location::caller()
    });
    let wrapper = quote! {
        #(#attributes)*
        #[track_caller]
        #visibility #signature {
            #aktor::call::Call::new(
                #parameter, #input, #operation,
                |#parameter, #input_name, #operation_name| <#target as #aktor::dispatch::#dispatch_trait<
                    #mode, #state_type, #input_type, #output, #role, #outer_codec
                >>::request(#parameter, #input_name, #operation_name, #name::#body, #lease_factory),
                #factory,
            )
        }
    };

    let transport =
        quote!(#aktor::dispatch::Transport<#state_type, #input_type, #output, #role, #outer_codec>);
    let page_factory = if options.latest {
        quote!(#name::#latest_factory)
    } else {
        quote!(#aktor::latest::AddAktorLatest)
    };
    let mut page_signature = signature.clone();
    page_signature.generics = parse_quote!(<#target: #transport>);
    page_signature.output =
        parse_quote!(-> #aktor::call::Call<#target, #input_type, #target::Request, #page_factory>);
    let page = quote! {
        #(#attributes)*
        #[track_caller]
        #visibility #page_signature {
            #aktor::call::Call::new(
                #parameter, #input, #operation,
                |#parameter, #input_name, #operation_name| <#target as #transport>::request(#parameter, #operation_name, #input_name),
                #page_factory,
            )
        }
    };

    let mut scope = signature::scope::NestedModule::new(
        &["NAME", "export", "handler", "latest", "Sender"],
        &function.signature.generics,
    );

    let mut state_type = state_type.clone();

    scope.visit_type_mut(&mut state_type);

    let mut argument_type = argument_type;
    let mut result_type = result_type;
    scope.visit_type_mut(&mut argument_type);
    scope.visit_type_mut(&mut result_type);

    let alias_item = callbacks.0.then(|| {
        quote! {
            #[doc(hidden)]
            #factory_visibility type #args_alias = #argument_type;
            #[doc(hidden)]
            #factory_visibility type #result_alias = #result_type;
        }
    });

    let (input_type, output): (syn::Type, syn::Type) = if callbacks.0 {
        (parse_quote!(#args_alias), parse_quote!(#result_alias))
    } else {
        (argument_type, result_type)
    };

    let mut input_types = input_types;

    for ty in &mut input_types {
        scope.visit_type_mut(ty);
    }

    let mut role: syn::Type = syn::parse2(role)?;

    scope.visit_type_mut(&mut role);

    let mut aktor: syn::Path = syn::parse2(aktor)?;

    scope.visit_path_mut(&mut aktor);

    let wire = codec(&aktor);

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
        let #unpack = #input_name;
        let #output_name = #invoke(#lease_reference, #(#generated_bindings),*).await;
        (#state_name, #output_name)
    });

    let bindings = &names.arguments;
    let parameter = state_name;

    let (latest_call, latest_decl) = if options.latest && !state.mutable {
        (
            &latest_body_fn,
            quote! {
                async fn #latest_body_fn(#state_name: &mut #state_type, #input_name: #input_type) -> #output {
                    #body(#state_name, #input_name).await
                }
            },
        )
    } else {
        (&body, quote!())
    };

    let latest_items = match options.latest {
        false => quote! {
            #[doc(hidden)]
            pub fn latest<#target: #aktor::latest::Marked>(_: #target) {}
        },
        true => quote! {
            /// Sends the newest input to a `latest()` session.
            #[derive(::core::clone::Clone)]
            #factory_visibility struct Sender<#latest_inner = #aktor::latest::DefaultSender<#input_type>> {
                /// The session underneath.
                pub inner: #latest_inner,
            }
            impl<#latest_inner: #aktor::latest::SendLatest<#input_type>> Sender<#latest_inner> {
                /// Replace the input that's still waiting with this one.
                pub fn send(&self, #(#bindings: #input_types),*) {
                    #aktor::latest::SendLatest::send(&self.inner, #unpack);
                }
            }

            impl<#latest_inner: #aktor::latest::SendLatest<#input_type>> #aktor::latest::SendLatest<#input_type> for Sender<#latest_inner> {
                fn send(&self, #input_name: #input_type) {
                    #aktor::latest::SendLatest::send(&self.inner, #input_name);
                }
            }
            impl<#latest_inner: #aktor::call::RetryRequest> #aktor::call::RetryRequest for Sender<#latest_inner> {
                type Options = #latest_inner::Options;

                fn retry(self, options: Self::Options) -> Self {
                    Self { inner: #aktor::call::RetryRequest::retry(self.inner, options) }
                }
            }

            #aktor::__aktor_body! { body {
                #[doc(hidden)]
                #factory_visibility struct #latest_factory<#latest_state, #latest_input, #latest_output, #latest_role, #latest_lease, #latest_future> {
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
                    type Sender = Sender<
                        <#target as #aktor::latest::TypedSession<
                            #state_type, #input_type, #output, #latest_lease, #role, #wire
                        >>::Sender<#latest_future>
                    >;
                    type Results = <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #output, #latest_lease, #role, #wire
                    >>::Results;

                    fn create(
                        self,
                        #parameter: #target,
                        #operation_name: #aktor::operation::Operation
                    ) -> (Self::Sender, Self::Results) {
                        let (#inner_name, #results_name) = <#target as #aktor::latest::TypedSession<
                            #state_type, #input_type, #output, #latest_lease, #role, #wire
                        >>::session(
                            #parameter, #operation_name,
                            #latest_call,
                            self.factory,
                        );
                        let #sender_name = Sender { inner: #inner_name };
                        (#sender_name, #results_name)
                    }
                }

                /// Keep only the newest input and read the results as they finish.
                #[track_caller]
                pub fn latest<#future_lifetime, #target>(#parameter: #target) -> (
                    Sender<
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
                    #target: #aktor::latest::TypedSession<
                        #state_type, #input_type, #output,
                        <#target as #latest_lease_trait>::Lease, #role, #wire
                    >,
                {
                    let (#inner_name, #results_name) = <#target as #aktor::latest::TypedSession<
                        #state_type, #input_type, #output, <#target as #latest_lease_trait>::Lease, #role, #wire
                    >>::session(
                        #parameter,
                        #aktor::operation::Operation { name: NAME, caller: ::core::panic::Location::caller() },
                        #latest_call,
                        #latest_body,
                    );
                    (Sender { inner: #inner_name }, #results_name)
                }
            } page {
                #[doc(hidden)]
                #factory_visibility struct #latest_factory;

                impl<#target> #aktor::latest::Factory<#target, #input_type> for #latest_factory
                where
                    #target: #aktor::latest::WorkerSession<#state_type, #input_type, #output, #role, #wire>,
                {
                    type Sender = Sender<#target::Sender>;
                    type Results = #target::Results;

                    fn create(self, #parameter: #target, #operation_name: #aktor::operation::Operation) -> (Self::Sender, Self::Results) {
                        let (#inner_name, #results_name) = #parameter.open_session(#operation_name);
                        (Sender { inner: #inner_name }, #results_name)
                    }
                }

                /// Keep only the newest input and read the results as they finish.
                #[track_caller]
                pub fn latest<#target>(#parameter: #target) -> (Sender<#target::Sender>, #target::Results)
                where
                    #target: #aktor::latest::WorkerSession<#state_type, #input_type, #output, #role, #wire>,
                {
                    #aktor::latest::Factory::create(#latest_factory, #parameter, #aktor::operation::Operation {
                        name: NAME, caller: ::core::panic::Location::caller(),
                    })
                }
            } }
        },
    };

    let items = quote! {
        #outer_aktor::__aktor_body! { body { #wrapper #implementation } page { #page } }

        #(#lints)*
        #[doc = #module_docs]
        #visibility mod #name {
            use super::*;

            #alias_item

            #aktor::__aktor_body! {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                pub struct #body_type;

                pub(super) async fn #body(#state_name: #state_reference, #input_name: #input_type) -> #output {
                    let #unpack = #input_name;
                    #invoke(#state_name, #(#bindings),*).await
                }
                #latest_decl
            }

            #[doc(hidden)]
            pub const NAME: &str = #wire_name;
            #latest_items

            #[doc(hidden)]
            #[derive(::core::default::Default)]
            struct #adapter;
            impl #aktor::dispatch::Export<#wire> for #adapter {
                const REMOTE: bool = #remote;
                const NAME: &'static str = NAME;

                type State = #state_type;
                type Input = #input_type;
                type Output = #output;
                type Role = #role;

                #aktor::__aktor_body! {
                    fn run<#future_lifetime>(
                        self,
                        #state_name: &#future_lifetime mut #state_type,
                        #input_name: #input_type
                    ) -> impl ::core::future::Future<Output = #output> + #future_lifetime {
                        #body(#state_name, #input_name)
                    }
                }
            }

            #[doc(hidden)]
            pub fn export() -> impl #aktor::dispatch::Export<#wire,
                State = #state_type,
                Input = #input_type,
                Output = #output,
                Role = #role
            > {
                #adapter
            }

            #aktor::__aktor_body! { #aktor::__aktor_testing! {
                /// A test response for this function in a preset.
                pub fn handler<#function_parameter>(mut #function_name: #function_parameter) -> #aktor::testing::Handler<#state_type, #role>
                where
                    #function_parameter: ::core::ops::AsyncFnMut(#(#input_types),*) -> #output + ::core::marker::Send + 'static,
                {
                    #aktor::testing::handler(
                        export(),
                        [
                            ::core::any::Any::type_id(&#body),
                            ::core::any::Any::type_id(&#latest_call),
                        ],
                        async move |#input_name: #input_type| {
                            let #unpack = #input_name;
                            #function_name(#(#bindings),*).await
                        },
                    )
                }
            } }

            #aktor::__aktor_register!(#adapter, #name, NAME, #wire, #remote, #serve);
        }
    };

    Ok(items)
}

// AktorData covers tuples up to twelve fields. A wider call nests its remaining arguments.
fn arguments<T: quote::ToTokens>(args: &[T]) -> TokenStream {
    if args.len() <= 12 {
        return quote!((#(#args,)*));
    }

    let (head, tail) = args.split_at(11);
    let tail = arguments(tail);
    quote!((#(#head,)* #tail))
}

fn serve_cfg(ty: &syn::Type) -> TokenStream {
    let segment = match ty {
        syn::Type::Paren(ty) => return serve_cfg(&ty.elem),
        syn::Type::Group(ty) => return serve_cfg(&ty.elem),
        syn::Type::Path(ty) => ty.path.segments.last(),
        _ => None,
    };

    if let Some(segment) = segment {
        let name = segment.ident.unraw().to_string();
        quote!(aktor_serve = #name)
    } else {
        quote!(any())
    }
}

/// Borrowed data can't take the concrete path. `'static` isn't borrowed from the call, so
/// `-> Result<u8, &'static str>` still gets its NAME, export and test handler.
struct References(bool);
impl<'ast> syn::visit::Visit<'ast> for References {
    fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
        if lifetime.ident != "static" {
            self.0 = true;
        }
    }

    fn visit_type_reference(&mut self, reference: &'ast syn::TypeReference) {
        match &reference.lifetime {
            Some(lifetime) if lifetime.ident == "static" => {
                syn::visit::visit_type(self, &reference.elem);
            }
            Some(_) | None => self.0 = true,
        }
    }

    // Argument and return references in a function signature are bound by that signature.
    fn visit_type_fn_ptr(&mut self, _: &'ast syn::TypeFnPtr) {}

    fn visit_bound_lifetimes(&mut self, _: &'ast syn::BoundLifetimes) {}

    fn visit_parenthesized_generic_arguments(
        &mut self,
        _: &'ast syn::ParenthesizedGenericArguments,
    ) {
    }

    fn visit_type_impl_trait(&mut self, _: &'ast syn::TypeImplTrait) {
        self.0 = true;
    }
}

// Use one type for the dispatch projection wherever a callback binds its own argument lifetimes.
struct Callbacks(bool);
impl<'ast> syn::visit::Visit<'ast> for Callbacks {
    fn visit_type_fn_ptr(&mut self, _: &'ast syn::TypeFnPtr) {
        self.0 = true;
    }

    fn visit_parenthesized_generic_arguments(
        &mut self,
        _: &'ast syn::ParenthesizedGenericArguments,
    ) {
        self.0 = true;
    }
}
