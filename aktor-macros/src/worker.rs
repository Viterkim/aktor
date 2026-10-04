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
        .actor
        .as_ref()
        .map_or_else(|| quote!(()), |role| quote!(#role));
    let mut names = names::Names::new(&function.signature, &function.body, quote!(#aktor #role));

    let state = input::parse::state(&function.signature.inputs[0])?;
    let state_type = &state.ty;
    let (implementation, invoke) =
        signature::implementation(&function.signature, &mut names, &output);
    let body = &function.body;

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
    let latest_inner = names::binding(&mut names.reserved, "__AktorLatestInner");
    let function_parameter = names::binding(&mut names.reserved, "__AktorFunction");
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

    function.signature.asyncness = None;
    function.signature.output = parse_quote!(-> #aktor::call::Call<#target, #input_type, <#target as #name::#dispatch_trait<#mode, #state_type, #input_type, #output, #role>>::Request, #name::#latest_factory<#state_type, #input_type, #output, #role>>);
    let signature = &function.signature;
    let visibility = &function.visibility;
    let attributes = &function.attributes;

    let mut scope = signature::scope::Child {
        shadowed: &[
            "NAME",
            "request",
            "export",
            "latest",
            "LatestSender",
            "Latest",
        ],
    };
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

    let dispatch = if state.mutable {
        quote!(#aktor::target::Write)
    } else {
        quote!(#aktor::target::Read)
    };
    let state_reference = if state.mutable {
        quote!(&mut #state_type)
    } else {
        quote!(&#state_type)
    };

    Ok(quote! {
        #(#attributes)*
        #[track_caller]
        #visibility #signature {
            #name::request(#parameter, #(#bindings),*)
        }

        #(#attributes)*
        #[doc(hidden)]
        #implementation #body

        #visibility mod #name {
            use super::*;

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
            pub struct #latest_factory<#latest_state, #latest_input, #latest_output, #latest_role> {
                pub marker: ::core::marker::PhantomData<fn() -> (#latest_state, #latest_input, #latest_output, #latest_role)>,
            }
            impl<#target> #aktor::latest::Factory<#target, #input_type> for #latest_factory<#state_type, #input_type, #output, #role>
            where
                #target: #aktor::latest::Session<#state_type, #input_type, #output, #role>,
            {
                type Sender = LatestSender<<#target as #aktor::latest::Session<#state_type, #input_type, #output, #role>>::Sender>;
                type Results = <#target as #aktor::latest::Session<#state_type, #input_type, #output, #role>>::Results;

                fn start(self, #parameter: #target, #input_name: #input_type, operation: #aktor::operation::Operation) -> (Self::Sender, Self::Results) {
                    let (#(#bindings,)*) = #input_name;
                    let (inner, results) = #aktor::latest::Session::session(
                        #parameter, operation,
                        async move |#state_name: &mut #state_type, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            super::#invoke(#state_name, #(#bindings),*).await
                        },
                    );
                    let sender = LatestSender { inner };
                    sender.send(#(#bindings),*);
                    (sender, results)
                }
            }

            #[track_caller]
            pub fn latest<#target>(#parameter: #target) -> (
                LatestSender<<#target as #aktor::latest::Session<#state_type, #input_type, #output, #role>>::Sender>,
                <#target as #aktor::latest::Session<#state_type, #input_type, #output, #role>>::Results
            )
            where
                #target: #aktor::latest::Session<#state_type, #input_type, #output, #role>,
            {
                let (inner, results) = #aktor::latest::Session::session(
                    #parameter,
                    #aktor::operation::Operation { name: NAME, caller: ::core::panic::Location::caller() },
                    async move |#state_name: &mut #state_type, #input_name| {
                        let (#(#bindings,)*) = #input_name;
                        super::#invoke(#state_name, #(#bindings),*).await
                    },
                );
                (LatestSender { inner }, results)
            }

            #[doc(hidden)]
            #[diagnostic::on_unimplemented(
                note = "Check the actor's state type and actor marker. Browser worker arguments and results must implement Serialize and DeserializeOwned."
            )]
            pub trait #dispatch_trait<
                #mode,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: ::core::marker::Send + 'static,
                #role_parameter
            > {
                type Request: ::core::future::Future<Output = #output_parameter>;

                fn request<#function_parameter>(
                    self,
                    input: #input_parameter,
                    operation: #aktor::operation::Operation,
                    function: #function_parameter
                ) -> Self::Request
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static;
            }

            impl<
                #target,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: ::core::marker::Send + 'static,
                #role_parameter
            > #dispatch_trait<
                #aktor::target::Native,
                #state_parameter,
                #input_parameter,
                #output_parameter,
                #role_parameter
            > for #target
            where
                #target: #dispatch<#state_parameter, #input_parameter, #role_parameter>
            {
                type Request = <#target as #dispatch<
                    #state_parameter,
                    #input_parameter,
                    #role_parameter
                >>::Output<#output_parameter>;

                fn request<#function_parameter>(
                    self,
                    input: #input_parameter,
                    operation: #aktor::operation::Operation,
                    function: #function_parameter
                ) -> Self::Request
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static
                {
                    <#target as #dispatch<#state_parameter, #input_parameter, #role_parameter>>::dispatch(
                        self,
                        operation,
                        function,
                        input
                    )
                }
            }
            impl<
                #target,
                #state_parameter: 'static,
                #input_parameter,
                #output_parameter: ::core::marker::Send + 'static,
                #role_parameter
            > #dispatch_trait<
                #aktor::target::Remote,
                #state_parameter,
                #input_parameter,
                #output_parameter,
                #role_parameter
            > for #target
            where
                #target: #aktor::target::Transport<
                    #state_parameter,
                    #input_parameter,
                    #output_parameter,
                    #role_parameter
                >
            {
                type Request = <#target as #aktor::target::Transport<
                    #state_parameter,
                    #input_parameter,
                    #output_parameter,
                    #role_parameter
                >>::Request;

                fn request<#function_parameter>(
                    self,
                    input: #input_parameter,
                    operation: #aktor::operation::Operation,
                    _: #function_parameter
                ) -> Self::Request
                where
                    #function_parameter: for<'s> ::core::ops::AsyncFnOnce(
                        #state_reference_generic,
                        #input_parameter
                    ) -> #output_parameter + ::core::marker::Send + 'static
                {
                    #aktor::target::Transport::request(self, operation, input)
                }
            }

            #[track_caller]
            pub fn request<
                #target: #dispatch_trait<#mode, #state_type, #input_type, #output, #role>,
                #mode
            >(#parameter: #target, #(#bindings: #input_types),*) -> #aktor::call::Call<#target, #input_type, #target::Request, #latest_factory<#state_type, #input_type, #output, #role>> {
                #aktor::call::Call::new(
                    #parameter, (#(#bindings,)*),
                    #aktor::operation::Operation { name: NAME, caller: ::core::panic::Location::caller() },
                    |#parameter, #input_name, operation| #parameter.request(
                        #input_name, operation,
                        async move |#state_name: #state_reference, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            super::#invoke(#state_name, #(#bindings),*).await
                        },
                    ),
                    #latest_factory { marker: ::core::marker::PhantomData },
                )
            }

            #[doc(hidden)]
            #[derive(Default)]
            struct #adapter;
            impl #aktor::target::Export for #adapter {
                type State = #state_type;
                type Input = #input_type;
                type Output = #output;
                type Role = #role;

                fn run(
                    self,
                    state: &mut #state_type,
                    input: #input_type
                ) -> #aktor::message::LocalFuture<'_, #output> {
                    #aktor::message::boxed(async move {
                        let (#(#bindings,)*) = input;
                        super::#invoke(state, #(#bindings),*).await
                    })
                }
            }

            pub fn export() -> impl #aktor::target::Export<
                State = #state_type,
                Input = #input_type,
                Output = #output,
                Role = #role
            > {
                #adapter
            }

            #aktor::__aktor_register!(#adapter, NAME);
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
