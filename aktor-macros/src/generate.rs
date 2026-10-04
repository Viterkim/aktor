use crate::{input, names, path, signature};
use proc_macro2::TokenStream;
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{FnArg, parse_quote};

pub fn expand(options: input::Options, mut function: input::Function) -> syn::Result<TokenStream> {
    if crate::worker::supports(&function)? {
        return crate::worker::expand(options, function);
    }

    let aktor = path::aktor(options.crate_path)?;
    let role = options
        .actor
        .map_or_else(|| quote!(()), |role| quote!(#role));
    let mut names = names::Names::new(&function.signature, &function.body, quote!(#aktor #role));
    let output = signature::output(&function.signature);
    let (implementation, invoke) =
        signature::implementation(&function.signature, &mut names, &output);
    let output = signature::dispatch_output(output);
    let mut opaque_output = OpaqueOutput(false);
    syn::visit::Visit::visit_type(&mut opaque_output, &output);
    let latest_factory = names::binding(&mut names.reserved, "__AktorLatestFactory");
    let latest_state = names::binding(&mut names.reserved, "__AktorLatestState");
    let latest_input = names::binding(&mut names.reserved, "__AktorLatestInput");
    let latest_output = names::binding(&mut names.reserved, "__AktorLatestOutput");
    let latest_role = names::binding(&mut names.reserved, "__AktorLatestRole");
    let latest_inner = names::binding(&mut names.reserved, "__AktorLatestInner");
    signature::normalize(&mut function.signature, &mut names);

    let state = input::parse::state(&function.signature.inputs[0])?;
    let state_type = &state.ty;
    let dispatch = if state.mutable {
        quote!(#aktor::target::Write)
    } else {
        quote!(#aktor::target::Read)
    };

    let input_type = signature::inputs(&function.signature);
    let target = &names.target;
    let state_name = &names.state;
    let input_name = &names.input;
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
    let body = &function.body;

    function.signature.asyncness = None;
    function
        .signature
        .generics
        .params
        .push(parse_quote!(#target: #dispatch<#state_type, #input_type, #role>));
    function
        .signature
        .generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#state_type: 'static));
    function.signature.output =
        parse_quote!(-> <#target as #dispatch<#state_type, #input_type, #role>>::Output<#output>);

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

    let factory_arguments: Vec<_> = function
        .signature
        .generics
        .params
        .iter()
        .rev()
        .skip(1)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|parameter| match parameter {
            syn::GenericParam::Lifetime(parameter) => {
                let lifetime = &parameter.lifetime;
                quote!(#lifetime)
            }
            syn::GenericParam::Type(parameter) => {
                let name = &parameter.ident;
                quote!(#name)
            }
            syn::GenericParam::Const(parameter) => {
                let name = &parameter.ident;
                quote!(#name)
            }
        })
        .collect();
    let raw_output = function.signature.output.clone();
    if !opaque_output.0 {
        let name = &function.signature.ident;
        let syn::ReturnType::Type(_, raw) = &raw_output else {
            return Err(syn::Error::new_spanned(
                &function.signature,
                "missing actor output",
            ));
        };
        function.signature.output = parse_quote!(-> #aktor::call::Call<#target, #input_type, #raw, #name::#latest_factory<#(#factory_arguments,)* #state_type, #input_type, #output, #role>>);
    }

    let attributes = &function.attributes;
    let visibility = &function.visibility;
    let signature = &function.signature;
    let name = &signature.ident;
    let generic_arguments: Vec<_> = signature
        .generics
        .params
        .iter()
        .filter_map(|parameter| match parameter {
            syn::GenericParam::Type(parameter) => {
                let name = &parameter.ident;
                Some(quote!(#name))
            }
            syn::GenericParam::Const(parameter) => {
                let name = &parameter.ident;
                Some(quote!(#name))
            }
            _ => None,
        })
        .collect();

    let mut request = signature.clone();
    request.ident = parse_quote!(request);
    let mut scope = signature::scope::Child {
        shadowed: &["request", "latest", "LatestSender"],
    };
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
        quote!(#aktor::target::Write)
    } else {
        quote!(#aktor::target::Read)
    };
    let state_reference = if state.mutable {
        quote!(&mut #state_type)
    } else {
        quote!(&#state_type)
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
        let mut user_generics = request.generics.clone();
        user_generics.params.pop();
        let arguments: Vec<_> = user_generics
            .params
            .iter()
            .map(|parameter| match parameter {
                syn::GenericParam::Lifetime(parameter) => {
                    let lifetime = &parameter.lifetime;
                    quote!(#lifetime)
                }
                syn::GenericParam::Type(parameter) => {
                    let name = &parameter.ident;
                    quote!(#name)
                }
                syn::GenericParam::Const(parameter) => {
                    let name = &parameter.ident;
                    quote!(#name)
                }
            })
            .collect();
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
        let (sender_impl, sender_type, sender_where) = sender_generics.split_for_impl();
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
        latest_signature.generics = user_generics;
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
        predicates.push(parse_quote!(#target: #aktor::latest::Session<#state_type, #input_type, #session_output, #role>));
        predicates.push(parse_quote!(#input_type: 'static));
        predicates.push(parse_quote!(#session_output: ::core::marker::Send + 'static));
        let mut factory_generics = sender_generics.clone();
        factory_generics.params.pop();
        for name in [&latest_state, &latest_input, &latest_output, &latest_role] {
            factory_generics.params.push(parse_quote!(#name));
        }
        let (factory_impl, _, factory_where) = factory_generics.split_for_impl();
        let (latest_impl, _, latest_where) = latest_signature.generics.split_for_impl();
        latest_signature.output = parse_quote!(-> (
            LatestSender<#(#arguments,)* <#target as #aktor::latest::Session<#state_type, #input_type, #session_output, #role>>::Sender>,
            <#target as #aktor::latest::Session<#state_type, #input_type, #session_output, #role>>::Results
        ));

        quote! {
            #[doc(hidden)]
            pub struct #latest_factory #factory_impl #factory_where {
                pub marker: ::core::marker::PhantomData<fn(#(#markers),*) -> (#latest_state, #latest_input, #latest_output, #latest_role)>,
            }
            impl #latest_impl #aktor::latest::Factory<#target, #input_type> for #latest_factory<#(#arguments,)* #state_type, #input_type, #session_output, #role> #latest_where {
                type Sender = LatestSender<#(#arguments,)* <#target as #aktor::latest::Session<#state_type, #input_type, #session_output, #role>>::Sender>;
                type Results = <#target as #aktor::latest::Session<#state_type, #input_type, #session_output, #role>>::Results;

                fn start(self, #parameter: #target, #input_name: #input_type, operation: #aktor::operation::Operation) -> (Self::Sender, Self::Results) {
                    let (#(#bindings,)*) = #input_name;
                    let (inner, results) = #aktor::latest::Session::session(
                        #parameter, operation,
                        async move |#state_name: &mut #state_type, #input_name| {
                            let (#(#bindings,)*) = #input_name;
                            super::#invoke(#state_name, #(#bindings),*).await
                        },
                    );
                    let sender = LatestSender { inner, marker: ::core::marker::PhantomData };
                    sender.send(#(#bindings),*);
                    (sender, results)
                }
            }

            pub struct LatestSender #sender_impl #sender_where {
                pub inner: #latest_inner,
                marker: ::core::marker::PhantomData<fn(#(#markers),*)>,
            }
            impl #send_impl LatestSender #sender_type #send_where {
                pub fn send(&self, #(#bindings: #input_types),*) {
                    #aktor::latest::SendLatest::send(&self.inner, (#(#bindings,)*));
                }
            }
            impl #clone_impl ::core::clone::Clone for LatestSender #sender_type #clone_where {
                fn clone(&self) -> Self {
                    Self { inner: self.inner.clone(), marker: ::core::marker::PhantomData }
                }
            }

            #[track_caller]
            pub #latest_signature {
                let (inner, results) = #aktor::latest::Session::session(
                    #parameter,
                    #aktor::operation::Operation { name: ::core::module_path!(), caller: ::core::panic::Location::caller() },
                    async move |#state_name: &mut #state_type, #input_name| {
                        let (#(#bindings,)*) = #input_name;
                        super::#invoke(#state_name, #(#bindings),*).await
                    },
                );
                (LatestSender { inner, marker: ::core::marker::PhantomData }, results)
            }
        }
    };

    let dispatch_body = quote! {
        <#target as #dispatch<#state_type, #input_type, #role>>::dispatch(
            #parameter, operation,
            async move |#state_name: #state_reference, #input_name| {
                let (#(#bindings,)*) = #input_name;
                super::#invoke(#state_name, #(#bindings),*).await
            },
            #input_name,
        )
    };
    let operation = quote!(#aktor::operation::Operation {
        name: ::core::module_path!(), caller: ::core::panic::Location::caller(),
    });
    let request_body = if opaque_output.0 {
        quote! { let operation = #operation; let #input_name = (#(#bindings,)*); #dispatch_body }
    } else {
        quote! {
            #aktor::call::Call::new(
                #parameter, (#(#bindings,)*), #operation,
                |#parameter, #input_name, operation| { #dispatch_body },
                #latest_factory { marker: ::core::marker::PhantomData },
            )
        }
    };

    Ok(quote! {
        #(#attributes)*
        #[allow(clippy::extra_unused_lifetimes)]
        #[track_caller]
        #visibility #signature {
            #name::request::<#(#generic_arguments),*>(#parameter, #(#bindings),*)
        }

        #(#attributes)*
        #[doc(hidden)]
        #[allow(clippy::extra_unused_lifetimes)]
        #implementation #body

        #visibility mod #name {
            use super::*;

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
