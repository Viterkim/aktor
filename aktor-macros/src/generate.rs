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
    let written_output = output.clone();

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

    let attributes = &function.attributes;
    let visibility = &function.visibility;
    let signature = &function.signature;
    let name = &signature.ident;
    let mut normal = signature.clone();
    normal.asyncness = Some(parse_quote!(async));
    normal.output = parse_quote!(-> #written_output);

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
    let mut scope = signature::scope::Child;
    scope.visit_signature_mut(&mut request);

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

    Ok(quote! {
        #(#attributes)*
        #[allow(clippy::extra_unused_lifetimes)]
        #visibility #normal {
            #name::request::<#(#generic_arguments),*>(#parameter, #(#bindings),*).await
        }

        #(#attributes)*
        #[doc(hidden)]
        #[allow(clippy::extra_unused_lifetimes)]
        #implementation #body

        #visibility mod #name {
            use super::*;

            #[track_caller]
            #[allow(clippy::extra_unused_lifetimes)]
            pub #request {
                <#target as #dispatch<#state_type, #input_type, #role>>::dispatch(
                    #parameter,
                    #aktor::operation::Operation {
                        name: ::core::module_path!(),
                        caller: ::core::panic::Location::caller(),
                    },
                    async move |#state_name: #state_reference, #input_name| {
                        let (#(#bindings,)*) = #input_name;
                        super::#invoke(#state_name, #(#bindings),*).await
                    },
                    (#(#bindings,)*),
                )
            }
        }
    })
}
