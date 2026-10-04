use super::*;
use crate::input::Function;
use crate::names::Names;
use quote::quote;
use syn::{FnArg, GenericParam};

pub fn implementation(
    function: &Function,
    names: &mut Names,
    output: &Type,
) -> (TokenStream, TokenStream) {
    let signature = &function.signature;
    let mut implementation = signature.clone();
    implementation.ident = names.body.clone();
    implementation.output = parse_quote!(-> #output);

    let mut arguments: Vec<_> = implementation
        .generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Type(param) => {
                let name = &param.ident;
                Some(quote!(#name))
            }
            GenericParam::Const(param) => {
                let name = &param.ident;
                Some(quote!({ #name }))
            }
            _ => None,
        })
        .collect();

    normalize(&mut implementation, names);
    let added: Vec<_> = implementation
        .generics
        .type_params()
        .skip(signature.generics.type_params().count())
        .map(|param| {
            let ident = &param.ident;
            quote!(#ident)
        })
        .collect();
    arguments.extend(added.iter().map(|_| quote!(_)));

    let state_borrow = signature.inputs.first().and_then(|input| match input {
        FnArg::Typed(input) => match &*input.ty {
            Type::Reference(state) => state.lifetime.as_ref(),
            _ => None,
        },
        _ => None,
    });
    let captures: Vec<_> = implementation
        .generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Lifetime(param) => {
                let lifetime = &param.lifetime;
                (Some(lifetime) != state_borrow).then(|| quote!(#lifetime))
            }
            GenericParam::Type(param) => {
                let ident = &param.ident;
                Some(quote!(#ident))
            }
            GenericParam::Const(param) => {
                let ident = &param.ident;
                Some(quote!(#ident))
            }
        })
        .collect();
    Captures { captures, added }.visit_return_type_mut(&mut implementation.output);

    let name = &names.body;
    let invoke = if arguments.is_empty() {
        quote!(self::#name::#name)
    } else {
        quote!(self::#name::#name::<#(#arguments),*>)
    };

    let operation = &signature.ident;
    let attributes = &function.attributes;
    let body = &function.body;
    let implementation = quote! {
        impl #operation::#name {
            #(#attributes)*
            #[doc(hidden)]
            #[allow(clippy::extra_unused_lifetimes)]
            pub #implementation #body
        }
    };

    (implementation, invoke)
}
