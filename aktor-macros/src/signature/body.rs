use super::*;
use crate::names::Names;
use quote::quote;
use syn::{FnArg, GenericParam, Signature};

pub fn implementation(
    signature: &Signature,
    names: &mut Names,
    output: &Type,
) -> (Signature, TokenStream) {
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
                Some(quote!(#name))
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

    let captures: Vec<_> = implementation
        .generics
        .params
        .iter()
        .map(|param| match param {
            GenericParam::Lifetime(param) => {
                let lifetime = &param.lifetime;
                quote!(#lifetime)
            }
            GenericParam::Type(param) => {
                let ident = &param.ident;
                quote!(#ident)
            }
            GenericParam::Const(param) => {
                let ident = &param.ident;
                quote!(#ident)
            }
        })
        .collect();
    Captures { captures, added }.visit_return_type_mut(&mut implementation.output);

    if let Some(FnArg::Typed(state)) = implementation.inputs.first_mut()
        && let Type::Reference(state) = state.ty.as_mut()
    {
        state.lifetime = None;
    }

    let name = &names.body;
    let invoke = if arguments.is_empty() {
        quote!(#name)
    } else {
        quote!(#name::<#(#arguments),*>)
    };

    (implementation, invoke)
}
