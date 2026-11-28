use super::*;
use crate::input::Function;
use crate::names::Names;
use quote::quote;
use syn::{FnArg, GenericParam};

pub fn implementation(
    function: &Function,
    names: &mut Names,
    output: &Type,
    aktor: &TokenStream,
) -> syn::Result<(TokenStream, TokenStream)> {
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
        quote!(#name::#name)
    } else {
        quote!(#name::#name::<#(#arguments),*>)
    };

    let operation = &signature.ident;
    let attributes = crate::input::attributes::implementation(&function.attributes)?;
    let body = &function.body;
    let mut declared = output;
    loop {
        match declared {
            Type::Group(group) => declared = &group.elem,
            Type::Paren(paren) => declared = &paren.elem,
            _ => break,
        }
    }

    let body = if let Type::Path(_) = declared {
        let statements = &body.stmts;
        let mut hint = output.clone();
        InferredOutput.visit_type_mut(&mut hint);

        let marker = crate::names::binding(&mut names.reserved, "__aktor_result_type");
        let result = crate::names::binding(&mut names.reserved, "__aktor_result");
        quote!({
            use #aktor::operation::CallResult as _;
            let #marker = ::core::marker::PhantomData::<fn(_) -> #hint>;
            #[allow(unreachable_code)]
            let #result = (async move { (&&#marker).body({ #(#statements)* }) }).await;
            (&#marker).finish(#result)
        })
    } else {
        quote!(#body)
    };
    let implementation = quote! {
        impl #operation::#name {
            #(#attributes)*
            #[doc(hidden)]
            #[allow(clippy::extra_unused_lifetimes)]
            pub #implementation #body
        }
    };

    Ok((implementation, invoke))
}

struct InferredOutput;

impl VisitMut for InferredOutput {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if let Type::ImplTrait(_) = ty {
            *ty = parse_quote!(_);
        } else {
            visit_mut::visit_type_mut(self, ty);
        }
    }
}
