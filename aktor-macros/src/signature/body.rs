use super::*;
use crate::input::Function;
use crate::names::Names;
use quote::quote;
use syn::{FnArg, GenericParam, visit::Visit};

pub fn implementation(
    function: &Function,
    names: &mut Names,
    output: &Type,
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
        quote!(self::#name::#name)
    } else {
        quote!(self::#name::#name::<#(#arguments),*>)
    };

    let operation = &signature.ident;
    let attributes = crate::input::attributes::implementation(&function.attributes)?;
    let body = &function.body;
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

pub fn direct(
    mut signature: syn::Signature,
    names: &Names,
    invoke: &TokenStream,
    attributes: &[syn::Attribute],
    scope: &mut super::scope::NestedModule,
) -> TokenStream {
    signature.ident = parse_quote!(direct);
    StaticOutput.visit_return_type_mut(&mut signature.output);

    let mut anonymous_inputs = HasImplTrait(false);
    for argument in &signature.inputs {
        anonymous_inputs.visit_fn_arg(argument);
    }
    if anonymous_inputs.0 {
        // The helper names these inputs. The public wrapper keeps `impl Trait` and returns a 'static value.
        DispatchOutput.visit_return_type_mut(&mut signature.output);
    }

    let mut bindings = Vec::new();
    let generated = std::iter::once(&names.state).chain(&names.arguments);

    for (argument, fallback) in signature.inputs.iter_mut().zip(generated) {
        if let FnArg::Typed(argument) = argument {
            let mut binding = fallback;

            if let syn::Pat::Ident(pattern) = &*argument.pat {
                let name = pattern.ident.to_string();
                if !scope.shadowed.contains(&name.trim_start_matches("r#")) {
                    binding = &pattern.ident;
                }
            }

            let binding = binding.clone();
            argument.pat = parse_quote!(#binding);
            bindings.push(binding);
        }
    }

    scope.visit_signature_mut(&mut signature);

    quote! {
        /// Run the function directly on borrowed state.
        #(#attributes)*
        #[allow(clippy::extra_unused_lifetimes)]
        pub #signature {
            #invoke(#(#bindings),*).await
        }
    }
}
