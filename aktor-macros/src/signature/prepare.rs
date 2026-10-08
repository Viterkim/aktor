use super::*;
use crate::names::Names;
use quote::quote;
use syn::{FnArg, GenericParam, Generics, ReturnType, Signature};

pub fn output(signature: &Signature) -> Type {
    let mut output = match &signature.output {
        ReturnType::Default => parse_quote!(()),
        ReturnType::Type(_, output) => (**output).clone(),
    };

    StaticOutput.visit_type_mut(&mut output);
    output
}

// The written capture list must not include our generated target.
pub fn dispatch_output(mut output: Type) -> Type {
    DispatchOutput.visit_type_mut(&mut output);
    output
}

pub fn normalize(signature: &mut Signature, names: &mut Names) {
    let mut borrows = Borrows {
        reserved: &mut names.reserved,
        lifetimes: Vec::new(),
    };

    for (index, argument) in signature.inputs.iter_mut().enumerate() {
        if let FnArg::Typed(argument) = argument {
            if index == 0 {
                if let Type::Reference(state) = argument.ty.as_mut() {
                    borrows.visit_type_mut(&mut state.elem);
                }
            } else {
                borrows.visit_type_mut(&mut argument.ty);
            }
        }
    }

    for lifetime in borrows.lifetimes.into_iter().rev() {
        signature
            .generics
            .params
            .insert(0, GenericParam::Lifetime(lifetime));
    }

    let mut inputs = Inputs {
        reserved: &mut names.reserved,
        parameters: Vec::new(),
    };

    for argument in &mut signature.inputs {
        if let FnArg::Typed(argument) = argument {
            inputs.visit_type_mut(&mut argument.ty);
        }
    }

    signature
        .generics
        .params
        .extend(inputs.parameters.into_iter().map(GenericParam::Type));
}

pub fn inputs(signature: &Signature) -> Type {
    let types: Vec<_> = signature
        .inputs
        .iter()
        .skip(1)
        .filter_map(|argument| match argument {
            FnArg::Typed(argument) => Some(&argument.ty),
            _ => None,
        })
        .collect();

    parse_quote!((#(#types,)*))
}

pub fn arguments(generics: &Generics) -> Vec<TokenStream> {
    generics
        .params
        .iter()
        .map(|parameter| match parameter {
            GenericParam::Lifetime(parameter) => {
                let lifetime = &parameter.lifetime;
                quote!(#lifetime)
            }
            GenericParam::Type(parameter) => {
                let name = &parameter.ident;
                quote!(#name)
            }
            GenericParam::Const(parameter) => {
                let name = &parameter.ident;
                quote!({ #name })
            }
        })
        .collect()
}
