use crate::{names, path};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use std::collections::HashSet;
use syn::{
    Expr, Ident, Token,
    parse::{Parse, ParseStream},
};

pub struct Entry {
    pub name: Ident,
    pub setup: Expr,
}

pub struct Setups {
    pub entries: Vec<Entry>,
}
impl Parse for Setups {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut entries = Vec::new();
        let mut names = HashSet::new();

        while !input.is_empty() {
            let name: Ident = input.parse()?;

            if !names.insert(name.to_string().trim_start_matches("r#").to_owned()) {
                return Err(syn::Error::new_spanned(name, "duplicate actor field"));
            }

            let setup = if input.peek(Token![:]) {
                input.parse::<Token![:]>()?;
                input.parse()?
            } else {
                syn::parse_quote!(#name)
            };

            entries.push(Entry { name, setup });

            if input.is_empty() {
                break;
            }

            input.parse::<Token![,]>()?;
        }

        if entries.is_empty() {
            return Err(input.error("give at least one actor setup"));
        }

        Ok(Self { entries })
    }
}

fn pairs(mut items: Vec<TokenStream>) -> syn::Result<TokenStream> {
    if items.len() <= 1 {
        return items
            .pop()
            .ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "missing actor setup"));
    }

    let right = items.split_off(items.len() / 2);
    let left = pairs(items)?;
    let right = pairs(right)?;

    Ok(quote!((#left, #right)))
}

pub fn expand(input: Setups) -> syn::Result<TokenStream> {
    let aktor = path::aktor(None)?;
    let fields: Vec<_> = input.entries.iter().map(|entry| &entry.name).collect();
    let initializers: Vec<_> = input
        .entries
        .iter()
        .map(|entry| {
            let field = &entry.name;
            let setup = &entry.setup;

            if let Expr::Path(path) = setup
                && path.qself.is_none()
                && path.path.get_ident() == Some(field)
            {
                quote!(#field)
            } else {
                quote!(#field: #setup)
            }
        })
        .collect();

    let mut reserved = HashSet::new();

    for entry in &input.entries {
        names::collect(entry.name.to_token_stream(), &mut reserved);
        names::collect(entry.setup.to_token_stream(), &mut reserved);
    }

    let inputs = names::binding(&mut reserved, "__AktorInputs");
    let handles = names::binding(&mut reserved, "__AktorHandles");
    let context = names::binding(&mut reserved, "__aktor_context");
    let result = names::binding(&mut reserved, "__aktor_result");

    let types: Vec<_> = (0..fields.len())
        .map(|index| names::binding(&mut reserved, &format!("__AktorSetup{index}")))
        .collect();
    let bindings: Vec<_> = (0..fields.len())
        .map(|index| names::binding(&mut reserved, &format!("__aktor_handle{index}")))
        .collect();

    let first_type = types
        .first()
        .ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "missing actor setup"))?;
    let first_field = fields
        .first()
        .ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "missing actor setup"))?;
    let other_types = &types[1..];

    let nested_type = pairs(types.iter().map(|ty| quote!(#ty)).collect())?;
    let nested_setup = pairs(fields.iter().map(|field| quote!(self.#field)).collect())?;
    let nested_handles = pairs(bindings.iter().map(|binding| quote!(#binding)).collect())?;

    Ok(quote!({
        struct #inputs<#(#types),*> {
            #(#fields: #types),*
        }
        #[allow(dead_code)]
        struct #handles<#(#types),*> {
            #(pub #fields: #types),*
        }
        impl<#first_type: #aktor::setup::AktorStart,
            #(#other_types: #aktor::setup::AktorStart<Group = #first_type::Group>),*>
            #aktor::setup::AktorStart for #inputs<#(#types),*>
        {
            type Error = <#nested_type as #aktor::setup::AktorStart>::Error;
            type Group = #first_type::Group;
            type Handles = #handles<#(#types::Handles),*>;
            type Startup = #aktor::setup::AktorTupleStartup<
                <#nested_type as #aktor::setup::AktorStart>::Startup,
                Self::Handles,
                Self::Error,
            >;

            fn source_is_setup(#result: &Self::Error) -> bool {
                <#nested_type as #aktor::setup::AktorStart>::source_is_setup(#result)
            }

            fn begin(&self, #context: &mut Self::Group) -> ::core::result::Result<(), #aktor::AktorSetupError> {
                #aktor::setup::AktorStart::begin(&self.#first_field, #context)
            }

            fn start_in(self, #context: #aktor::setup::AktorStartContext<Self::Group>) -> Self::Startup {
                #aktor::setup::AktorTupleStartup::new(
                    #aktor::setup::AktorStart::start_in(#nested_setup, #context),
                    |#result| #result.map(|#nested_handles| #handles { #(#fields: #bindings),* }),
                )
            }
        }
        #inputs { #(#initializers),* }
    }))
}
