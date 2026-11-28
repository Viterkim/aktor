use super::{
    Member, Shape,
    bounds::add_bounds,
    parse::{check_variant, is_packed, options},
};
use crate::{names, path};
use proc_macro2::{Literal, TokenStream};
use quote::{ToTokens, quote, quote_spanned};
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
};
use syn::{Data, DeriveInput, Ident, parse_quote, spanned::Spanned};

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let options = options(&input.attrs)?;
    let aktor = path::aktor(options.krate)?;
    let data = quote!(#aktor::data);
    let name = &input.ident;
    let mut generics = input.generics.clone();

    let mut reserved = HashSet::new();
    let tokens = input.to_token_stream();
    let macro_syntax = names::collect(tokens.clone(), &mut reserved);

    // Names produced inside a caller's macro are invisible here. Keep the impl bounds in
    // their original scope and avoid reusing a predictable parameter name in that scope.
    let suffix = macro_syntax.then(|| {
        let mut hash = DefaultHasher::new();
        tokens.to_string().hash(&mut hash);
        format!("_{:016x}", hash.finish())
    });
    let suffix = suffix.unwrap_or_default();
    let writer = names::binding(&mut reserved, &format!("__aktor_writer{suffix}"));
    let reader = names::binding(&mut reserved, &format!("__aktor_reader{suffix}"));

    // The inference can't see through a type alias, so a written bound replaces it entirely.
    let bounded = options.bound.is_some();

    if let Some(bound) = options.bound {
        generics.make_where_clause().predicates.extend(bound);
    }

    let mut shapes = Vec::new();
    let variants = match input.data {
        Data::Struct(structure) => {
            shapes.push(Shape::parse(structure.fields)?);
            None
        }
        Data::Enum(enumeration) => {
            let variants = enumeration
                .variants
                .into_iter()
                .map(|variant| {
                    check_variant(&variant)?;
                    shapes.push(Shape::parse(variant.fields)?);
                    Ok(variant.ident)
                })
                .collect::<syn::Result<Vec<_>>>()?;

            Some(variants)
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                name,
                "AktorData supports structs and enums",
            ));
        }
    };

    if !bounded {
        add_bounds(&mut generics, name, &shapes, &aktor);
    }

    // A max_len field goes through the checked write and read, spanned at the field so a
    // field without a length gets AktorMaxLen's message right there.
    let write = |field: &Member, value: TokenStream| match &field.max_len {
        Some(max) => {
            let ty = &field.ty;
            quote_spanned!(field.ty.span()=> #data::write_max::<#ty>(#writer, #value, #max)?;)
        }
        None => quote!(#data::Writer::write(#writer, #value)?;),
    };
    let read = |field: &Member| match &field.max_len {
        Some(max) => {
            let ty = &field.ty;
            quote_spanned!(field.ty.span()=> #data::read_max::<#ty>(#reader, #max)?)
        }
        None => quote!(#data::Reader::read(#reader)?),
    };
    let restore = |shape: &Shape, prefix: TokenStream| {
        let values = shape
            .members
            .iter()
            .map(|field| match field.skip {
                true => quote!(::core::default::Default::default()),
                false => read(field),
            })
            .collect();

        shape.construct(prefix, values)
    };

    let (encode, decode) = match variants {
        None => {
            let shape = &shapes[0];
            let packed = is_packed(&input.attrs);
            let active: Vec<_> = shape.members.iter().filter(|field| !field.skip).collect();

            if packed && !generics.params.is_empty() {
                for field in &active {
                    let ty = &field.ty;
                    generics
                        .make_where_clause()
                        .predicates
                        .push(parse_quote!(#ty: ::core::marker::Copy));
                }
            }

            if packed && let Some(field) = active.iter().find(|field| field.max_len.is_some()) {
                return Err(syn::Error::new_spanned(
                    &field.ty,
                    "packed structs send Copy fields, there's no length for max_len to check",
                ));
            }

            let writes = active.iter().map(|field| {
                let member = &field.name;

                match packed {
                    true => {
                        quote_spanned!(field.ty.span()=> #data::write_packed(#writer, self.#member)?;)
                    }
                    false => write(field, quote!(&self.#member)),
                }
            });
            let restored = restore(shape, quote!(Self));

            (
                quote! {
                    #(#writes)*
                    ::core::result::Result::Ok(())
                },
                quote!(::core::result::Result::Ok(#restored)),
            )
        }
        Some(variants) if variants.is_empty() => (
            quote!(match *self {}),
            quote!(::core::result::Result::Err(#data::DataError::from(#data::DataReason::EmptyEnum))),
        ),
        Some(variants) => {
            let field_count = shapes
                .iter()
                .map(|shape| shape.members.len())
                .max()
                .unwrap_or(0);
            let bindings: Vec<Ident> = (1..=field_count)
                .map(|index| names::binding(&mut reserved, &format!("__aktor_field_{index}")))
                .collect();
            let mut writes = Vec::new();
            let mut reads = Vec::new();

            for (index, (variant, shape)) in variants.iter().zip(&shapes).enumerate() {
                let index = Literal::usize_unsuffixed(index);
                let pattern = shape
                    .members
                    .iter()
                    .zip(&bindings)
                    .map(|(field, binding)| match field.skip {
                        true => quote!(_),
                        false => quote!(#binding),
                    })
                    .collect();
                let pattern = shape.construct(quote!(Self::#variant), pattern);
                let fields = shape
                    .members
                    .iter()
                    .zip(&bindings)
                    .filter(|(field, _)| !field.skip)
                    .map(|(field, binding)| write(field, quote!(#binding)));

                writes.push(quote! {
                    #pattern => {
                        #data::Writer::length(#writer, #index);
                        #(#fields)*
                    }
                });

                let restored = restore(shape, quote!(Self::#variant));
                reads.push(quote!(#index => #restored));
            }

            (
                quote! {
                    #[allow(unused_imports)]
                    use ::core::mem::{#(drop as #bindings),*};

                    match self {
                        #(#writes)*
                    }

                    ::core::result::Result::Ok(())
                },
                quote! {
                    ::core::result::Result::Ok(match #data::Reader::length(#reader)? {
                        #(#reads,)*
                        tag => {
                            return ::core::result::Result::Err(
                                #data::DataError::from(#data::DataReason::Variant(tag)),
                            );
                        }
                    })
                },
            )
        }
    };

    let (implementation, arguments, bounds) = generics.split_for_impl();
    // A constant in the caller's module can turn a parameter into a constant pattern.
    // An opaque macro in the impl bounds still needs the caller's value names.
    let opaque_bounds =
        macro_syntax && names::collect(quote!(#implementation #bounds), &mut HashSet::new());
    let parameter_imports = (!opaque_bounds).then(|| {
        quote! {
            #[allow(unused_imports)]
            use ::core::mem::{drop as #writer, drop as #reader};
        }
    });

    Ok(quote! {
        const _: () = {
            #parameter_imports

            impl #implementation #aktor::AktorData for #name #arguments #bounds {
                #[inline]
                fn encode_data(
                    &self,
                    #writer: &mut #data::Writer,
                ) -> ::core::result::Result<(), #data::DataError> {
                    #encode
                }

                #[inline]
                fn decode_data(
                    #reader: &mut #data::Reader<'_>,
                ) -> ::core::result::Result<Self, #data::DataError> {
                    #decode
                }
            }
        };
    })
}
