use super::{
    Shape, Style,
    bounds::{NormalizeSelf, RenameLifetime, add_bounds},
    parse::{check_serde_attrs, check_variant},
};
use crate::{names, path};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
};
use syn::{Data, DeriveInput, Type, ext::IdentExt, parse_quote, visit_mut::VisitMut};

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let mut overridden = None;
    let mut via_serde = false;

    for attribute in &input.attrs {
        if attribute.path().is_ident("aktor") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("crate") {
                    if overridden.is_some() {
                        return Err(meta.error("duplicate aktor(crate = path)"));
                    }

                    overridden = Some(meta.value()?.parse()?);
                    Ok(())
                } else if meta.path.is_ident("serde") {
                    if via_serde {
                        return Err(meta.error("duplicate aktor(serde)"));
                    }

                    via_serde = true;
                    Ok(())
                } else {
                    Err(meta.error("expected aktor(serde) or aktor(crate = path) on the type"))
                }
            })?;
        }
    }

    let aktor = path::aktor(overridden)?;
    let serde = quote!(#aktor::data::__serde);
    let mut reserved = HashSet::new();

    let tokens = input.to_token_stream();
    let macro_syntax = names::collect(tokens.clone(), &mut reserved);
    let serializer = names::binding(&mut reserved, "__AktorSerializer");
    let deserializer = names::binding(&mut reserved, "__AktorDeserializer");

    // Names produced inside a caller's macro are invisible here. Keep the impl bounds in
    // their original scope and avoid reusing a predictable parameter name in that scope.
    let (serializer_value, deserializer_value) = if macro_syntax {
        let mut hash = DefaultHasher::new();
        tokens.to_string().hash(&mut hash);
        let suffix = hash.finish();

        (
            names::binding(&mut reserved, &format!("__aktor_serializer_{suffix:016x}")),
            names::binding(
                &mut reserved,
                &format!("__aktor_deserializer_{suffix:016x}"),
            ),
        )
    } else {
        (
            names::binding(&mut reserved, "__aktor_serializer"),
            names::binding(&mut reserved, "__aktor_deserializer"),
        )
    };
    let local = names::binding(&mut reserved, "__aktor_wire");
    let name = &input.ident;
    let mut generics = input.generics.clone();
    let decode_lifetime_name = names::binding(&mut reserved, "__aktor_decode_lifetime");
    let decode_lifetime = syn::Lifetime::new(
        &format!("'{decode_lifetime_name}"),
        decode_lifetime_name.span(),
    );

    let implement = |generics: &syn::Generics,
                     helpers: TokenStream,
                     serialize: TokenStream,
                     deserialize: TokenStream| {
        let (implementation, arguments, bounds) = generics.split_for_impl();

        // A constant in the caller's module can turn a parameter into a constant pattern.
        // An opaque macro in the impl bounds still needs the caller's value names.
        let opaque_bounds =
            macro_syntax && names::collect(quote!(#implementation #bounds), &mut HashSet::new());
        let parameter_imports = (!opaque_bounds).then(|| {
            quote! {
                #[allow(unused_imports)]
                use ::core::mem::{drop as #serializer_value, drop as #deserializer_value};
            }
        });

        quote! {
            const _: () = {
                #helpers

                {
                    #parameter_imports

                    impl #implementation #aktor::AktorData for #name #arguments #bounds {
                        fn serialize_data<#serializer: #serde::Serializer>(
                            &self,
                            #serializer_value: #serializer,
                        ) -> ::core::result::Result<#serializer::Ok, #serializer::Error> {
                            #serialize
                        }

                        fn deserialize_data<#decode_lifetime, #deserializer: #serde::Deserializer<#decode_lifetime>>(
                            #deserializer_value: #deserializer,
                        ) -> ::core::result::Result<Self, #deserializer::Error> {
                            #deserialize
                        }
                    }
                }
            };
        }
    };

    if via_serde {
        check_serde_attrs(&input)?;
        generics.make_where_clause().predicates.push(parse_quote!(
            Self: #serde::Serialize + #serde::de::DeserializeOwned
        ));

        return Ok(implement(
            &generics,
            TokenStream::new(),
            quote!(#serde::Serialize::serialize(self, #serializer_value)),
            quote!(#serde::Deserialize::deserialize(#deserializer_value)),
        ));
    }

    let serde_path = syn::LitStr::new(&serde.to_string(), proc_macro2::Span::call_site());
    let reference = names::binding(&mut reserved, "__AktorWireRef");
    let owned = names::binding(&mut reserved, "__AktorWireOwned");
    let marker = names::binding(&mut reserved, "__aktor_marker");

    let mut shapes = Vec::new();
    let variants = match input.data {
        Data::Struct(structure) => {
            shapes.push(Shape::parse(structure.fields)?);
            None
        }
        Data::Enum(enumeration) => {
            if enumeration.variants.is_empty() {
                return Ok(implement(
                    &generics,
                    TokenStream::new(),
                    quote!(match *self {}),
                    quote!(::core::result::Result::Err(#serde::de::Error::custom("empty AktorData enum"))),
                ));
            }

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

    if let Some(variants) = &variants
        && shapes
            .iter()
            .all(|shape| matches!(shape.style, Style::Unit))
    {
        let wire_name = reference.to_string();
        let labels = variants.iter().map(|variant| variant.unraw().to_string());
        let indices = 0..variants.len() as u32;

        return Ok(implement(
            &generics,
            quote! {
                #[allow(non_camel_case_types)]
                #[derive(#serde::Deserialize)]
                #[serde(crate = #serde_path)]
                enum #owned { #(#variants,)* }
            },
            quote! {
                #[allow(unused_imports)]
                use ::core::mem::drop as #local;
                let #local = match self { #(Self::#variants => (#indices, #labels),)* };
                #serde::Serializer::serialize_unit_variant(#serializer_value, #wire_name, #local.0, #local.1)
            },
            quote! {
                ::core::result::Result::Ok(match <#owned as #serde::Deserialize>::deserialize(#deserializer_value)? {
                    #(#owned::#variants => Self::#variants,)*
                })
            },
        ));
    }

    add_bounds(&mut generics, name, &shapes, &aktor);

    let has_fields = shapes
        .iter()
        .flat_map(|shape| &shape.members)
        .any(|field| !field.skip);
    let (ref_field, owned_field, imports) = if has_fields && !macro_syntax {
        let reference = names::binding(&mut reserved, "__Ref");
        let owned = names::binding(&mut reserved, "__Owned");
        let imports = quote!(use #aktor::data::{Ref as #reference, Owned as #owned};);
        (quote!(#reference), quote!(#owned), imports)
    } else {
        // A type macro can name a type that we didn't see in the input.
        (
            quote!(#aktor::data::Ref),
            quote!(#aktor::data::Owned),
            TokenStream::new(),
        )
    };

    let (_, arguments, _) = generics.split_for_impl();
    let renamed_lifetime_name = names::binding(&mut reserved, "__aktor_type_lifetime");
    let renamed_lifetime = syn::Lifetime::new(
        &format!("'{renamed_lifetime_name}"),
        renamed_lifetime_name.span(),
    );
    let mut helper_generics = generics.clone();
    let original: Type = parse_quote!(#name #arguments);

    NormalizeSelf(&original).visit_generics_mut(&mut helper_generics);
    RenameLifetime(&renamed_lifetime).visit_generics_mut(&mut helper_generics);

    for shape in &mut shapes {
        for field in &mut shape.members {
            NormalizeSelf(&original).visit_type_mut(&mut field.ty);
            RenameLifetime(&renamed_lifetime).visit_type_mut(&mut field.ty);
        }
    }

    let (owned_impl, helper_arguments, owned_bounds) = helper_generics.split_for_impl();
    let (helpers, serialize, deserialize) = if let Some(variants) = variants {
        let lifetime_name = names::binding(&mut reserved, "__aktor_wire_lifetime");
        let lifetime = syn::Lifetime::new(&format!("'{lifetime_name}"), lifetime_name.span());

        let mut borrowed = helper_generics.clone();
        borrowed.params.insert(0, parse_quote!(#lifetime));
        let (borrowed_impl, _, borrowed_bounds) = borrowed.split_for_impl();

        let mut inferred = generics.clone();
        inferred.params.insert(0, parse_quote!('_));

        let (_, borrowed_args, _) = inferred.split_for_impl();

        let refs = shapes
            .iter()
            .map(|shape| shape.helper(&ref_field, Some(&lifetime)));
        let values = shapes.iter().map(|shape| shape.helper(&owned_field, None));
        let mut writes = Vec::new();
        let mut reads = Vec::new();

        let field_count = shapes
            .iter()
            .map(|shape| shape.members.len())
            .max()
            .unwrap_or(0);
        let bindings: Vec<_> = (1..=field_count)
            .map(|index| names::binding(&mut reserved, &format!("__aktor_field_{index}")))
            .collect();

        for (variant, shape) in variants.iter().zip(&shapes) {
            let pattern_values = shape
                .members
                .iter()
                .zip(&bindings)
                .map(|(field, binding)| {
                    if field.skip {
                        quote!(_)
                    } else {
                        quote!(#binding)
                    }
                })
                .collect();
            let pattern = shape.construct(quote!(Self::#variant), pattern_values, true);
            let wire_values = shape
                .members
                .iter()
                .zip(&bindings)
                .filter(|(field, _)| !field.skip)
                .map(|(_, binding)| quote!(#ref_field(#binding)))
                .collect();
            let wire = shape.construct(quote!(#reference::#variant), wire_values, false);
            writes.push(quote!(#pattern => #wire));

            let read_values = shape
                .members
                .iter()
                .zip(&bindings)
                .filter(|(field, _)| !field.skip)
                .map(|(_, binding)| quote!(#binding))
                .collect();
            let read_pattern = shape.construct(quote!(#owned::#variant), read_values, false);
            let restored = shape
                .members
                .iter()
                .zip(&bindings)
                .map(|(field, binding)| {
                    if field.skip {
                        quote!(::core::default::Default::default())
                    } else {
                        quote!(#binding.0)
                    }
                })
                .collect();
            let restored = shape.construct(quote!(Self::#variant), restored, true);
            reads.push(quote!(#read_pattern => #restored));
        }

        let helpers = quote! {
            #[allow(non_camel_case_types)]
            #[derive(#serde::Serialize)]
            #[serde(crate = #serde_path, bound = "")]
            enum #reference #borrowed_impl #borrowed_bounds {
                #(#variants #refs,)*
                #[serde(skip)]
                #marker(::core::marker::PhantomData<&#lifetime #name #helper_arguments>),
            }

            #[allow(non_camel_case_types)]
            #[derive(#serde::Deserialize)]
            #[serde(crate = #serde_path, bound = "")]
            enum #owned #owned_impl #owned_bounds {
                #(#variants #values,)*
                #[serde(skip)]
                #marker(::core::marker::PhantomData<fn() -> #name #helper_arguments>),
            }
        };

        let binding_imports = quote! {
            #[allow(unused_imports)]
            use ::core::mem::{drop as #local, #(drop as #bindings),*};
        };
        let serialize = quote! {
            #binding_imports
            let #local: #reference #borrowed_args = match self { #(#writes,)* };
            #serde::Serialize::serialize(&#local, #serializer_value)
        };

        let deserialize = quote! {
            #binding_imports
            let #local: #owned #arguments = #serde::Deserialize::deserialize(#deserializer_value)?;
            ::core::result::Result::Ok(match #local {
                #(#reads,)*
                #owned::#marker(_) => return ::core::result::Result::Err(#serde::de::Error::custom("invalid AktorData variant")),
            })
        };

        (helpers, serialize, deserialize)
    } else {
        let shape = &shapes[0];
        let active: Vec<_> = shape.members.iter().filter(|field| !field.skip).collect();
        let wire_fields: Vec<_> = active
            .iter()
            .enumerate()
            .map(|(index, field)| match shape.style {
                Style::Named => field.name.clone(),
                Style::Tuple | Style::Unit => syn::Member::Unnamed(index.into()),
            })
            .collect();
        let types: Vec<_> = active.iter().map(|field| &field.ty).collect();

        let helpers = match shape.style {
            Style::Named => quote! {
                #[derive(#serde::Deserialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #owned #owned_impl #owned_bounds {
                    #(#wire_fields: #owned_field<#types>,)*
                    #[serde(skip)]
                    #marker: ::core::marker::PhantomData<fn() -> #name #helper_arguments>,
                }
            },
            Style::Tuple | Style::Unit => quote! {
                #[derive(#serde::Deserialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #owned #owned_impl (
                    #(#owned_field<#types>,)*
                    #[serde(skip)]
                    ::core::marker::PhantomData<fn() -> #name #helper_arguments>
                ) #owned_bounds;
            },
        };

        let wire_name = reference.to_string();
        let count = active.len();
        let serialize = if active.is_empty() && matches!(shape.style, Style::Tuple | Style::Unit) {
            // The original tuple helper has only its PhantomData field here.
            quote!(#serde::Serializer::serialize_newtype_struct(
                #serializer_value, #wire_name, &::core::marker::PhantomData::<&Self>
            ))
        } else {
            let fields = names::binding(&mut reserved, "__AktorFields");
            let (method, interface) = match shape.style {
                Style::Named => (quote!(serialize_struct), quote!(SerializeStruct)),
                Style::Tuple | Style::Unit => {
                    (quote!(serialize_tuple_struct), quote!(SerializeTupleStruct))
                }
            };
            let writes = active.iter().map(|field| {
                let member = &field.name;

                match member {
                    syn::Member::Named(name) => {
                        let key = name.unraw().to_string();
                        quote!(#fields::serialize_field(&mut #local, #key, &#ref_field(&self.#member))?;)
                    }
                    syn::Member::Unnamed(_) => {
                        quote!(#fields::serialize_field(&mut #local, &#ref_field(&self.#member))?;)
                    }
                }
            });

            quote! {
                #[allow(unused_imports)]
                use ::core::mem::drop as #local;
                use #serde::ser::#interface as #fields;
                let mut #local = #serde::Serializer::#method(#serializer_value, #wire_name, #count)?;
                #(#writes)*
                #fields::end(#local)
            }
        };

        let mut active_index = 0;
        let restored = shape
            .members
            .iter()
            .map(|field| {
                if field.skip {
                    quote!(::core::default::Default::default())
                } else {
                    let wire_field = &wire_fields[active_index];
                    active_index += 1;
                    quote!(#local.#wire_field.0)
                }
            })
            .collect();
        let restored = shape.construct(quote!(Self), restored, true);
        let deserialize = quote! {
            #[allow(unused_imports)]
            use ::core::mem::drop as #local;
            let #local: #owned #arguments = #serde::Deserialize::deserialize(#deserializer_value)?;
            ::core::result::Result::Ok(#restored)
        };

        (helpers, serialize, deserialize)
    };

    Ok(implement(
        &generics,
        quote!(#imports #[allow(non_camel_case_types)] #helpers),
        serialize,
        deserialize,
    ))
}
