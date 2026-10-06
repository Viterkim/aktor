use crate::{names, path};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use std::collections::HashSet;
use syn::visit_mut::VisitMut;
use syn::{
    Data, DeriveInput, Field, Fields, GenericParam, Generics, Ident, Meta, Token, Type,
    parse_quote, punctuated::Punctuated, visit::Visit,
};

#[derive(Clone)]
struct Member {
    name: syn::Member,
    ty: Type,
    skip: bool,
}
#[derive(Clone)]
struct Shape {
    fields: Fields,
    members: Vec<Member>,
}
impl Shape {
    fn parse(fields: Fields) -> syn::Result<Self> {
        let members = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok(Member {
                    name: field
                        .ident
                        .clone()
                        .map_or_else(|| syn::Member::Unnamed(index.into()), syn::Member::Named),
                    ty: field.ty.clone(),
                    skip: skipped(field)?,
                })
            })
            .collect::<syn::Result<Vec<_>>>()?;
        Ok(Self { fields, members })
    }
    fn helper(&self, aktor: &TokenStream, lifetime: Option<&syn::Lifetime>) -> TokenStream {
        let fields = self
            .members
            .iter()
            .filter(|field| !field.skip)
            .map(|field| {
                let ty = &field.ty;
                let ty = if let Some(lifetime) = lifetime {
                    quote!(#aktor::data::Ref<#lifetime, #ty>)
                } else {
                    quote!(#aktor::data::Owned<#ty>)
                };
                let name = &field.name;
                match &self.fields {
                    Fields::Named(_) => quote!(#name: #ty),
                    _ => ty,
                }
            });
        match &self.fields {
            Fields::Named(_) => quote!({ #(#fields,)* }),
            Fields::Unnamed(_) => quote!((#(#fields,)*)),
            Fields::Unit => quote!(),
        }
    }
    fn construct(
        &self,
        prefix: TokenStream,
        values: Vec<TokenStream>,
        include_skipped: bool,
    ) -> TokenStream {
        let members = self
            .members
            .iter()
            .filter(|field| include_skipped || !field.skip);
        match &self.fields {
            Fields::Named(_) => {
                let fields = members.zip(values).map(|(member, value)| {
                    let name = &member.name;
                    quote!(#name: #value)
                });
                quote!(#prefix { #(#fields,)* })
            }
            Fields::Unnamed(_) => quote!(#prefix(#(#values,)*)),
            Fields::Unit => prefix,
        }
    }
}

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let mut overridden = None;
    for attribute in &input.attrs {
        if attribute.path().is_ident("aktor") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("crate") {
                    overridden = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("expected aktor(crate = path) on the type"))
                }
            })?;
        }
    }
    let aktor = path::aktor(overridden)?;
    let serde = quote!(#aktor::data::__serde);
    let serde_path = syn::LitStr::new(&serde.to_string(), proc_macro2::Span::call_site());
    let mut reserved = HashSet::new();
    names::collect(input.to_token_stream(), &mut reserved);
    let reference = names::binding(&mut reserved, "__AktorWireRef");
    let owned = names::binding(&mut reserved, "__AktorWireOwned");
    let marker = names::binding(&mut reserved, "__aktor_marker");
    let local = names::binding(&mut reserved, "__aktor_wire");
    let serializer = names::binding(&mut reserved, "__AktorSerializer");
    let deserializer = names::binding(&mut reserved, "__AktorDeserializer");
    let lifetime_name = names::binding(&mut reserved, "__aktor_wire_lifetime");
    let lifetime = syn::Lifetime::new(&format!("'{lifetime_name}"), lifetime_name.span());
    let name = &input.ident;
    let mut generics = input.generics.clone();
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
    add_bounds(&mut generics, name, &shapes, &aktor);
    let (implementation, arguments, bounds) = generics.split_for_impl();
    let decode_lifetime_name = names::binding(&mut reserved, "__aktor_decode_lifetime");
    let decode_lifetime = syn::Lifetime::new(
        &format!("'{decode_lifetime_name}"),
        decode_lifetime_name.span(),
    );
    let renamed_lifetime_name = names::binding(&mut reserved, "__aktor_type_lifetime");
    let renamed_lifetime = syn::Lifetime::new(
        &format!("'{renamed_lifetime_name}"),
        renamed_lifetime_name.span(),
    );
    let mut helper_generics = generics.clone();
    let mut helper_shapes = shapes.clone();
    let original: Type = parse_quote!(#name #arguments);
    NormalizeSelf(&original).visit_generics_mut(&mut helper_generics);
    RenameLifetime(&renamed_lifetime).visit_generics_mut(&mut helper_generics);
    for shape in &mut helper_shapes {
        for field in &mut shape.members {
            NormalizeSelf(&original).visit_type_mut(&mut field.ty);
            RenameLifetime(&renamed_lifetime).visit_type_mut(&mut field.ty);
        }
    }
    let (owned_impl, helper_arguments, owned_bounds) = helper_generics.split_for_impl();
    let mut borrowed = helper_generics.clone();
    borrowed.params.insert(0, parse_quote!(#lifetime));
    let (borrowed_impl, _, borrowed_bounds) = borrowed.split_for_impl();
    let mut inferred = generics.clone();
    inferred.params.insert(0, parse_quote!(#lifetime));
    if let Some(GenericParam::Lifetime(parameter)) = inferred.params.first_mut() {
        parameter.lifetime = syn::Lifetime::new("'_", proc_macro2::Span::call_site());
    }
    let (_, borrowed_args, _) = inferred.split_for_impl();

    let (helpers, serialize, deserialize) = if let Some(variants) = variants {
        let refs = helper_shapes
            .iter()
            .map(|shape| shape.helper(&aktor, Some(&lifetime)));
        let values = helper_shapes.iter().map(|shape| shape.helper(&aktor, None));
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        for (variant, shape) in variants.iter().zip(&shapes) {
            let bindings: Vec<_> = shape
                .members
                .iter()
                .map(|_| names::binding(&mut reserved, "__aktor_field"))
                .collect();
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
                .map(|(_, binding)| quote!(#aktor::data::Ref(#binding)))
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
        let serialize = quote! {
            let #local: #reference #borrowed_args = match self { #(#writes,)* };
            #serde::Serialize::serialize(&#local, serializer)
        };
        let deserialize = quote! {
            let #local: #owned #arguments = #serde::Deserialize::deserialize(deserializer)?;
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
            .map(|(index, field)| match &shape.fields {
                Fields::Named(_) => field.name.clone(),
                _ => syn::Member::Unnamed(index.into()),
            })
            .collect();
        let types: Vec<_> = helper_shapes[0]
            .members
            .iter()
            .filter(|field| !field.skip)
            .map(|field| &field.ty)
            .collect();
        let helpers = match &shape.fields {
            Fields::Named(_) => quote! {
                #[derive(#serde::Serialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #reference #borrowed_impl #borrowed_bounds {
                    #(#wire_fields: #aktor::data::Ref<#lifetime, #types>,)*
                    #[serde(skip)]
                    #marker: ::core::marker::PhantomData<&#lifetime #name #helper_arguments>,
                }
                #[derive(#serde::Deserialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #owned #owned_impl #owned_bounds {
                    #(#wire_fields: #aktor::data::Owned<#types>,)*
                    #[serde(skip)]
                    #marker: ::core::marker::PhantomData<fn() -> #name #helper_arguments>,
                }
            },
            _ => quote! {
                #[derive(#serde::Serialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #reference #borrowed_impl (#(#aktor::data::Ref<#lifetime, #types>,)* #[serde(skip)] ::core::marker::PhantomData<&#lifetime #name #helper_arguments>) #borrowed_bounds;
                #[derive(#serde::Deserialize)]
                #[serde(crate = #serde_path, bound = "")]
                struct #owned #owned_impl (#(#aktor::data::Owned<#types>,)* #[serde(skip)] ::core::marker::PhantomData<fn() -> #name #helper_arguments>) #owned_bounds;
            },
        };
        let serialize = match &shape.fields {
            Fields::Named(_) => {
                let fields = active.iter().map(|field| {
                    let field = &field.name;
                    quote!(#field: #aktor::data::Ref(&self.#field))
                });
                quote!(let #local = #reference { #(#fields,)* #marker: ::core::marker::PhantomData::<&Self> };)
            }
            _ => {
                let fields = active.iter().map(|field| {
                    let field = &field.name;
                    quote!(#aktor::data::Ref(&self.#field))
                });
                quote!(let #local = #reference(#(#fields,)* ::core::marker::PhantomData::<&Self>);)
            }
        };
        let serialize = quote! { #serialize #serde::Serialize::serialize(&#local, serializer) };
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
            let #local: #owned #arguments = #serde::Deserialize::deserialize(deserializer)?;
            ::core::result::Result::Ok(#restored)
        };
        (helpers, serialize, deserialize)
    };
    Ok(quote! {
        const _: () = {
            #[allow(non_camel_case_types)]
            #helpers
            impl #implementation #aktor::AktorData for #name #arguments #bounds {
                fn serialize_data<#serializer: #serde::Serializer>(&self, serializer: #serializer) -> ::core::result::Result<#serializer::Ok, #serializer::Error> {
                    #serialize
                }
                fn deserialize_data<#decode_lifetime, #deserializer: #serde::Deserializer<#decode_lifetime>>(deserializer: #deserializer) -> ::core::result::Result<Self, #deserializer::Error> {
                    #deserialize
                }
            }
        };
    })
}

fn skipped(field: &Field) -> syn::Result<bool> {
    let mut skip = false;
    let mut hidden = None;
    for attribute in &field.attrs {
        if attribute.path().is_ident("aktor") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    if skip {
                        return Err(meta.error("duplicate aktor(skip)"));
                    }
                    skip = true;
                    Ok(())
                } else {
                    Err(meta.error("expected aktor(skip)"))
                }
            })?;
        }
        if attribute.path().is_ident("serde") {
            let options =
                attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for option in options {
                if option.path().is_ident("skip")
                    || option.path().is_ident("skip_serializing")
                    || option.path().is_ident("skip_serializing_if")
                {
                    hidden = Some(option);
                }
            }
        }
    }
    if !skip && let Some(option) = hidden {
        return Err(syn::Error::new_spanned(
            option,
            "this field is hidden by Serde but would be sent by AktorData; add #[aktor(skip)] to omit it from worker transport",
        ));
    }
    Ok(skip)
}

struct Mentions<'a> {
    names: &'a HashSet<String>,
    found: bool,
}
impl<'ast> Visit<'ast> for Mentions<'_> {
    fn visit_ident(&mut self, ident: &'ast Ident) {
        if self.names.contains(&ident.to_string()) {
            self.found = true;
        }
    }
}
fn add_bounds(generics: &mut Generics, name: &Ident, shapes: &[Shape], aktor: &TokenStream) {
    let parameters: HashSet<_> = generics
        .params
        .iter()
        .filter_map(|parameter| match parameter {
            GenericParam::Type(ty) => Some(ty.ident.to_string()),
            _ => None,
        })
        .collect();
    let recursive = HashSet::from([name.to_string(), "Self".to_owned()]);
    let mut recursive_parameters = HashSet::new();
    for field in shapes.iter().flat_map(|shape| &shape.members) {
        let ty = &field.ty;
        if field.skip {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#ty: ::core::default::Default));
            continue;
        }
        let mut uses_parameter = Mentions {
            names: &parameters,
            found: false,
        };
        uses_parameter.visit_type(ty);
        if !uses_parameter.found {
            continue;
        }
        let mut uses_self = Mentions {
            names: &recursive,
            found: false,
        };
        uses_self.visit_type(ty);
        if uses_self.found {
            for parameter in &parameters {
                let mut mentioned = Mentions {
                    names: &HashSet::from([parameter.clone()]),
                    found: false,
                };
                mentioned.visit_type(ty);
                if mentioned.found {
                    recursive_parameters.insert(parameter.clone());
                }
            }
        } else {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#ty: #aktor::AktorData));
        }
    }
    for parameter in recursive_parameters {
        let parameter = Ident::new(&parameter, proc_macro2::Span::call_site());
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#parameter: #aktor::AktorData));
    }
}

struct NormalizeSelf<'a>(&'a Type);
impl VisitMut for NormalizeSelf<'_> {
    fn visit_type_path_mut(&mut self, ty: &mut syn::TypePath) {
        syn::visit_mut::visit_type_path_mut(self, ty);

        if ty.qself.is_none()
            && ty
                .path
                .segments
                .first()
                .is_some_and(|part| part.ident == "Self")
        {
            let original = self.0;

            if ty.path.segments.len() == 1 {
                *ty = parse_quote!(#original);
            } else {
                let rest = syn::Path {
                    leading_colon: None,
                    segments: ty.path.segments.iter().skip(1).cloned().collect(),
                };
                *ty = parse_quote!(<#original>::#rest);
            }
        }
    }

    fn visit_expr_path_mut(&mut self, expr: &mut syn::ExprPath) {
        syn::visit_mut::visit_expr_path_mut(self, expr);

        if expr.qself.is_none()
            && expr
                .path
                .segments
                .first()
                .is_some_and(|part| part.ident == "Self")
            && expr.path.segments.len() > 1
        {
            let original = self.0;
            let rest = syn::Path {
                leading_colon: None,
                segments: expr.path.segments.iter().skip(1).cloned().collect(),
            };
            *expr = parse_quote!(<#original>::#rest);
        }
    }
}

struct RenameLifetime<'a>(&'a syn::Lifetime);
impl VisitMut for RenameLifetime<'_> {
    fn visit_lifetime_mut(&mut self, lifetime: &mut syn::Lifetime) {
        if lifetime.ident == "de" {
            *lifetime = self.0.clone();
        }
    }
}

fn check_variant(variant: &syn::Variant) -> syn::Result<()> {
    for attribute in &variant.attrs {
        if attribute.path().is_ident("aktor") {
            return Err(syn::Error::new_spanned(
                attribute,
                "AktorData does not support skipped variants; aktor(skip) applies to fields",
            ));
        }
        if attribute.path().is_ident("serde") {
            let options =
                attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for option in options {
                if option.path().is_ident("skip")
                    || option.path().is_ident("skip_serializing")
                    || option.path().is_ident("skip_deserializing")
                {
                    return Err(syn::Error::new_spanned(
                        option,
                        "AktorData does not support skipped variants",
                    ));
                }
            }
        }
    }
    Ok(())
}
