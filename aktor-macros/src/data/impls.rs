use super::{Member, Shape, parse::skipped};
use proc_macro2::TokenStream;
use quote::quote;
use syn::Fields;

impl Shape {
    pub fn parse(fields: Fields) -> syn::Result<Self> {
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

    pub fn helper(&self, aktor: &TokenStream, lifetime: Option<&syn::Lifetime>) -> TokenStream {
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

    pub fn construct(
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
