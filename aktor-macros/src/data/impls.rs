use super::{Member, Shape, Style, parse::skipped};
use proc_macro2::TokenStream;
use quote::quote;
use syn::Fields;

impl Shape {
    pub fn parse(fields: Fields) -> syn::Result<Self> {
        let style = match &fields {
            Fields::Named(_) => Style::Named,
            Fields::Unnamed(_) => Style::Tuple,
            Fields::Unit => Style::Unit,
        };
        let members = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let skip = skipped(&field)?;
                Ok(Member {
                    name: field
                        .ident
                        .map_or_else(|| syn::Member::Unnamed(index.into()), syn::Member::Named),
                    ty: field.ty,
                    skip,
                })
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(Self { style, members })
    }

    pub fn helper(&self, wrapper: &TokenStream, lifetime: Option<&syn::Lifetime>) -> TokenStream {
        let fields = self
            .members
            .iter()
            .filter(|field| !field.skip)
            .map(|field| {
                let ty = &field.ty;
                let ty = if let Some(lifetime) = lifetime {
                    quote!(#wrapper<#lifetime, #ty>)
                } else {
                    quote!(#wrapper<#ty>)
                };

                let name = &field.name;

                match self.style {
                    Style::Named => quote!(#name: #ty),
                    Style::Tuple | Style::Unit => ty,
                }
            });

        match self.style {
            Style::Named => quote!({ #(#fields,)* }),
            Style::Tuple => quote!((#(#fields,)*)),
            Style::Unit => quote!(),
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

        match self.style {
            Style::Named => {
                let fields = members.zip(values).map(|(member, value)| {
                    let name = &member.name;
                    quote!(#name: #value)
                });

                quote!(#prefix { #(#fields,)* })
            }
            Style::Tuple => quote!(#prefix(#(#values,)*)),
            Style::Unit => prefix,
        }
    }
}
