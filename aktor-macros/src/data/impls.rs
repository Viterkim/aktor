use super::{Member, Shape, Style, parse::field_options};
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
                let (skip, max_len) = field_options(&field)?;
                Ok(Member {
                    name: field
                        .ident
                        .map_or_else(|| syn::Member::Unnamed(index.into()), syn::Member::Named),
                    ty: field.ty,
                    skip,
                    max_len,
                })
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(Self { style, members })
    }

    /// The variant or struct built from one value per member, skipped ones included.
    pub fn construct(&self, prefix: TokenStream, values: Vec<TokenStream>) -> TokenStream {
        match self.style {
            Style::Named => {
                let fields = self.members.iter().zip(values).map(|(member, value)| {
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
