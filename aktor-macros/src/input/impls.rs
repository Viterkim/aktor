use super::*;
use syn::{
    ItemFn,
    ext::IdentExt,
    parse::{Parse, ParseStream},
};

impl Parse for Options {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut crate_path = None;
        let mut role = None;
        let mut data = false;

        while !input.is_empty() {
            let option = input.call(Ident::parse_any)?;
            if option == "data" {
                if data {
                    return Err(syn::Error::new(option.span(), "duplicate `data` option"));
                }

                data = true;

                if input.is_empty() {
                    break;
                }

                input.parse::<syn::Token![,]>()?;
                continue;
            }

            let value = match option.to_string().as_str() {
                "crate" => &mut crate_path,
                "role" | "actor" => &mut role,
                _ => {
                    return Err(syn::Error::new(
                        option.span(),
                        format!("unknown #[aktor] option `{option}`"),
                    ));
                }
            };

            if value.is_some() {
                return Err(syn::Error::new(
                    option.span(),
                    format!("duplicate `{option}` option"),
                ));
            }

            input.parse::<syn::Token![=]>()?;
            *value = Some(input.parse()?);

            if input.is_empty() {
                break;
            }

            input.parse::<syn::Token![,]>()?;
        }

        Ok(Self {
            crate_path,
            role,
            data,
        })
    }
}

impl Parse for Function {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let function = input.parse::<ItemFn>()?;

        Self::new(function)
    }
}
