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
        let mut name: Option<syn::LitStr> = None;
        let mut remote = false;
        let mut latest = false;

        while !input.is_empty() {
            let option = input.call(Ident::parse_any)?;
            if option == "remote" || option == "latest" {
                let flag = if option == "remote" {
                    &mut remote
                } else {
                    &mut latest
                };

                if *flag {
                    return Err(syn::Error::new(
                        option.span(),
                        format!("duplicate `{option}` option"),
                    ));
                }

                *flag = true;

                if input.is_empty() {
                    break;
                }

                input.parse::<syn::Token![,]>()?;
                continue;
            }

            if option == "name" {
                if name.is_some() {
                    return Err(syn::Error::new(option.span(), "duplicate `name` option"));
                }

                input.parse::<syn::Token![=]>()?;

                let written: syn::LitStr = input.parse()?;

                if written.value().is_empty() {
                    return Err(syn::Error::new(
                        written.span(),
                        "the operation name can't be empty",
                    ));
                }

                name = Some(written);

                if input.is_empty() {
                    break;
                }

                input.parse::<syn::Token![,]>()?;
                continue;
            }

            let value = match option.to_string().as_str() {
                "crate" => &mut crate_path,
                "role" => &mut role,
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
            name,
            remote,
            latest,
        })
    }
}

impl Parse for Function {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let function = input.parse::<ItemFn>()?;

        Self::new(function)
    }
}
