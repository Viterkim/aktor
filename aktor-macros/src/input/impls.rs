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
        let mut serde = false;
        let mut remote = false;

        while !input.is_empty() {
            let option = input.call(Ident::parse_any)?;
            if option == "data" || option == "serde" || option == "remote" {
                if (option == "data" && serde) || (option == "serde" && data) {
                    return Err(syn::Error::new(
                        option.span(),
                        "choose either `data` or `serde`",
                    ));
                }

                let flag = if option == "data" {
                    &mut data
                } else if option == "serde" {
                    &mut serde
                } else {
                    &mut remote
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
            serde,
            remote,
        })
    }
}

impl Parse for Function {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let function = input.parse::<ItemFn>()?;

        Self::new(function)
    }
}
