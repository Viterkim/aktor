use syn::{Attribute, Meta, Token, parse_quote, punctuated::Punctuated};

pub fn wrapper(attributes: &[Attribute]) -> syn::Result<Vec<Attribute>> {
    filter(attributes, |meta| {
        meta.path().get_ident().is_some_and(|name| {
            matches!(
                name.to_string().as_str(),
                "doc"
                    | "cfg"
                    | "allow"
                    | "warn"
                    | "deny"
                    | "forbid"
                    | "deprecated"
                    | "must_use"
                    | "inline"
                    | "cold"
                    | "track_caller"
            )
        })
    })
}

pub fn implementation(attributes: &[Attribute]) -> syn::Result<Vec<Attribute>> {
    filter(attributes, |meta| !meta.path().is_ident("deprecated"))
}

fn filter(attributes: &[Attribute], keep: fn(&Meta) -> bool) -> syn::Result<Vec<Attribute>> {
    let mut output = Vec::new();

    for attribute in attributes {
        if let Some(meta) = filter_meta(&attribute.meta, keep)? {
            let mut attribute = attribute.clone();
            attribute.meta = meta;
            output.push(attribute);
        }
    }

    Ok(output)
}

fn filter_meta(meta: &Meta, keep: fn(&Meta) -> bool) -> syn::Result<Option<Meta>> {
    if let Meta::List(list) = meta
        && list.path.is_ident("cfg_attr")
    {
        let arguments = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        let mut arguments = arguments.into_iter();
        let Some(condition) = arguments.next() else {
            return Err(syn::Error::new_spanned(list, "cfg_attr needs a condition"));
        };
        let mut attributes = Vec::new();

        for meta in arguments {
            if let Some(meta) = filter_meta(&meta, keep)? {
                attributes.push(meta);
            }
        }

        return Ok(
            (!attributes.is_empty()).then(|| parse_quote!(cfg_attr(#condition, #(#attributes),*)))
        );
    }

    Ok(keep(meta).then(|| meta.clone()))
}
