use syn::{Field, Meta, Token, punctuated::Punctuated};

pub fn skipped(field: &Field) -> syn::Result<bool> {
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

pub fn check_variant(variant: &syn::Variant) -> syn::Result<()> {
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
