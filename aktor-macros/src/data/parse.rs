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
                if option.path().is_ident("skip") || option.path().is_ident("skip_serializing") {
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

pub fn check_serde_attrs(input: &syn::DeriveInput) -> syn::Result<()> {
    fn check(attrs: &[syn::Attribute]) -> syn::Result<()> {
        if let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("aktor")) {
            return Err(syn::Error::new_spanned(
                attr,
                "put this rule in the type's Serde implementation when using #[aktor(serde)]",
            ));
        }

        Ok(())
    }

    match &input.data {
        syn::Data::Struct(data) => data.fields.iter().try_for_each(|field| check(&field.attrs)),
        syn::Data::Enum(data) => {
            for variant in &data.variants {
                check(&variant.attrs)?;
                for field in &variant.fields {
                    check(&field.attrs)?;
                }
            }

            Ok(())
        }
        syn::Data::Union(_) => Err(syn::Error::new_spanned(
            &input.ident,
            "AktorData supports structs and enums",
        )),
    }
}
