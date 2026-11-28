use super::Options;
use syn::{Attribute, Field, Meta, Token, WherePredicate, punctuated::Punctuated};

pub fn options(attrs: &[Attribute]) -> syn::Result<Options> {
    let mut options = Options {
        krate: None,
        bound: None,
    };

    for attribute in attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("aktor"))
    {
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                if options.krate.is_some() {
                    return Err(meta.error("duplicate aktor(crate = path)"));
                }

                options.krate = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("bound") {
                if options.bound.is_some() {
                    return Err(meta.error("duplicate aktor(bound = \"...\")"));
                }

                let written: syn::LitStr = meta.value()?.parse()?;
                let predicates = written
                    .parse_with(Punctuated::<WherePredicate, Token![,]>::parse_terminated)?;

                options.bound = Some(predicates.into_iter().collect());
                Ok(())
            } else {
                Err(meta
                    .error("expected aktor(bound = \"...\") or aktor(crate = path) on the type"))
            }
        })?;
    }

    Ok(options)
}

pub fn is_packed(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("repr"))
        .filter_map(|attribute| {
            attribute
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .ok()
        })
        .flatten()
        .any(|option| option.path().is_ident("packed"))
}

/// `#[aktor(skip)]` and `#[aktor(max_len = N)]` on one field.
pub fn field_options(field: &Field) -> syn::Result<(bool, Option<syn::Expr>)> {
    let mut skip = false;
    let mut max_len = None;

    for attribute in &field.attrs {
        if attribute.path().is_ident("aktor") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    if skip {
                        return Err(meta.error("duplicate aktor(skip)"));
                    }

                    skip = true;
                    Ok(())
                } else if meta.path.is_ident("max_len") {
                    if max_len.is_some() {
                        return Err(meta.error("duplicate aktor(max_len = ...)"));
                    }

                    max_len = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("expected aktor(skip) or aktor(max_len = ...)"))
                }
            })?;
        }
    }

    if skip && let Some(max_len) = &max_len {
        return Err(syn::Error::new_spanned(
            max_len,
            "a skipped field isn't sent, so there's no length for max_len to check",
        ));
    }

    Ok((skip, max_len))
}

pub fn check_variant(variant: &syn::Variant) -> syn::Result<()> {
    for attribute in &variant.attrs {
        if attribute.path().is_ident("aktor") {
            return Err(syn::Error::new_spanned(
                attribute,
                "AktorData does not support skipped variants; aktor(skip) applies to fields",
            ));
        }
    }

    Ok(())
}
