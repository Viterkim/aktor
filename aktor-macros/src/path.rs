use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Ident, Path};

pub fn aktor(overridden: Option<Path>) -> syn::Result<TokenStream> {
    if let Some(path) = overridden {
        return Ok(quote!(#path));
    }

    match crate_name("aktor") {
        Ok(FoundCrate::Itself) => Ok(quote!(::aktor)),
        Ok(FoundCrate::Name(name)) => {
            let name = Ident::new(&name.replace('-', "_"), Span::call_site());
            Ok(quote!(::#name))
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!("could not find `aktor`: {error}; try #[aktor(crate = path)]"),
        )),
    }
}
