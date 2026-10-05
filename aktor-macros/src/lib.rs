#![doc = include_str!("../README.md")]

mod generate;
mod input;
mod names;
mod path;
mod setups;
mod signature;
mod worker;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn aktor(options: TokenStream, item: TokenStream) -> TokenStream {
    let options = parse_macro_input!(options as input::Options);
    let function = parse_macro_input!(item as input::Function);

    match generate::expand(options, function) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn aktor_setups(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as setups::Setups);

    match setups::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
