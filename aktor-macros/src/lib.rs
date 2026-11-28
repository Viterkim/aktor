#![doc = include_str!("../README.md")]

mod data;
mod generate;
mod input;
mod names;
mod path;
mod signature;
mod worker;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Turns an `async fn` taking `&State` or `&mut State` into an actor call.
///
/// You can pass an actor handle to queue the call and `.await` it, or pass `&State` / `&mut State`
/// directly (for example inside another query or transaction) to run it right there.
///
/// A worker can run the same function when its arguments and result implement `AktorData`.
/// A page built with `wasm_browser_workers` only compiles the call. The worker built with
/// `wasm_browser_worker_server` compiles the body.
///
/// For a return type of `Result<T, (E, String)>`, the body can return any error that converts
/// into the pair, including the error from an `ErResult`.
///
/// Options include `role = Type`, `latest` (for `.latest()` sessions), `remote` (for
/// `aktor-remote`), `name = "..."`, and `crate = path`.
#[proc_macro_attribute]
pub fn aktor(options: TokenStream, item: TokenStream) -> TokenStream {
    let options = parse_macro_input!(options as input::Options);
    let function = parse_macro_input!(item as input::Function);

    match generate::expand(options, function) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives `AktorData` encoding for args and replies crossing a browser worker or `aktor-remote`
/// boundary. It works in every build, `transport` only adds the sending, so a type shared between
/// a native build and a worker just derives it.
///
/// Options include `#[aktor(skip)]` and `#[aktor(max_len = N)]` on fields,
/// `#[aktor(bound = "...")]`, and `#[aktor(crate = path)]`.
#[proc_macro_derive(AktorData, attributes(aktor))]
pub fn data(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    match data::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
