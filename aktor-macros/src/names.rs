use proc_macro2::{Span, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::HashSet;
use syn::{Block, Ident, Signature};

pub struct Names {
    pub state: Ident,
    pub input: Ident,
    pub operation: Ident,
    pub inner: Ident,
    pub results: Ident,
    pub sender: Ident,
    pub function: Ident,
    pub target: Ident,
    pub body: Ident,
    pub arguments: Vec<Ident>,
    pub reserved: HashSet<String>,
}
impl Names {
    pub fn new(signature: &Signature, body: &Block, extra: TokenStream) -> Self {
        let mut reserved = HashSet::new();

        collect(signature.to_token_stream(), &mut reserved);
        collect(body.to_token_stream(), &mut reserved);
        collect(extra, &mut reserved);

        Self {
            state: binding(&mut reserved, "__aktor_state"),
            input: binding(&mut reserved, "__aktor_input"),
            operation: binding(&mut reserved, "__aktor_operation"),
            inner: binding(&mut reserved, "__aktor_inner"),
            results: binding(&mut reserved, "__aktor_results"),
            sender: binding(&mut reserved, "__aktor_sender"),
            function: binding(&mut reserved, "__aktor_function"),
            target: binding(&mut reserved, "__AktorTarget"),
            body: binding(
                &mut reserved,
                &format!(
                    "__aktor_{}_body",
                    signature.ident.to_string().trim_start_matches("r#")
                ),
            ),
            arguments: (1..signature.inputs.len())
                .map(|n| binding(&mut reserved, &format!("__aktor_arg_{n}")))
                .collect(),
            reserved,
        }
    }
}

pub fn collect(tokens: TokenStream, reserved: &mut HashSet<String>) {
    for token in tokens {
        match token {
            TokenTree::Ident(ident) => {
                reserved.insert(ident.to_string().trim_start_matches("r#").to_owned());
            }
            TokenTree::Group(group) => collect(group.stream(), reserved),
            _ => {}
        }
    }
}

pub fn binding(reserved: &mut HashSet<String>, base: &str) -> Ident {
    let mut name = base.to_owned();

    while reserved.contains(&name) {
        name.insert(0, '_');
    }

    reserved.insert(name.clone());
    Ident::new(&name, Span::mixed_site())
}
